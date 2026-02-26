import os
import re
import shlex
import time
from functools import lru_cache
from typing import List, Optional, Set, Tuple

from backend.src.actions.handlers.common_utils import (
    load_json_object,
    parse_command_tokens,
    resolve_path_with_workdir,
    truncate_inline_text,
)
from backend.src.common.path_utils import normalize_windows_abs_path_on_posix
from backend.src.common.task_error_codes import format_task_error
from backend.src.common.utils import parse_json_dict, parse_json_value
from backend.src.constants import (
    ACTION_TYPE_TOOL_CALL,
    AGENT_EXPERIMENT_DIR_REL,
    AGENT_SHELL_COMMAND_DEFAULT_TIMEOUT_MS,
    AUTO_TOOL_DESCRIPTION_TEMPLATE,
    AUTO_TOOL_PREFIX,
    DEFAULT_TOOL_VERSION,
    ERROR_MESSAGE_PROMPT_RENDER_FAILED,
    TOOL_NAME_WEB_FETCH,
    WEB_FETCH_BLOCK_MARKERS_DEFAULT,
    AGENT_WEB_FETCH_BLOCK_MARKERS_ENV,
    AGENT_WEB_FETCH_BLOCK_MARKERS_MAX,
    TOOL_METADATA_SOURCE_AUTO,
    SHELL_COMMAND_REQUIRE_FILE_WRITE_BINDING_DEFAULT,
)
from backend.src.services.execution.shell_command import run_shell_command
from backend.src.services.debug.safe_debug import safe_write_debug as _safe_write_debug
from backend.src.services.tools.tool_records import create_tool_record as _create_tool_record
from backend.src.repositories.task_steps_repo import list_task_steps_for_run
from backend.src.repositories.tools_repo import (
    get_tool,
    get_tool_by_name,
    get_tool_metadata_by_id,
    get_tool_metadata_by_name,
)
from backend.src.services.permissions.permissions_store import is_tool_enabled


def _normalize_web_fetch_marker(phrase: object, tag: object) -> Optional[Tuple[str, str]]:
    text = str(phrase or "").strip().lower()
    code = str(tag or "").strip().lower()
    if not text:
        return None
    if not code:
        code = "custom_blocked"
    return text, code


def _iter_env_web_fetch_markers(raw_env: str) -> List[Tuple[str, str]]:
    text = str(raw_env or "").strip()
    if not text:
        return []
    payload = parse_json_value(text)
    if not isinstance(payload, list):
        return []

    parsed: List[Tuple[str, str]] = []
    for item in payload:
        if isinstance(item, str):
            normalized = _normalize_web_fetch_marker(item, "custom_blocked")
            if normalized:
                parsed.append(normalized)
            continue
        if isinstance(item, list) and len(item) >= 1:
            phrase = item[0]
            tag = item[1] if len(item) >= 2 else "custom_blocked"
            normalized = _normalize_web_fetch_marker(phrase, tag)
            if normalized:
                parsed.append(normalized)
            continue
        if isinstance(item, dict):
            normalized = _normalize_web_fetch_marker(item.get("phrase"), item.get("tag") or item.get("code"))
            if normalized:
                parsed.append(normalized)
    return parsed


@lru_cache(maxsize=1)
def _get_web_fetch_block_markers() -> List[Tuple[str, str]]:
    """
    统一获取 web_fetch 拦截判定规则（默认 + 环境变量扩展）。

    环境变量格式（JSON array）：
    - ["blocked by upstream"]
    - [["blocked by upstream", "request_blocked"]]
    - [{"phrase":"blocked by upstream","tag":"request_blocked"}]
    """
    merged: List[Tuple[str, str]] = []
    seen = set()
    for phrase, tag in list(WEB_FETCH_BLOCK_MARKERS_DEFAULT or []):
        normalized = _normalize_web_fetch_marker(phrase, tag)
        if not normalized:
            continue
        if normalized[0] in seen:
            continue
        seen.add(normalized[0])
        merged.append(normalized)

    env_markers = _iter_env_web_fetch_markers(os.getenv(AGENT_WEB_FETCH_BLOCK_MARKERS_ENV, ""))
    for phrase, tag in env_markers:
        if phrase in seen:
            continue
        seen.add(phrase)
        merged.append((phrase, tag))

    try:
        limit = max(1, int(AGENT_WEB_FETCH_BLOCK_MARKERS_MAX or 64))
    except Exception:
        limit = 64
    return merged[:limit]


def _detect_web_fetch_block_reason(output_text: str) -> Optional[str]:
    """
    尝试识别 web_fetch 返回的“反爬/限流/拦截页面”。

    说明：
    - curl -f 只能识别 HTTP>=400；但部分站点会返回 200 + 拦截页面正文；
    - 这些正文不应作为“抓取成功证据”继续进入 json_parse/task_output，否则会诱发“编数据”。
    """
    raw = str(output_text or "").strip()
    if not raw:
        return None
    lowered = raw.lower()
    sample = lowered[:4000]
    for phrase, tag in _get_web_fetch_block_markers():
        if phrase in sample:
            return tag
    # 兜底：部分站点只返回状态行/标题，不包含完整描述文本。
    if re.search(r"\bhttp/[0-9.]+\s+429\b", sample):
        return "too_many_requests"
    if re.search(r"\bhttp/[0-9.]+\s+403\b", sample):
        return "access_denied"
    if re.search(r"\bhttp/[0-9.]+\s+503\b", sample):
        return "service_unavailable"
    return None


def _detect_web_fetch_semantic_error(output_text: str) -> Optional[str]:
    """
    检测 web_fetch 的“业务语义失败”（例如 success=false / error 对象）。

    背景：
    - 某些数据接口会返回 200 + JSON 错误体（如 missing_access_key）；
    - 这类响应不应被当作抓取成功继续流转，否则会诱发后续空产物/伪结果。
    """
    raw = str(output_text or "").strip()
    if not raw:
        return None

    parsed = parse_json_dict(raw)
    if not parsed:
        return None

    success_value = parsed.get("success")
    status_text = str(parsed.get("status") or "").strip().lower()
    error_obj = parsed.get("error")

    has_error_payload = bool(
        isinstance(error_obj, dict)
        or (isinstance(error_obj, str) and str(error_obj).strip())
    )
    explicit_failure = (success_value is False) or (status_text in {"error", "failed", "fail"})
    if not explicit_failure and not has_error_payload:
        return None

    if isinstance(error_obj, dict):
        error_type = str(
            error_obj.get("type")
            or error_obj.get("code")
            or error_obj.get("name")
            or ""
        ).strip()
        error_message = truncate_inline_text(
            error_obj.get("info")
            or error_obj.get("message")
            or error_obj.get("detail")
            or "",
            180,
        )
    else:
        error_type = ""
        error_message = truncate_inline_text(error_obj, 180)

    if error_type and error_message:
        return f"{error_type}: {error_message}"
    if error_type:
        return error_type
    if error_message:
        return error_message

    return "semantic_error"


def _normalize_exec_spec(exec_spec: dict) -> dict:
    """
    tool_metadata.exec 兼容归一化：
    - 常见别名：shell -> command，timeout -> timeout_ms
    - args 可能被模型输出成字符串：转为 command
    - command 可能被模型输出成 list：转为 args

    约定：最终使用字段
    - type: "shell"（可省略，若提供了 command/args 会自动按 shell 执行）
    - command: str 或 args: list[str]
    - timeout_ms: int（可选）
    - workdir: str（建议必填）
    """
    if not isinstance(exec_spec, dict):
        return {}
    spec = dict(exec_spec)

    # 常见别名：shell -> command
    shell_value = spec.get("shell")
    if (
        isinstance(shell_value, str)
        and shell_value.strip()
        and not spec.get("command")
    ):
        spec["command"] = shell_value.strip()

    # args: str -> command
    args_value = spec.get("args")
    if isinstance(args_value, str) and args_value.strip():
        if not spec.get("command"):
            spec["command"] = args_value.strip()
        spec.pop("args", None)

    # command: list -> args
    cmd_value = spec.get("command")
    if isinstance(cmd_value, list) and cmd_value:
        existing_args = spec.get("args")
        if isinstance(existing_args, list) and existing_args:
            # 兼容：模型可能同时输出 command(list) + args(list)。这里把两者拼接成“完整命令 token 列表”，
            # 避免丢失 command(list) 导致仅执行 args（常见错误：args=["GC=F"] -> [WinError 2]）。
            spec["args"] = [str(v) for v in cmd_value] + [str(v) for v in existing_args]
        else:
            spec["args"] = [str(v) for v in cmd_value]
        spec.pop("command", None)

    # timeout: 兼容字段；若 < 1000 认为是秒，否则认为是毫秒
    if spec.get("timeout_ms") is None and spec.get("timeout") is not None:
        value = spec.get("timeout")
        try:
            num = float(value)
            spec["timeout_ms"] = int(num * 1000) if 0 < num < 1000 else int(num)
        except Exception:
            pass

    return spec


def _load_tool_metadata_from_db(tool_id: Optional[int], tool_name: Optional[str]) -> Optional[dict]:
    """
    读取 tools_items.metadata（JSON）并解析为 dict。
    """
    if tool_id is None and not tool_name:
        return None
    if tool_id is not None:
        return get_tool_metadata_by_id(tool_id=int(tool_id))
    return get_tool_metadata_by_name(name=str(tool_name or ""))


def _has_nonempty_exec_spec(spec: dict) -> bool:
    """判断 exec_spec 是否包含有效内容（兼容模型输出空 exec {}）。"""
    return bool(
        str(spec.get("type") or "").strip()
        or (isinstance(spec.get("args"), list) and spec.get("args"))
        or str(spec.get("command") or "").strip()
    )


def _resolve_tool_exec_spec(payload: dict) -> Optional[dict]:
    """
    优先从 payload.tool_metadata 读取 exec，其次从 tools_items.metadata 读取 exec。
    """
    meta = payload.get("tool_metadata")
    if isinstance(meta, dict):
        exec_spec = meta.get("exec")
        if isinstance(exec_spec, dict):
            exec_spec = _normalize_exec_spec(exec_spec)
            if _has_nonempty_exec_spec(exec_spec):
                return exec_spec
    meta = _load_tool_metadata_from_db(payload.get("tool_id"), payload.get("tool_name"))
    if isinstance(meta, dict):
        exec_spec = meta.get("exec")
        if isinstance(exec_spec, dict):
            normalized = _normalize_exec_spec(exec_spec)
            if _has_nonempty_exec_spec(normalized):
                return normalized

    # 再兜底：从实验目录脚本推断执行定义（防止“已写脚本但漏填 exec”中断）。
    inferred = _infer_exec_spec_from_workspace_script(payload)
    if isinstance(inferred, dict):
        return _normalize_exec_spec(inferred)
    return None


def _infer_exec_spec_from_workspace_script(payload: dict) -> Optional[dict]:
    """
    兜底：当 tool_metadata.exec 缺失时，尝试从实验目录推断脚本执行命令。

    适用场景：模型先写了 `backend/.agent/workspace/<tool_name>.py`，
    但下一步 tool_call 漏填了 tool_metadata.exec。
    """
    tool_name = str(payload.get("tool_name") or "").strip()
    if not tool_name:
        return None

    workdir = ""
    meta = payload.get("tool_metadata")
    if isinstance(meta, dict):
        exec_meta = meta.get("exec")
        if isinstance(exec_meta, dict):
            workdir = str(exec_meta.get("workdir") or "").strip()
    if not workdir:
        workdir = os.getcwd()

    workspace_dir = os.path.join(workdir, str(AGENT_EXPERIMENT_DIR_REL).replace("/", os.sep))
    candidates = [
        (os.path.join(workspace_dir, f"{tool_name}.py"), "python"),
        (os.path.join(workspace_dir, f"{tool_name}.sh"), "sh"),
        (os.path.join(workspace_dir, f"{tool_name}.ps1"), "powershell"),
        (os.path.join(workspace_dir, f"{tool_name}.bat"), None),
        (os.path.join(workspace_dir, f"{tool_name}.cmd"), None),
    ]

    for script_path, launcher in candidates:
        if not os.path.exists(script_path):
            continue
        rel_script = os.path.relpath(script_path, workdir)
        rel_script = rel_script.replace("\\", "/")
        command = f"{launcher} {rel_script}" if launcher else rel_script
        return {
            "type": "shell",
            "command": command,
            "workdir": workdir,
            "timeout_ms": AGENT_SHELL_COMMAND_DEFAULT_TIMEOUT_MS,
        }

    return None


def _looks_like_executable_token(token: str) -> bool:
    head = str(token or "").strip()
    if not head:
        return False
    lowered = head.lower()
    if lowered.startswith("-"):
        return False
    ext = os.path.splitext(lowered)[1]
    if ext in {".py", ".js", ".ts", ".sh", ".ps1", ".bat", ".cmd", ".txt", ".md", ".csv", ".json"}:
        return False
    if "/" in head or "\\" in head:
        return True
    if lowered.endswith(".exe"):
        return True
    return lowered in {
        "python",
        "python3",
        "py",
        "pip",
        "pip3",
        "curl",
        "wget",
        "node",
        "npm",
        "npx",
        "git",
        "cmd",
        "cmd.exe",
        "powershell",
        "pwsh",
        "uv",
        "uvicorn",
    }


def _extract_script_candidates_from_tokens(tokens: List[str]) -> List[str]:
    if not tokens:
        return []

    first = str(tokens[0] or "").strip()
    if not first:
        return []

    first_name = os.path.splitext(os.path.basename(first))[0].lower()
    if first_name in {"python", "python3", "py"}:
        if len(tokens) >= 2 and str(tokens[1] or "").strip() in {"-c", "-m"}:
            return []
        for token in tokens[1:]:
            current = str(token or "").strip()
            if not current or current.startswith("-"):
                continue
            if current.lower().endswith((".py", ".sh", ".ps1", ".bat", ".cmd")):
                return [current]
            return []
        return []

    if first.lower().endswith((".py", ".sh", ".ps1", ".bat", ".cmd")):
        return [first]
    return []


def _extract_script_candidates_from_exec_spec(exec_spec: dict, tool_input: str) -> List[str]:
    if not isinstance(exec_spec, dict):
        return []

    args = exec_spec.get("args")
    command = exec_spec.get("command")
    tokens: List[str] = []

    if isinstance(args, list) and args:
        formatted_args = [str(item).replace("{input}", tool_input) for item in args]
        if isinstance(command, str) and command.strip():
            command_tokens = parse_command_tokens(str(command).replace("{input}", tool_input))
            if command_tokens:
                if _looks_like_executable_token(formatted_args[0]):
                    tokens = formatted_args
                else:
                    tokens = command_tokens + formatted_args
            else:
                tokens = formatted_args
        else:
            tokens = formatted_args
    elif isinstance(command, str) and command.strip():
        tokens = parse_command_tokens(str(command).replace("{input}", tool_input))
    elif isinstance(command, list) and command:
        tokens = [str(item).replace("{input}", tool_input) for item in command if str(item).strip()]

    return _extract_script_candidates_from_tokens(tokens)


def _collect_written_script_paths_for_run(
    *,
    task_id: int,
    run_id: int,
    current_step_id: Optional[int],
    workdir: str,
) -> Set[str]:
    paths: Set[str] = set()
    try:
        rows = list_task_steps_for_run(task_id=int(task_id), run_id=int(run_id))
    except Exception:
        return paths

    for row in rows or []:
        if not row:
            continue
        try:
            row_id = int(row["id"]) if row["id"] is not None else None
        except Exception:
            row_id = None
        if current_step_id is not None and row_id == int(current_step_id):
            continue

        status = str(row["status"] or "").strip().lower() if "status" in row.keys() else ""
        if status != "done":
            continue

        detail_obj = load_json_object(row["detail"] if "detail" in row.keys() else None)
        action_type = str(detail_obj.get("type") or "").strip().lower() if isinstance(detail_obj, dict) else ""
        if action_type not in {"file_write", "file_append"}:
            continue

        payload_obj = detail_obj.get("payload") if isinstance(detail_obj, dict) else None
        result_obj = load_json_object(row["result"] if "result" in row.keys() else None)

        raw_path = ""
        if isinstance(result_obj, dict):
            raw_path = str(result_obj.get("path") or "").strip()
        if not raw_path and isinstance(payload_obj, dict):
            raw_path = str(payload_obj.get("path") or "").strip()

        resolved = resolve_path_with_workdir(raw_path, workdir)
        if resolved:
            paths.add(os.path.normcase(resolved))

    return paths


def _enforce_tool_exec_script_dependency(
    *,
    task_id: int,
    run_id: int,
    step_row,
    exec_spec: dict,
    tool_input: str,
) -> Optional[str]:
    """
    强约束：tool_call.exec 若引用脚本文件，必须满足“脚本存在 + 当前 run 已有 file_write/file_append 成功步骤”。

    目的：避免出现“先执行工具脚本，后补写脚本”导致的不可重复失败。
    """
    if not SHELL_COMMAND_REQUIRE_FILE_WRITE_BINDING_DEFAULT:
        return None
    if not isinstance(exec_spec, dict):
        return None

    workdir = normalize_windows_abs_path_on_posix(str(exec_spec.get("workdir") or "").strip())
    if not workdir:
        workdir = os.getcwd()

    script_candidates = _extract_script_candidates_from_exec_spec(exec_spec, tool_input)
    if not script_candidates:
        return None

    current_step_id = None
    try:
        if hasattr(step_row, "keys") and "id" in step_row.keys() and step_row["id"] is not None:
            current_step_id = int(step_row["id"])
    except Exception:
        current_step_id = None

    written_paths = _collect_written_script_paths_for_run(
        task_id=int(task_id),
        run_id=int(run_id),
        current_step_id=current_step_id,
        workdir=workdir,
    )

    missing_paths: List[str] = []
    unbound_paths: List[str] = []
    for candidate in script_candidates:
        absolute_path = resolve_path_with_workdir(candidate, workdir)
        if not absolute_path:
            continue
        if not os.path.exists(absolute_path):
            missing_paths.append(absolute_path)
            continue
        normalized = os.path.normcase(absolute_path)
        if normalized not in written_paths:
            unbound_paths.append(absolute_path)

    if missing_paths:
        return (
            f"工具执行失败: 脚本不存在: {', '.join(missing_paths)}"
            "（请先执行 file_write/file_append 并确认落盘）"
        )
    if unbound_paths:
        return (
            f"工具执行失败: 脚本依赖未绑定: {', '.join(unbound_paths)}"
            "（当前 run 未发现对应的 file_write/file_append 成功步骤）"
        )
    return None


def _execute_tool_with_exec_spec(exec_spec: dict, tool_input: str) -> Tuple[Optional[str], Optional[str]]:
    """
    执行工具（目前仅支持 shell）。
    返回：(output_text, error_message)
    """
    exec_spec = _normalize_exec_spec(exec_spec)

    args = exec_spec.get("args")
    command = exec_spec.get("command")
    timeout_ms = exec_spec.get("timeout_ms")
    workdir = exec_spec.get("workdir") or os.getcwd()

    exec_type = (exec_spec.get("type") or "").strip().lower()
    # 兼容：模型可能漏填 type，但提供了 args/command。此时默认按 shell 执行，避免直接失败。
    if not exec_type:
        has_cmd = bool((isinstance(args, list) and args) or (isinstance(command, str) and command.strip()))
        if has_cmd:
            exec_type = "shell"
        else:
            return None, "工具未配置 exec.type（仅支持 shell），且缺少 command/args"

    if exec_type != "shell":
        # 兼容：部分模型会输出 type="empty"/"cmd" 等无效值，但同时给了 command/args。
        # 若存在可执行命令，则按 shell 兜底继续执行，避免“自举工具”链路被无意义阻断。
        has_cmd = bool((isinstance(args, list) and args) or (isinstance(command, str) and command.strip()))
        if has_cmd and exec_type in {"empty", "cmd", "command"}:
            exec_type = "shell"
        else:
            return None, f"不支持的工具执行类型: {exec_type}"

    def _split_command_text(text: str) -> list[str]:
        tokens = shlex.split(text, posix=os.name != "nt")
        if os.name == "nt":
            # 参见 services/execution/shell_command.py：Windows 下要剥离最外层引号，
            # 否则 python -c 会把代码当作字符串字面量导致无输出。
            cleaned: list[str] = []
            for item in tokens:
                s = str(item)
                if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
                    s = s[1:-1]
                cleaned.append(s)
            tokens = cleaned
        return [str(t).replace("{input}", tool_input) for t in tokens]

    def _looks_like_executable_token(token: str) -> bool:
        head = str(token or "").strip()
        if not head:
            return False
        low = head.lower()
        if low.startswith("-"):
            return False
        # 常见脚本/数据文件不是可执行文件（Windows 尤其如此）：
        # - 若把 *.py 当作命令执行，会触发 [WinError 2]（找不到可执行文件）；
        # - 正确做法是由 exec.command/args 提供 python，并把脚本路径作为参数追加。
        ext = os.path.splitext(low)[1]
        if ext in {".py", ".js", ".ts", ".sh", ".ps1", ".txt", ".md", ".csv", ".json"}:
            return False
        if "/" in head or "\\" in head:
            return True
        if low.endswith(".exe") or low.endswith(".bat") or low.endswith(".cmd"):
            return True
        return low in {
            "python",
            "python3",
            "py",
            "pip",
            "pip3",
            "curl",
            "wget",
            "node",
            "npm",
            "npx",
            "git",
            "cmd.exe",
            "powershell",
            "pwsh",
            "uv",
            "uvicorn",
        }

    cmd_value = None
    uses_input_placeholder = False
    if isinstance(args, list) and args:
        formatted_args = []
        for item in args:
            text = str(item)
            if "{input}" in text:
                uses_input_placeholder = True
            formatted_args.append(text.replace("{input}", tool_input))
        if isinstance(command, str) and command.strip():
            # 兼容：模型常把 exec.command 当作“主命令”，把 exec.args 当作“附加参数”。
            # 若 args 看起来不像可执行文件（例如 ["GC=F","3mo","1d"]），则把它们追加到 command 的 token 后面。
            if _looks_like_executable_token(formatted_args[0]):
                cmd_value = formatted_args
            else:
                cmd_value = _split_command_text(command) + formatted_args
        else:
            cmd_value = formatted_args
    elif isinstance(command, str) and command.strip():
        if "{input}" in command:
            uses_input_placeholder = True
        cmd_value = command.replace("{input}", tool_input)
    else:
        return None, "工具未配置 command/args"

    retry_cfg = exec_spec.get("retry")
    max_attempts = 1
    backoff_ms = 0
    if isinstance(retry_cfg, dict):
        try:
            max_attempts = int(retry_cfg.get("max_attempts") or retry_cfg.get("attempts") or 1)
        except Exception:
            max_attempts = 1
        try:
            backoff_ms = int(retry_cfg.get("backoff_ms") or retry_cfg.get("delay_ms") or 0)
        except Exception:
            backoff_ms = 0

    # 防止无限重试卡死：即使配置过大也做一个上限保护
    if max_attempts <= 0:
        max_attempts = 1
    if max_attempts > 6:
        max_attempts = 6
    if backoff_ms < 0:
        backoff_ms = 0

    last_error = None
    last_result = None

    for attempt in range(0, max_attempts):
        result, error_message = run_shell_command(
            {
                "command": cmd_value,
                "workdir": workdir,
                "timeout_ms": timeout_ms,
                "stdin": tool_input if not uses_input_placeholder else "",
            }
        )
        if error_message:
            last_error = error_message
            last_result = None
        elif not isinstance(result, dict):
            last_error = "工具执行返回格式异常"
            last_result = None
        else:
            last_error = None
            last_result = dict(result)
            if bool(last_result.get("ok")):
                stdout = str(last_result.get("stdout") or "")
                stderr = str(last_result.get("stderr") or "")
                output_text = stdout.strip() or stderr.strip()
                return output_text or "", None

            stdout = str(last_result.get("stdout") or "")
            stderr = str(last_result.get("stderr") or "")
            rc = last_result.get("returncode")
            detail = stderr.strip() or stdout.strip() or (str(rc) if rc is not None else "")
            last_error = f"工具执行失败: {detail}".strip()

        # 最后一次失败：直接返回
        if attempt >= max_attempts - 1:
            break

        # 有 retry 配置才进入重试分支
        if max_attempts <= 1:
            break

        # 简单退避（可配置），避免瞬时抖动导致的连续失败
        if backoff_ms > 0:
            try:
                time.sleep(float(backoff_ms) / 1000.0)
            except Exception:
                pass

    if last_error:
        return None, last_error
    if isinstance(last_result, dict):
        stdout = str(last_result.get("stdout") or "")
        stderr = str(last_result.get("stderr") or "")
        rc = last_result.get("returncode")
        detail = stderr.strip() or stdout.strip() or (str(rc) if rc is not None else "")
        return None, f"工具执行失败: {detail}".strip()
    return None, "工具执行失败"


def _build_tool_metadata(task_id: int, run_id: int, step_row, payload: dict) -> dict:
    tool_input = payload.get("input")
    tool_output = payload.get("output")

    if isinstance(tool_input, dict):
        input_schema = {"type": "object", "keys": list(tool_input.keys())}
    elif isinstance(tool_input, list):
        input_schema = {"type": "list", "length": len(tool_input)}
    elif tool_input is None:
        input_schema = {"type": "empty"}
    else:
        input_schema = {"type": "text"}

    if isinstance(tool_output, dict):
        output_schema = {"type": "object", "keys": list(tool_output.keys())}
    elif isinstance(tool_output, list):
        output_schema = {"type": "list", "length": len(tool_output)}
    elif tool_output is None:
        output_schema = {"type": "empty"}
    else:
        output_schema = {"type": "text"}

    step_id = None
    step_title = None
    if hasattr(step_row, "keys"):
        if "id" in step_row.keys():
            step_id = step_row["id"]
        if "title" in step_row.keys():
            step_title = step_row["title"]
    return {
        "source": TOOL_METADATA_SOURCE_AUTO,
        "task_id": task_id,
        "run_id": run_id,
        "step_id": step_id,
        "step_title": step_title,
        "input_sample": tool_input,
        "output_sample": tool_output,
        "input_schema": input_schema,
        "output_schema": output_schema,
        "action_type": ACTION_TYPE_TOOL_CALL,
    }


def execute_tool_call(task_id: int, run_id: int, step_row, payload: dict) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 tool_call：
    - output 允许为空：若为空则尝试按 metadata.exec 执行并回填
    - 若工具不存在则自动创建 tools_items，并可自动沉淀为 skill（失败不阻塞本步）
    """
    tool_input = payload.get("input")
    tool_output = payload.get("output")
    if tool_output is None:
        payload["output"] = ""
        tool_output = ""

    if not isinstance(tool_input, str) or not tool_input.strip():
        raise ValueError("tool_call.input 不能为空")
    if not isinstance(tool_output, str):
        raise ValueError("tool_call.output 必须是字符串")

    if payload.get("tool_id") is None and not payload.get("tool_name"):
        tool_name = f"{AUTO_TOOL_PREFIX}_{task_id}_{step_row['id']}"
        payload["tool_name"] = tool_name
        step_title = None
        if hasattr(step_row, "keys") and "title" in step_row.keys():
            step_title = step_row["title"]
        payload.setdefault(
            "tool_description",
            AUTO_TOOL_DESCRIPTION_TEMPLATE.format(step_title=step_title or tool_name),
        )
        payload.setdefault("tool_version", DEFAULT_TOOL_VERSION)

    # 检查工具是否被禁用
    tool_name_value = str(payload.get("tool_name") or "").strip()
    tool_id_value = payload.get("tool_id")
    if not tool_name_value and tool_id_value is not None:
        try:
            row = get_tool(tool_id=int(tool_id_value))
            tool_name_value = str(row["name"] or "").strip() if row else ""
        except Exception:
            tool_name_value = ""
    if tool_name_value and not is_tool_enabled(tool_name_value):
        raise ValueError(f"tool 已禁用: {tool_name_value}")

    # 判断是否为“新创建工具”：用于后续沉淀为技能卡（skill）
    tool_was_missing = False
    try:
        if payload.get("tool_id") is None and payload.get("tool_name"):
            existed = get_tool_by_name(name=str(payload.get("tool_name") or ""))
            tool_was_missing = existed is None
    except Exception:
        tool_was_missing = False

    if payload.get("tool_metadata") is None:
        payload["tool_metadata"] = _build_tool_metadata(task_id, run_id, step_row, payload)

    # tool_call 必须真实执行：禁止让模型在 output 里“手填/编造结果”。
    # - 若 tools_items.metadata.exec 已存在：无论 output 是否为空，都以真实执行结果覆盖 output；
    # - 若 exec 不存在：直接报错，促使模型“先补齐工具的 exec 再继续”（工具自举）。
    exec_spec = _resolve_tool_exec_spec(payload)
    if exec_spec is None:
        raise ValueError(
            "tool_call 缺少可执行定义：请在 tool_metadata.exec 中提供 "
            "type=shell，且包含 command(str) 或 args(list)，可选 timeout_ms，建议 workdir；"
            "并把 output 留空让系统真实执行"
        )

    allow_empty_output = False
    if isinstance(exec_spec, dict):
        try:
            allow_empty_output = bool(exec_spec.get("allow_empty_output"))
        except Exception:
            allow_empty_output = False

    dependency_error = _enforce_tool_exec_script_dependency(
        task_id=int(task_id),
        run_id=int(run_id),
        step_row=step_row,
        exec_spec=exec_spec,
        tool_input=str(tool_input),
    )
    if dependency_error:
        raise ValueError(dependency_error)

    output_text, exec_error = _execute_tool_with_exec_spec(exec_spec, str(tool_input))
    if exec_error:
        # 检测 TLS/SSL 握手失败
        lowered_err = str(exec_error).lower()
        if "handshake" in lowered_err and ("ssl" in lowered_err or "tls" in lowered_err):
            raise ValueError(format_task_error(code="tls_handshake_failed", message=exec_error))
        raise ValueError(exec_error)

    output_text = str(output_text or "")
    warnings: List[str] = []
    if tool_was_missing and not output_text.strip():
        warnings.append("新创建工具执行输出为空：建议让工具打印关键结果/关键日志，或使用文件落盘并在后续步骤验证产物。")
    if not output_text.strip() and not allow_empty_output:
        warnings.append("工具输出为空：若该工具以文件落盘为主，请设置 exec.allow_empty_output=true 并补充验证步骤。")

    payload["output"] = output_text
    # 让 metadata 里保留一次可读的样例，便于后续沉淀为技能/回放调试
    if isinstance(payload.get("tool_metadata"), dict):
        payload["tool_metadata"]["input_sample"] = str(tool_input)
        payload["tool_metadata"]["output_sample"] = output_text

    payload.setdefault("task_id", task_id)
    payload.setdefault("run_id", run_id)
    result = _create_tool_record(payload)
    record = result.get("record") if isinstance(result, dict) else None
    if not isinstance(record, dict):
        raise ValueError(ERROR_MESSAGE_PROMPT_RENDER_FAILED)
    if warnings:
        record["warnings"] = warnings

    # web_fetch：避免把“限流/反爬拦截页正文”当成成功证据继续执行。
    # 说明：
    # - 该工具常用于冷启动抓取；若继续在拦截页上做解析/产物落盘，很容易诱发“编数据”或输出无效结果；
    # - 这里选择“记录证据 + 标记本步失败”，让上层 replan 换源/退避/提示用户提供 API Key。
    if tool_name_value == TOOL_NAME_WEB_FETCH:
        block_reason = _detect_web_fetch_block_reason(output_text)
        if block_reason:
            warn_text = f"web_fetch 可能被限流/反爬拦截：{block_reason}"
            if isinstance(record.get("warnings"), list):
                record["warnings"].append(warn_text)
            else:
                record["warnings"] = [warn_text]

            url_text = truncate_inline_text(str(tool_input), 180)
            preview = truncate_inline_text(output_text, 260)
            tail = f" {preview}" if preview else ""
            error_code = "rate_limited" if block_reason == "too_many_requests" else "web_fetch_blocked"
            return record, format_task_error(
                code=error_code,
                message=f"web_fetch 可能被限流/反爬（{block_reason}）：{url_text}{tail}",
            )

        semantic_error = _detect_web_fetch_semantic_error(output_text)
        if semantic_error:
            warn_text = f"web_fetch 语义失败：{semantic_error}"
            if isinstance(record.get("warnings"), list):
                record["warnings"].append(warn_text)
            else:
                record["warnings"] = [warn_text]

            url_text = truncate_inline_text(str(tool_input), 180)
            semantic_lower = str(semantic_error).lower()
            error_code = "missing_api_key" if ("missing_access_key" in semantic_lower or "access key" in semantic_lower) else "web_fetch_blocked"
            return record, format_task_error(
                code=error_code,
                message=f"web_fetch 返回错误响应（{semantic_error}）：{url_text}",
            )

    # 新工具：此处只负责“真实执行 + 记录调用”，不直接沉淀为 skill。
    # 说明：
    # - 新工具会先以 tools_items.metadata.approval.status=draft 形式保存；
    # - 只有在 run 成功结束且 Eval Agent 评估通过后，才会自动批准并生成 tool skill。
    if tool_was_missing:
        _safe_write_debug(
            int(task_id),
            int(run_id),
            message="tool.created_draft",
            data={"tool_id": record.get("tool_id"), "tool_name": record.get("tool_name")},
            level="info",
        )

    return record, None
