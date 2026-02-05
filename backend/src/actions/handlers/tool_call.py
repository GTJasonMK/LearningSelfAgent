import logging
import os
import shlex
from typing import Optional, Tuple

from backend.src.constants import (
    ACTION_TYPE_TOOL_CALL,
    AUTO_TOOL_DESCRIPTION_TEMPLATE,
    AUTO_TOOL_PREFIX,
    DEFAULT_TOOL_VERSION,
    ERROR_MESSAGE_PROMPT_RENDER_FAILED,
    TOOL_METADATA_SOURCE_AUTO,
)
from backend.src.services.execution.shell_command import run_shell_command
from backend.src.services.debug.debug_output import write_task_debug_output
from backend.src.services.tools.tool_records import create_tool_record as _create_tool_record
from backend.src.repositories.tools_repo import (
    get_tool,
    get_tool_by_name,
    get_tool_metadata_by_id,
    get_tool_metadata_by_name,
)
from backend.src.services.permissions.permissions_store import is_tool_enabled

logger = logging.getLogger(__name__)

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


def _safe_write_debug(
    task_id: int,
    run_id: int,
    *,
    message: str,
    data: Optional[dict] = None,
    level: str = "debug",
) -> None:
    """
    工具链路的调试输出不应影响主链路：失败时降级为 logger.exception。
    """
    try:
        write_task_debug_output(
            task_id=int(task_id),
            run_id=int(run_id),
            message=message,
            data=data if isinstance(data, dict) else None,
            level=level,
        )
    except Exception:
        logger.exception("write_task_debug_output failed: %s", message)


def _load_tool_metadata_from_db(tool_id: Optional[int], tool_name: Optional[str]) -> Optional[dict]:
    """
    读取 tools_items.metadata（JSON）并解析为 dict。
    """
    if tool_id is None and not tool_name:
        return None
    if tool_id is not None:
        return get_tool_metadata_by_id(tool_id=int(tool_id))
    return get_tool_metadata_by_name(name=str(tool_name or ""))


def _resolve_tool_exec_spec(payload: dict) -> Optional[dict]:
    """
    优先从 payload.tool_metadata 读取 exec，其次从 tools_items.metadata 读取 exec。
    """
    meta = payload.get("tool_metadata")
    if isinstance(meta, dict):
        exec_spec = meta.get("exec")
        if isinstance(exec_spec, dict):
            exec_spec = _normalize_exec_spec(exec_spec)
            # 兼容：模型可能会输出空 exec {}。这种情况下不要“抢占”掉 DB 里的 exec。
            has_any = bool(
                str(exec_spec.get("type") or "").strip()
                or (isinstance(exec_spec.get("args"), list) and exec_spec.get("args"))
                or str(exec_spec.get("command") or "").strip()
            )
            if has_any:
                return exec_spec
    meta = _load_tool_metadata_from_db(payload.get("tool_id"), payload.get("tool_name"))
    if not isinstance(meta, dict):
        return None
    exec_spec = meta.get("exec")
    return _normalize_exec_spec(exec_spec) if isinstance(exec_spec, dict) else None


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

    result, error_message = run_shell_command(
        {
            "command": cmd_value,
            "workdir": workdir,
            "timeout_ms": timeout_ms,
            "stdin": tool_input if not uses_input_placeholder else "",
        }
    )
    if error_message:
        return None, error_message
    if not isinstance(result, dict):
        return None, "工具执行返回格式异常"

    stdout = str(result.get("stdout") or "")
    stderr = str(result.get("stderr") or "")
    ok = bool(result.get("ok"))
    if not ok:
        detail = stderr.strip() or stdout.strip() or str(result.get("returncode"))
        return None, f"工具执行失败: {detail}"
    output_text = stdout.strip() or stderr.strip()
    return output_text or "", None


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

    output_text, exec_error = _execute_tool_with_exec_spec(exec_spec, str(tool_input))
    if exec_error:
        raise ValueError(exec_error)

    output_text = str(output_text or "")
    # 新工具必须“可观察地成功”：至少要有非空输出，避免把“没跑通/没产出”的工具注册到库里。
    # 说明：对已存在工具允许 exec.allow_empty_output=true（某些工具用文件落盘而非 stdout），
    # 但新工具自举阶段不允许为空，否则 Eval 很难基于证据审查工具是否可用。
    if tool_was_missing and not output_text.strip():
        raise ValueError("新创建工具自测失败：执行成功但输出为空（请让工具输出可解析结果/关键日志）")
    if not output_text.strip() and not allow_empty_output:
        raise ValueError("工具输出为空（请让工具打印关键结果或设置 exec.allow_empty_output=true）")

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
