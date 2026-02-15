import json
import os
import re
import shlex
from typing import List, Optional, Set, Tuple

from backend.src.common.path_utils import normalize_windows_abs_path_on_posix
from backend.src.constants import (
    AGENT_EXPERIMENT_DIR_REL,
    ERROR_MESSAGE_COMMAND_FAILED,
    SHELL_COMMAND_AUTO_REWRITE_COMPLEX_PYTHON_C_DEFAULT,
    SHELL_COMMAND_DISALLOW_COMPLEX_PYTHON_C_DEFAULT,
    SHELL_COMMAND_REQUIRE_FILE_WRITE_BINDING_DEFAULT,
)
from backend.src.repositories.task_steps_repo import list_task_steps_for_run
from backend.src.services.execution.shell_command import run_shell_command


def _load_json_object(value: object) -> Optional[dict]:
    if isinstance(value, dict):
        return dict(value)
    if not isinstance(value, str):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except Exception:
        return None
    return dict(parsed) if isinstance(parsed, dict) else None


def _parse_command_args(command: object) -> List[str]:
    if isinstance(command, list):
        return [str(item) for item in command if str(item).strip()]
    if isinstance(command, str):
        text = str(command).strip()
        if not text:
            return []
        args = shlex.split(text, posix=os.name != "nt")
        if os.name != "nt":
            return [str(item) for item in args]
        cleaned: List[str] = []
        for item in args:
            token = str(item)
            if len(token) >= 2 and ((token[0] == token[-1] == '"') or (token[0] == token[-1] == "'")):
                token = token[1:-1]
            cleaned.append(token)
        return cleaned
    return []


def _is_python_executable(token: str) -> bool:
    name = os.path.splitext(os.path.basename(str(token or "").strip()))[0].lower()
    return name in {"python", "python3", "py"}


def _looks_like_script_path(token: str) -> bool:
    text = str(token or "").strip().lower()
    return text.endswith((".py", ".sh", ".ps1", ".bat", ".cmd"))


def _extract_script_candidates(command: object) -> List[str]:
    args = _parse_command_args(command)
    if not args:
        return []
    head = str(args[0] or "").strip()
    if not head:
        return []
    if _is_python_executable(head):
        if len(args) >= 2 and str(args[1] or "").strip() in {"-c", "-m"}:
            return []
        for token in args[1:]:
            current = str(token or "").strip()
            if not current or current.startswith("-"):
                continue
            return [current] if _looks_like_script_path(current) else []
        return []
    return [head] if _looks_like_script_path(head) else []


def _extract_python_c_code(command: object) -> str:
    args = _parse_command_args(command)
    if len(args) < 3:
        return ""
    head = str(args[0] or "").strip()
    if not _is_python_executable(head):
        return ""
    if str(args[1] or "").strip() != "-c":
        return ""
    return str(args[2] or "").strip()


def _is_complex_python_c_code(code: str) -> bool:
    text = str(code or "").strip()
    if not text:
        return False
    if len(text) >= 220:
        return True

    lowered = text.lower()
    has_compound = bool(re.search(r"\b(with|try|except|finally|if|for|while|def|class|lambda)\b", lowered))
    if has_compound and (";" in text or "\n" in text):
        return True

    if ": with " in lowered or ": for " in lowered or ": if " in lowered:
        return True
    return False


def _can_compile_python_code(code: str) -> bool:
    source = str(code or "").strip()
    if not source:
        return False
    try:
        compile(source, "<auto_python_c>", "exec")
        return True
    except Exception:
        return False


def _normalize_python_c_script_source(code: str) -> str:
    """
    把模型常见的“单行复杂 python -c”转换为更稳定的脚本文本。

    处理目标：
    - `; with ...` / `; for ...` 这类复合语句拼接；
    - `if ...: with ...` 这类非法同一行复合语句；
    - 兜底把分号拆行，降低 SyntaxError 概率。
    """
    source = str(code or "").strip()
    if not source:
        return source
    if _can_compile_python_code(source):
        return source

    rewritten = re.sub(
        r";\s*(?=(with|for|if|try|while|def|class|async\s+def|elif|else|except|finally)\b)",
        "\n",
        source,
    )
    rewritten = re.sub(r":\s*(?=(with|for|if|try|while|def|class|async\s+def)\b)", ":\n    ", rewritten)
    rewritten = re.sub(r"\n[ \t]+(?=(elif|else|except|finally)\b)", "\n", rewritten)
    return rewritten


def _has_risky_inline_control_flow(code: str) -> bool:
    """
    检测高风险“单行复合控制流”：
    - 这类 python -c 常由模型压缩生成，自动改写极易引入语义漂移；
    - 例如：`for ...: ...; if ...: ...; ...`。
    """
    text = str(code or "").strip()
    if not text:
        return False
    if ";" not in text:
        return False
    if "\n" in text:
        return False

    # 同一行出现多个控制块头，且通过分号串联，风险高。
    block_headers = re.findall(r"\b(if|for|while|with|try|except|finally|elif|else)\b[^:]*:", text)
    if len(block_headers) >= 2:
        return True

    # 循环体里继续嵌套控制流，自动改写很容易错缩进。
    if re.search(
        r"\b(for|while)\b[^:]*:\s*[^;]+;\s*(if|for|while|with|try|except|finally|elif|else)\b",
        text,
        re.IGNORECASE,
    ):
        return True

    return False


def _resolve_workspace_dir(workdir: str, context: Optional[dict]) -> str:
    workspace_rel = normalize_windows_abs_path_on_posix(
        str((context or {}).get("agent_workspace_rel") or AGENT_EXPERIMENT_DIR_REL).strip()
    )
    if not workspace_rel:
        workspace_rel = str(AGENT_EXPERIMENT_DIR_REL).strip()

    base = normalize_windows_abs_path_on_posix(str(workdir or "").strip())
    if not base:
        base = os.getcwd()
    if not os.path.isabs(base):
        base = os.path.abspath(base)

    if os.path.isabs(workspace_rel):
        return os.path.abspath(workspace_rel)
    return os.path.abspath(os.path.join(base, workspace_rel))


def _maybe_rewrite_complex_python_c_payload(
    *,
    task_id: int,
    run_id: int,
    step_row: Optional[dict],
    payload: dict,
    context: Optional[dict],
) -> Tuple[dict, Optional[str], Optional[str]]:
    strict = (context or {}).get("disallow_complex_python_c")
    if strict is None:
        strict = SHELL_COMMAND_DISALLOW_COMPLEX_PYTHON_C_DEFAULT

    auto_rewrite = (context or {}).get("auto_rewrite_complex_python_c")
    if auto_rewrite is None:
        auto_rewrite = SHELL_COMMAND_AUTO_REWRITE_COMPLEX_PYTHON_C_DEFAULT

    code = _extract_python_c_code(payload.get("command"))
    if not code or not _is_complex_python_c_code(code):
        return payload, None, None

    if not bool(strict):
        return payload, None, None

    if not bool(auto_rewrite):
        return payload, None, None

    if _has_risky_inline_control_flow(code):
        return (
            payload,
            None,
            (
                f"{ERROR_MESSAGE_COMMAND_FAILED}:检测到高风险单行控制流 python -c"
                "（为避免语义漂移，请先 file_write 脚本，再用 shell_command 执行）"
            ),
        )

    args = _parse_command_args(payload.get("command"))
    if len(args) < 3:
        return payload, None, None

    python_exec = str(args[0] or "").strip() or "python"
    extra_args = [str(item) for item in args[3:]]
    workdir = str(payload.get("workdir") or "").strip() or os.getcwd()
    workspace_dir = _resolve_workspace_dir(workdir, context)

    step_id = "x"
    if isinstance(step_row, dict):
        try:
            if step_row.get("id") is not None:
                step_id = str(int(step_row.get("id")))
        except Exception:
            step_id = "x"

    script_name = f"_auto_python_c_t{int(task_id)}_r{int(run_id)}_s{step_id}.py"
    script_path = os.path.abspath(os.path.join(workspace_dir, script_name))
    normalized_code = _normalize_python_c_script_source(code)
    if not _can_compile_python_code(normalized_code):
        return (
            payload,
            None,
            (
                f"{ERROR_MESSAGE_COMMAND_FAILED}:复杂 python -c 自动重写后仍不可执行"
                "（请先 file_write 脚本，再用 shell_command 执行）"
            ),
        )

    try:
        os.makedirs(os.path.dirname(script_path), exist_ok=True)
        with open(script_path, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(normalized_code)
            if not normalized_code.endswith("\n"):
                handle.write("\n")
    except Exception as exc:
        return payload, None, f"{ERROR_MESSAGE_COMMAND_FAILED}:自动重写 python -c 失败: {exc}"

    patched = dict(payload)
    patched["command"] = [python_exec, script_path] + extra_args
    patched.setdefault("workdir", workdir)

    if isinstance(context, dict):
        raw_bound = context.get("shell_dependency_auto_bind_paths")
        bound_paths: List[str] = []
        if isinstance(raw_bound, list):
            for item in raw_bound:
                value = str(item or "").strip()
                if value:
                    bound_paths.append(value)
        if script_path not in bound_paths:
            bound_paths.append(script_path)
        context["shell_dependency_auto_bind_paths"] = bound_paths
        context["shell_auto_rewrite_last_script"] = script_path

    return patched, script_path, None


def _enforce_python_c_policy(payload: dict, context: Optional[dict]) -> Optional[str]:
    strict = (context or {}).get("disallow_complex_python_c")
    if strict is None:
        strict = SHELL_COMMAND_DISALLOW_COMPLEX_PYTHON_C_DEFAULT
    if not bool(strict):
        return None

    code = _extract_python_c_code(payload.get("command"))
    if not code:
        return None
    if _is_complex_python_c_code(code):
        return (
            f"{ERROR_MESSAGE_COMMAND_FAILED}:检测到复杂 python -c 命令"
            "（请先 file_write 脚本，再用 shell_command 执行脚本）"
        )
    return None


def _resolve_path(raw_path: str, workdir: str) -> str:
    path_text = normalize_windows_abs_path_on_posix(str(raw_path or "").strip())
    if not path_text:
        return ""
    if os.path.isabs(path_text):
        return os.path.abspath(path_text)
    base = normalize_windows_abs_path_on_posix(str(workdir or "").strip())
    if not base:
        base = os.getcwd()
    if not os.path.isabs(base):
        base = os.path.abspath(base)
    return os.path.abspath(os.path.join(base, path_text))


def _collect_written_script_paths(
    *,
    task_id: int,
    run_id: int,
    workdir: str,
    current_step_id: Optional[int],
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
            row_data = dict(row)
        except Exception:
            continue

        try:
            row_id = int(row_data.get("id")) if row_data.get("id") is not None else None
        except Exception:
            row_id = None
        if current_step_id is not None and row_id == int(current_step_id):
            continue

        status = str(row_data.get("status") or "").strip().lower()
        if status != "done":
            continue

        detail_obj = _load_json_object(row_data.get("detail"))
        action_type = str(detail_obj.get("type") or "").strip().lower() if isinstance(detail_obj, dict) else ""
        if action_type not in {"file_write", "file_append"}:
            continue

        payload_obj = detail_obj.get("payload") if isinstance(detail_obj, dict) else None
        result_obj = _load_json_object(row_data.get("result"))

        path_text = ""
        if isinstance(result_obj, dict):
            path_text = str(result_obj.get("path") or "").strip()
        if not path_text and isinstance(payload_obj, dict):
            path_text = str(payload_obj.get("path") or "").strip()
        resolved = _resolve_path(path_text, workdir)
        if not resolved:
            continue
        paths.add(os.path.normcase(resolved))

    return paths


def _enforce_script_dependency(
    *,
    task_id: int,
    run_id: int,
    step_row: Optional[dict],
    payload: dict,
    context: Optional[dict],
) -> Optional[str]:
    strict = (context or {}).get("enforce_shell_script_dependency")
    if strict is None:
        strict = SHELL_COMMAND_REQUIRE_FILE_WRITE_BINDING_DEFAULT
    if not bool(strict):
        return None

    candidates = _extract_script_candidates(payload.get("command"))
    if not candidates:
        return None

    workdir = str(payload.get("workdir") or "").strip() or os.getcwd()
    current_step_id = None
    if isinstance(step_row, dict):
        try:
            if step_row.get("id") is not None:
                current_step_id = int(step_row.get("id"))
        except Exception:
            current_step_id = None

    written_paths = _collect_written_script_paths(
        task_id=int(task_id),
        run_id=int(run_id),
        workdir=workdir,
        current_step_id=current_step_id,
    )

    auto_bound_paths: Set[str] = set()
    raw_auto_bound = (context or {}).get("shell_dependency_auto_bind_paths")
    if isinstance(raw_auto_bound, list):
        for item in raw_auto_bound:
            resolved_auto = _resolve_path(str(item or "").strip(), workdir)
            if resolved_auto:
                auto_bound_paths.add(os.path.normcase(resolved_auto))

    missing_paths: List[str] = []
    unbound_paths: List[str] = []
    for candidate in candidates:
        absolute_path = _resolve_path(candidate, workdir)
        if not absolute_path:
            continue
        if not os.path.exists(absolute_path):
            missing_paths.append(absolute_path)
            continue
        normalized_path = os.path.normcase(absolute_path)
        if normalized_path in auto_bound_paths:
            continue
        if normalized_path not in written_paths:
            unbound_paths.append(absolute_path)

    if missing_paths:
        return (
            f"{ERROR_MESSAGE_COMMAND_FAILED}:脚本不存在: {', '.join(missing_paths)}"
            "（请先通过 file_write 创建并确认落盘）"
        )
    if unbound_paths:
        return (
            f"{ERROR_MESSAGE_COMMAND_FAILED}:脚本依赖未绑定: {', '.join(unbound_paths)}"
            "（当前 run 未发现对应的 file_write/file_append 成功步骤）"
        )
    return None


def _is_validation_step_title(step_row: Optional[dict]) -> bool:
    title = ""
    if isinstance(step_row, dict):
        title = str(step_row.get("title") or "")
    lowered = title.lower()
    return (
        "验证" in title
        or "校验" in title
        or "检查" in title
        or "自测" in title
        or "verify" in lowered
        or "test" in lowered
        or "smoke" in lowered
    )


def _extract_missing_url_failure_reason(result: dict) -> str:
    stderr = str((result or {}).get("stderr") or "")
    stdout = str((result or {}).get("stdout") or "")
    detail = f"{stderr}\n{stdout}".lower()
    if not detail.strip():
        return ""

    for marker in ("no url provided", "url required", "缺少url"):
        if marker in detail:
            return marker
    return ""


def _looks_like_missing_url_failure(result: dict) -> bool:
    return bool(_extract_missing_url_failure_reason(result))


def _extract_missing_input_failure_reason(result: dict) -> str:
    stderr = str((result or {}).get("stderr") or "")
    stdout = str((result or {}).get("stdout") or "")
    detail = f"{stderr}\n{stdout}".lower()
    if not detail.strip():
        return ""

    for marker in (
        "未提供输入数据",
        "missing input data",
        "no input data",
        "no input provided",
        "stdin is empty",
        "input required",
    ):
        if marker in detail:
            return marker
    return ""


def _build_retry_payload_with_context_stdin(
    *,
    payload: dict,
    context: Optional[dict],
) -> Tuple[Optional[dict], int]:
    if not isinstance(context, dict):
        return None, 0
    parse_text = str(context.get("latest_parse_input_text") or "").strip()
    if not parse_text:
        return None, 0

    existing_stdin = payload.get("stdin")
    if isinstance(existing_stdin, str) and existing_stdin.strip():
        return None, 0

    patched = dict(payload)
    patched["stdin"] = parse_text
    return patched, len(parse_text)


def _maybe_attach_context_stdin_before_run(
    *,
    payload: dict,
    context: Optional[dict],
) -> Tuple[dict, int]:
    """
    对“仅执行脚本且未传入参数/stdin”的场景，自动附加最近解析输入作为 stdin。
    """
    patched = dict(payload or {})
    if not isinstance(context, dict):
        return patched, 0

    existing_stdin = patched.get("stdin")
    if isinstance(existing_stdin, str) and existing_stdin.strip():
        return patched, 0

    parse_text = str(context.get("latest_parse_input_text") or "").strip()
    if not parse_text:
        return patched, 0

    args = _parse_command_args(patched.get("command"))
    if not args:
        return patched, 0

    script_only = False
    head = str(args[0] or "").strip()
    if _is_python_executable(head) and len(args) == 2:
        script_token = str(args[1] or "").strip()
        script_only = script_token.lower().endswith(".py")
    elif len(args) == 1:
        script_only = head.lower().endswith(".py")

    if not script_only:
        return patched, 0

    patched["stdin"] = parse_text
    return patched, len(parse_text)


def _build_retry_payload_with_default_url(
    *,
    payload: dict,
    step_row: Optional[dict],
    context: Optional[dict],
) -> Tuple[Optional[dict], str]:
    if not _is_validation_step_title(step_row):
        return None, ""

    args = _parse_command_args(payload.get("command"))
    if len(args) != 2:
        return None, ""
    if not _is_python_executable(str(args[0] or "")):
        return None, ""

    script_token = str(args[1] or "").strip()
    if not script_token.lower().endswith(".py"):
        return None, ""

    script_name = os.path.basename(script_token).lower()
    if not any(k in script_name for k in ("fetch", "crawl", "http", "url", "request")):
        return None, ""

    default_url = str((context or {}).get("shell_default_url") or "").strip()
    if not default_url:
        default_url = str((context or {}).get("latest_external_url") or "").strip()
    if not default_url:
        default_url = "https://example.com"

    patched = dict(payload)
    command_raw = payload.get("command")
    if isinstance(command_raw, list):
        patched["command"] = list(command_raw) + [default_url]
        return patched, default_url

    if isinstance(command_raw, str) and command_raw.strip():
        patched["command"] = f"{command_raw} {shlex.quote(default_url)}"
        return patched, default_url

    return None, ""



def execute_shell_command(
    task_id: int,
    run_id: int,
    step_row: Optional[dict],
    payload: dict,
    *,
    context: Optional[dict] = None,
) -> Tuple[Optional[dict], Optional[str]]:
    ctx: dict = context if isinstance(context, dict) else {}

    command = payload.get("command")
    if isinstance(command, str):
        if not command.strip():
            raise ValueError("shell_command.command 不能为空")
    elif isinstance(command, list):
        if not command:
            raise ValueError("shell_command.command 不能为空")
    else:
        raise ValueError("shell_command.command 不能为空")

    payload, _rewritten_script_path, rewrite_error = _maybe_rewrite_complex_python_c_payload(
        task_id=int(task_id),
        run_id=int(run_id),
        step_row=step_row,
        payload=dict(payload),
        context=ctx,
    )
    if rewrite_error:
        raise ValueError(rewrite_error)

    python_c_error = _enforce_python_c_policy(payload, ctx)
    if python_c_error:
        raise ValueError(python_c_error)

    dependency_error = _enforce_script_dependency(
        task_id=int(task_id),
        run_id=int(run_id),
        step_row=step_row,
        payload=payload,
        context=ctx,
    )
    if dependency_error:
        raise ValueError(dependency_error)

    payload, auto_attached_stdin_len = _maybe_attach_context_stdin_before_run(payload=payload, context=ctx)
    result, error_message = run_shell_command(payload)
    if error_message:
        raise ValueError(error_message)
    if not isinstance(result, dict):
        raise ValueError(ERROR_MESSAGE_COMMAND_FAILED)
    if auto_attached_stdin_len > 0 and isinstance(result, dict):
        try:
            result = dict(result)
            result["auto_stdin_attached"] = True
            result["auto_stdin_chars"] = int(auto_attached_stdin_len)
        except Exception:
            pass

    ok = bool(result.get("ok"))
    if not ok:
        retry_payload: Optional[dict] = None
        retry_url = ""
        retry_stdin_len = 0
        retry_trigger = ""

        retry_reason = _extract_missing_url_failure_reason(result)
        if retry_reason:
            retry_payload, retry_url = _build_retry_payload_with_default_url(
                payload=payload,
                step_row=step_row,
                context=ctx,
            )
            if isinstance(retry_payload, dict):
                retry_trigger = "missing_url"

        if not isinstance(retry_payload, dict):
            retry_reason = _extract_missing_input_failure_reason(result)
            if retry_reason:
                retry_payload, retry_stdin_len = _build_retry_payload_with_context_stdin(
                    payload=payload,
                    context=ctx,
                )
                if isinstance(retry_payload, dict):
                    retry_trigger = "missing_input_data"

        if isinstance(retry_payload, dict):
            retry_result, retry_error = run_shell_command(retry_payload)
            if not retry_error and isinstance(retry_result, dict) and bool(retry_result.get("ok")):
                merged_result = dict(retry_result)
                auto_retry_payload = {
                    "trigger": retry_trigger or "auto_retry",
                    "reason": retry_reason or retry_trigger or "auto_retry",
                    "fallback_url": retry_url,
                    "stdin_chars": int(retry_stdin_len or 0),
                    "initial_returncode": result.get("returncode"),
                    "initial_stdout": str(result.get("stdout") or ""),
                    "initial_stderr": str(result.get("stderr") or ""),
                    "retry_command": retry_payload.get("command"),
                }
                if retry_stdin_len > 0:
                    auto_retry_payload["retry_stdin_attached"] = True
                merged_result["auto_retry"] = auto_retry_payload
                if retry_url:
                    ctx["shell_default_url"] = retry_url
                return merged_result, None

        stdout = str(result.get("stdout") or "").strip()
        stderr = str(result.get("stderr") or "").strip()
        rc = result.get("returncode")
        detail = stderr or stdout or (str(rc) if rc is not None else "")
        detail = detail.strip()
        raise ValueError(f"{ERROR_MESSAGE_COMMAND_FAILED}:{detail}" if detail else ERROR_MESSAGE_COMMAND_FAILED)
    return result, None
