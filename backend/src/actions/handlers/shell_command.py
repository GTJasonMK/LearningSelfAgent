import os
import re
from typing import List, Optional, Set, Tuple

from backend.src.common.python_code import (
    can_compile_python_source,
    has_risky_inline_control_flow,
    normalize_python_c_source,
)
from backend.src.common.path_utils import normalize_windows_abs_path_on_posix
from backend.src.common.task_error_codes import format_task_error
from backend.src.actions.handlers.common_utils import (
    load_json_object,
    parse_command_tokens,
    resolve_path_with_workdir,
)
from backend.src.constants import (
    AGENT_EXPERIMENT_DIR_REL,
    ERROR_MESSAGE_COMMAND_FAILED,
    ERROR_MESSAGE_PERMISSION_DENIED,
    SHELL_COMMAND_AUTO_REWRITE_COMPLEX_PYTHON_C_DEFAULT,
    SHELL_COMMAND_DISALLOW_COMPLEX_PYTHON_C_DEFAULT,
    SHELL_COMMAND_REQUIRE_FILE_WRITE_BINDING_DEFAULT,
)

# python -c 代码复杂度判断阈值（字符数）
_COMPLEX_PYTHON_C_CODE_LENGTH_THRESHOLD = 220
from backend.src.repositories.task_steps_repo import list_task_steps_for_run
from backend.src.services.permissions.permissions_store import has_exec_permission
from backend.src.services.execution.shell_command import run_shell_command


def _detect_unsupported_shell_operator(command: object) -> str:
    """
    shell_command 仅支持“单条命令 + 参数”语义，不支持 shell 连接符。

    背景：
    - 当前执行器走 subprocess(argv)；`&&/||/;` 不会被当作 shell 语法执行，
      而会污染脚本参数，触发误导性的参数契约错误。
    - 提前在编排层报错，可促使模型拆成多个步骤，避免进入“伪重试循环”。
    """
    tokens = parse_command_tokens(command)
    for token in tokens:
        value = str(token or "").strip()
        if value in {"&&", "||", ";", "|"}:
            return value
    return ""


def _is_python_executable(token: str) -> bool:
    name = os.path.splitext(os.path.basename(str(token or "").strip()))[0].lower()
    return name in {"python", "python3", "py"}


def _looks_like_script_path(token: str) -> bool:
    text = str(token or "").strip().lower()
    return text.endswith((".py", ".sh", ".ps1", ".bat", ".cmd"))


def _extract_script_candidates(command: object) -> List[str]:
    args = parse_command_tokens(command)
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
    args = parse_command_tokens(command)
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
    if len(text) >= _COMPLEX_PYTHON_C_CODE_LENGTH_THRESHOLD:
        return True

    lowered = text.lower()
    has_compound = bool(re.search(r"\b(with|try|except|finally|if|for|while|def|class|lambda)\b", lowered))
    if has_compound and (";" in text or "\n" in text):
        return True

    if ": with " in lowered or ": for " in lowered or ": if " in lowered:
        return True
    return False


def _normalize_python_c_script_source(code: str) -> str:
    """
    把模型常见的“单行复杂 python -c”转换为更稳定的脚本文本。

    处理目标：
    - `; with ...` / `; for ...` 这类复合语句拼接；
    - `if ...: with ...` 这类非法同一行复合语句；
    - 兜底把分号拆行，降低 SyntaxError 概率。
    """
    return normalize_python_c_source(code, compile_name="<auto_python_c>")


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

    if has_risky_inline_control_flow(code):
        return (
            payload,
            None,
            (
                f"{ERROR_MESSAGE_COMMAND_FAILED}:检测到高风险单行控制流 python -c"
                "（为避免语义漂移，请先 file_write 脚本，再用 shell_command 执行）"
            ),
        )

    args = parse_command_tokens(payload.get("command"))
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
    if not can_compile_python_source(normalized_code, filename="<auto_python_c>"):
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

        detail_obj = load_json_object(row_data.get("detail"))
        action_type = str(detail_obj.get("type") or "").strip().lower() if isinstance(detail_obj, dict) else ""
        if action_type not in {"file_write", "file_append"}:
            continue

        payload_obj = detail_obj.get("payload") if isinstance(detail_obj, dict) else None
        result_obj = load_json_object(row_data.get("result"))

        path_text = ""
        if isinstance(result_obj, dict):
            path_text = str(result_obj.get("path") or "").strip()
        if not path_text and isinstance(payload_obj, dict):
            path_text = str(payload_obj.get("path") or "").strip()
        resolved = resolve_path_with_workdir(path_text, workdir)
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
            resolved_auto = resolve_path_with_workdir(str(item or "").strip(), workdir)
            if resolved_auto:
                auto_bound_paths.add(os.path.normcase(resolved_auto))

    missing_paths: List[str] = []
    unbound_paths: List[str] = []
    for candidate in candidates:
        absolute_path = resolve_path_with_workdir(candidate, workdir)
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

    args = parse_command_tokens(patched.get("command"))
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

    args = parse_command_tokens(payload.get("command"))
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
        # 去硬编码：不再注入固定公网地址，避免把“参数缺失”伪装成“抓取成功”。
        return None, ""

    patched = dict(payload)
    command_raw = payload.get("command")
    if isinstance(command_raw, list):
        patched["command"] = list(command_raw) + [default_url]
        return patched, default_url

    if isinstance(command_raw, str) and command_raw.strip():
        patched["command"] = f"{command_raw} {shlex.quote(default_url)}"
        return patched, default_url

    return None, ""


_WARNING_ONLY_LINE_RE = re.compile(r"(?:^warning:|deprecationwarning)", re.IGNORECASE)


def _build_shell_failure_detail(*, stdout: str, stderr: str, returncode: object) -> str:
    """
    生成 shell 失败摘要：优先保留真实错误信息，避免 warning 噪声遮蔽。
    """
    lines: List[str] = []
    for chunk in (str(stderr or ""), str(stdout or "")):
        for raw in chunk.splitlines():
            text = str(raw or "").strip()
            if text:
                lines.append(text)

    if not lines:
        return str(returncode) if returncode is not None else ""

    def _is_warning_only(text: str) -> bool:
        value = str(text or "").strip()
        if not value:
            return True
        if _WARNING_ONLY_LINE_RE.search(value):
            return True
        if value.startswith("^"):
            return True
        return False

    candidates = [line for line in lines if not _is_warning_only(line)] or list(lines)
    # 优先返回“最终异常行”（例如 ValueError/TypeError），避免只拿到 Traceback 头信息。
    exception_line_re = re.compile(r"^[A-Za-z_][\w.]+(?:Error|Exception):\s+.+$")
    for line in reversed(candidates):
        if exception_line_re.match(str(line or "").strip()):
            return line

    priority_markers = (
        "error",
        "failed",
        "exception",
        "traceback",
        "not found",
        "missing",
        "denied",
        "timeout",
    )
    for line in candidates:
        lowered = str(line).lower()
        if any(marker in lowered for marker in priority_markers):
            return line

    return candidates[0]



def execute_shell_command(
    task_id: int,
    run_id: int,
    step_row: Optional[dict],
    payload: dict,
    *,
    context: Optional[dict] = None,
) -> Tuple[Optional[dict], Optional[str]]:
    ctx: dict = context if isinstance(context, dict) else {}
    payload = dict(payload or {})

    command = payload.get("command")
    if isinstance(command, str):
        if not command.strip():
            raise ValueError("shell_command.command 不能为空")
    elif isinstance(command, list):
        if not command:
            raise ValueError("shell_command.command 不能为空")
    else:
        raise ValueError("shell_command.command 不能为空")

    operator = _detect_unsupported_shell_operator(command)
    if operator:
        raise ValueError(
            format_task_error(
                code="shell_operators_not_supported",
                message=(
                    f"{ERROR_MESSAGE_COMMAND_FAILED}: shell_command 不支持连接符 `{operator}`；"
                    "请拆分为多个 shell_command 步骤，或改用脚本文件执行。"
                ),
            )
        )

    # 权限门禁前置：先校验 execute 权限，再进行任何自动落盘（如 python -c 重写脚本）。
    # 避免“权限被拒绝但仍写入 _auto_python_c_*.py”的副作用。
    workdir = str(payload.get("workdir") or "").strip() or os.getcwd()
    payload["workdir"] = workdir
    if not has_exec_permission(workdir):
        raise ValueError(ERROR_MESSAGE_PERMISSION_DENIED)

    payload, _rewritten_script_path, rewrite_error = _maybe_rewrite_complex_python_c_payload(
        task_id=int(task_id),
        run_id=int(run_id),
        step_row=step_row,
        payload=payload,
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
            else:
                raise ValueError(
                    format_task_error(
                        code="missing_url_input",
                        message=f"{ERROR_MESSAGE_COMMAND_FAILED}: 缺少 URL 输入，且当前上下文无可复用来源 URL",
                    )
                )

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
        detail = _build_shell_failure_detail(stdout=stdout, stderr=stderr, returncode=rc)
        detail = detail.strip()

        # 检测 Python 依赖缺失（ModuleNotFoundError / ImportError）
        combined = f"{stderr}\n{stdout}"
        dep_match = re.search(
            r"(?:ModuleNotFoundError|ImportError):\s*No module named\s+['\"]?(\S+?)['\"]?\s*$",
            combined,
            re.MULTILINE,
        )
        if dep_match:
            module_name = dep_match.group(1).strip("'\"")
            raise ValueError(
                format_task_error(
                    code="dependency_missing",
                    message=f"{ERROR_MESSAGE_COMMAND_FAILED}: 缺少依赖 {module_name}（{dep_match.group(0).strip()}）",
                )
            )

        combined_lower = combined.lower()
        # DNS 解析失败：这类错误通常与当前环境网络能力相关，应提示换源/重试而非盲目重复同命令。
        if any(
            marker in combined_lower
            for marker in (
                "could not resolve host",
                "temporary failure in name resolution",
                "name or service not known",
                "nodename nor servname provided",
            )
        ):
            raise ValueError(
                format_task_error(
                    code="dns_resolution_failed",
                    message=f"{ERROR_MESSAGE_COMMAND_FAILED}: DNS 解析失败（{detail or '无法解析目标域名'}）",
                )
            )

        # 参数契约错配（脚本定义与命令传参不一致）：常见于位置参数/命名参数混用。
        if (
            re.search(r"invalid isoformat string:\s*['\"]--[a-z0-9_-]+", combined, re.IGNORECASE)
            or "unrecognized arguments" in combined_lower
            or "the following arguments are required" in combined_lower
        ):
            raise ValueError(
                format_task_error(
                    code="script_arg_contract_mismatch",
                    message=f"{ERROR_MESSAGE_COMMAND_FAILED}: 脚本参数契约不匹配（{detail or '请检查脚本入参与命令是否一致'}）",
                )
            )

        raise ValueError(f"{ERROR_MESSAGE_COMMAND_FAILED}:{detail}" if detail else ERROR_MESSAGE_COMMAND_FAILED)
    return result, None
