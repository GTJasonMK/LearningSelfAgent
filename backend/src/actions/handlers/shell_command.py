import csv
import json
from pathlib import Path
import os
import re
import shlex
import sys
import ast
from typing import Dict, List, Optional, Set, Tuple

from backend.src.common.python_code import (
    can_compile_python_source,
    has_risky_inline_control_flow,
    normalize_python_c_source,
)
from backend.src.common.path_utils import normalize_windows_abs_path_on_posix
from backend.src.common.csv_artifact_quality import build_csv_quality_failure_text, load_csv_quality_stats
from backend.src.common.task_error_codes import format_task_error
from backend.src.common.utils import is_test_env, parse_json_value
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
        return format_task_error(
            code="script_missing",
            message=(
                f"{ERROR_MESSAGE_COMMAND_FAILED}:脚本不存在: {', '.join(missing_paths)}"
                "（请先通过 file_write 创建并确认落盘）"
            ),
        )
    if unbound_paths:
        return format_task_error(
            code="script_dependency_unbound",
            message=(
                f"{ERROR_MESSAGE_COMMAND_FAILED}:脚本依赖未绑定: {', '.join(unbound_paths)}"
                "（当前 run 未发现对应的 file_write/file_append 成功步骤）"
            ),
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



_MATERIALIZABLE_SAMPLE_FILE_EXTS = {".html", ".htm", ".json", ".csv", ".tsv", ".txt", ".xml"}


def _extract_missing_input_file_path(result: dict) -> str:
    stderr = str((result or {}).get("stderr") or "")
    stdout = str((result or {}).get("stdout") or "")
    detail = f"{stderr}\n{stdout}"
    if not detail.strip():
        return ""
    patterns = (
        r'No such file or directory:\s*["\']([^"\']+)["\']',
        r'cannot find the file specified:\s*["\']([^"\']+)["\']',
        r'error reading file:\s*\[errno\s*2\]\s*no such file or directory:\s*["\']([^"\']+)["\']',
        r'file\s+([^\s"\']+\.(?:html?|json|csv|tsv|txt|xml))\s+not found',
    )
    for pattern in patterns:
        match = re.search(pattern, detail, re.IGNORECASE)
        if match:
            return str(match.group(1) or "").strip()
    return ""


def _build_retry_payload_with_materialized_context_file(
    *,
    payload: dict,
    result: dict,
    context: Optional[dict],
    workdir: str,
) -> Tuple[Optional[dict], str]:
    if not isinstance(context, dict):
        return None, ""
    parse_text = str(context.get("latest_parse_input_text") or "").strip()
    if not parse_text:
        return None, ""

    missing_path = _extract_missing_input_file_path(result)
    if not missing_path or os.path.isabs(missing_path):
        return None, ""
    ext = os.path.splitext(str(missing_path or ""))[1].lower()
    if ext not in _MATERIALIZABLE_SAMPLE_FILE_EXTS:
        return None, ""

    abs_workdir = resolve_path_with_workdir(".", workdir)
    abs_target = resolve_path_with_workdir(missing_path, workdir)
    if not abs_target or not abs_workdir:
        return None, ""
    try:
        common = os.path.commonpath([abs_workdir, abs_target])
    except Exception:
        return None, ""
    if common != abs_workdir:
        return None, ""

    os.makedirs(os.path.dirname(abs_target) or abs_workdir, exist_ok=True)
    with open(abs_target, "w", encoding="utf-8", newline="") as handle:
        handle.write(parse_text)
    return dict(payload or {}), abs_target
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
_POSITIONAL_REQUIRED_PREFIX = "@pos:"
_CSV_REQUIRED_COLUMNS_RE = re.compile(
    r"missing required columns:\s*([^\n\r]+)",
    re.IGNORECASE,
)
_CSV_ALIAS_DATE_KEYS = (
    "date",
    "timestamp",
    "datetime",
    "time",
    "day",
    "trade_date",
    "trading_date",
    "日期",
    "时间",
)
_CSV_ALIAS_PRICE_KEYS = (
    "price",
    "close",
    "value",
    "amount",
    "cny_per_g",
    "cnyg",
    "price_cny",
    "price_cny_per_gram",
    "price_cny_per_g",
    "金价",
    "价格",
    "收盘",
)


def _looks_like_nonempty_string_list(value: object) -> bool:
    if not isinstance(value, list):
        return False
    for item in value:
        if not isinstance(item, str) or not str(item).strip():
            return False
    return True


def _merge_unique_args(base: List[str], extra: List[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for item in list(base or []) + list(extra or []):
        current = str(item or "").strip()
        if not current or current in seen:
            continue
        seen.add(current)
        out.append(current)
    return out


def _is_cli_option_token(token: str) -> bool:
    value = str(token or "").strip()
    if not value:
        return False
    if not value.startswith("-"):
        return False
    # 保留负数作为位置参数值，避免误判。
    if re.match(r"^-\d", value):
        return False
    return True


def _normalize_script_run_target(script: str, args: List[str]) -> Tuple[str, List[str], Optional[str]]:
    target = str(script or "").strip()
    normalized_args = [str(item) for item in list(args or []) if str(item).strip()]
    if not target:
        return "", normalized_args, None

    # 回归防护：模型有时会把 script 错写为 python/python3/py，
    # 并把真实脚本路径塞进 args[0]。
    if _is_python_executable(target):
        first_arg = str(normalized_args[0] or "").strip() if normalized_args else ""
        if first_arg.lower().endswith(".py"):
            return first_arg, [str(item) for item in normalized_args[1:] if str(item).strip()], None
        return (
            "",
            normalized_args,
            format_task_error(
                code="script_payload_invalid",
                message="shell_command.script 不能是 python 可执行程序，请填写脚本路径（例如 backend/.agent/workspace/x.py）",
            ),
        )
    return target, normalized_args, None


def _build_script_command_payload(payload: dict, context: Optional[dict]) -> Tuple[dict, Optional[str]]:
    patched = dict(payload or {})
    script = str(patched.get("script") or "").strip()
    if not script:
        return patched, None

    args_raw = patched.get("args")
    args: List[str] = []
    if isinstance(args_raw, list):
        args = [str(item) for item in args_raw if str(item).strip()]
    elif isinstance(args_raw, str) and args_raw.strip():
        args = parse_command_tokens(args_raw)

    script, args, target_error = _normalize_script_run_target(script, args)
    if target_error:
        return patched, target_error

    python_exec = str((context or {}).get("python_executable_override") or "").strip()
    if not python_exec:
        python_exec = str(sys.executable or "").strip() or "python"

    patched["script"] = script
    patched["args"] = list(args)
    patched["command"] = [python_exec, script] + args
    return patched, None


def _extract_python_constant_str(node: object) -> str:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return str(node.value)
    if isinstance(node, ast.Str):
        return str(node.s)
    return ""


def _extract_python_constant_str_list(node: object) -> List[str]:
    values: List[str] = []
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        for item in list(node.elts or []):
            text = _extract_python_constant_str(item)
            if text:
                values.append(text)
    return _merge_unique_args([], values)


def _extract_python_constant_bool(node: object) -> Optional[bool]:
    if isinstance(node, ast.Constant) and isinstance(node.value, bool):
        return bool(node.value)
    if isinstance(node, ast.NameConstant):
        value = node.value
        if isinstance(value, bool):
            return bool(value)
    return None


def _extract_python_constant_int(node: object) -> Optional[int]:
    if isinstance(node, ast.Constant) and isinstance(node.value, int) and not isinstance(node.value, bool):
        return int(node.value)
    if isinstance(node, ast.Num) and isinstance(node.n, int):
        return int(node.n)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        inner = _extract_python_constant_int(node.operand)
        if inner is not None:
            return -int(inner)
    return None



def _unwrap_simple_sequence_wrapper(node: ast.AST) -> ast.AST:
    current = node
    while isinstance(current, ast.Call):
        func_name = ''
        if isinstance(current.func, ast.Name):
            func_name = str(current.func.id or '').strip()
        if func_name not in {'list', 'tuple'} or len(list(current.args or [])) != 1:
            break
        current = current.args[0]
    return current



def _is_sys_argv_attr(node: ast.AST) -> bool:
    current = _unwrap_simple_sequence_wrapper(node)
    return (
        isinstance(current, ast.Attribute)
        and isinstance(current.value, ast.Name)
        and str(current.value.id or '').strip() == 'sys'
        and str(current.attr or '').strip() == 'argv'
    )



def _extract_simple_name(target: ast.AST) -> str:
    if isinstance(target, ast.Name):
        return str(target.id or '').strip()
    return ''



def _sanitize_positional_name(raw: object) -> str:
    text = str(raw or '').strip()
    if not text:
        return ''
    text = re.sub(r'^[<\[(\{\s]+|[>\])\}\s]+$', '', text)
    text = re.sub(r'^[<\[(\{\s]+', '', text)
    text = re.sub(r'[>\])\}\s]+$', '', text)
    text = re.sub(r'\s+', '_', text).strip('_')
    return text[:64]



def _extract_usage_placeholder_names(source: str) -> List[str]:
    names: List[str] = []
    for match in re.finditer(r'<([^<>]{1,64})>', str(source or '')):
        name = _sanitize_positional_name(match.group(1))
        if name:
            names.append(name)
    return _merge_unique_args([], names)



def _extract_subscript_index(node: ast.Subscript) -> Optional[int]:
    target = node.slice
    if isinstance(target, ast.Index):
        target = target.value
    return _extract_python_constant_int(target)



def _extract_sys_argv_alias_offset(node: ast.AST) -> Optional[int]:
    current = _unwrap_simple_sequence_wrapper(node)
    if _is_sys_argv_attr(current):
        return 0
    if not isinstance(current, ast.Subscript):
        return None
    base = _unwrap_simple_sequence_wrapper(current.value)
    if not _is_sys_argv_attr(base):
        return None
    target = current.slice
    if isinstance(target, ast.Index):
        target = target.value
    if not isinstance(target, ast.Slice):
        return None
    if target.upper is not None:
        return None
    lower = _extract_python_constant_int(target.lower) if target.lower is not None else 0
    if lower is None or lower < 0:
        return None
    return int(lower)



def _discover_sys_argv_alias_offsets(tree: ast.AST) -> Dict[str, int]:
    aliases: Dict[str, int] = {}
    for node in ast.walk(tree):
        target = None
        value = None
        if isinstance(node, ast.Assign) and len(list(node.targets or [])) == 1:
            target = node.targets[0]
            value = node.value
        elif isinstance(node, ast.AnnAssign):
            target = node.target
            value = node.value
        if target is None or value is None:
            continue
        name = _extract_simple_name(target)
        if not name:
            continue
        offset = _extract_sys_argv_alias_offset(value)
        if offset is None:
            continue
        aliases[name] = int(offset)
    return aliases



def _resolve_sys_argv_container_offset(node: ast.AST, alias_offsets: Dict[str, int]) -> Optional[int]:
    current = _unwrap_simple_sequence_wrapper(node)
    if _is_sys_argv_attr(current):
        return 0
    if isinstance(current, ast.Name):
        name = str(current.id or '').strip()
        if name in alias_offsets:
            return int(alias_offsets[name])
    return None



def _extract_sys_argv_positional_index(node: ast.AST, alias_offsets: Dict[str, int]) -> Optional[int]:
    current = _unwrap_simple_sequence_wrapper(node)
    if not isinstance(current, ast.Subscript):
        return None
    container_offset = _resolve_sys_argv_container_offset(current.value, alias_offsets)
    if container_offset is None:
        return None
    index_value = _extract_subscript_index(current)
    if index_value is None:
        return None
    position = int(index_value) + int(container_offset)
    if container_offset == 0 and position <= 0:
        return None
    if position <= 0:
        return None
    return position



def _extract_sys_argv_len_required_count(node: ast.AST, alias_offsets: Dict[str, int]) -> Optional[int]:
    if not isinstance(node, ast.Compare):
        return None
    if len(list(node.ops or [])) != 1 or len(list(node.comparators or [])) != 1:
        return None
    op = node.ops[0]
    if not isinstance(op, (ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE)):
        return None

    left = node.left
    right = node.comparators[0]
    if not isinstance(left, ast.Call):
        return None
    if not isinstance(left.func, ast.Name) or str(left.func.id or '').strip() != 'len':
        return None
    if len(list(left.args or [])) != 1:
        return None
    offset = _resolve_sys_argv_container_offset(left.args[0], alias_offsets)
    if offset is None:
        return None
    constant = _extract_python_constant_int(right)
    if constant is None:
        return None
    required = int(constant) + int(offset) - 1
    if required <= 0:
        return None
    return required



def _extract_sys_argv_contract(script_path: str) -> Dict[str, object]:
    empty = {
        'known_options': [],
        'required_options': [],
        'required_positionals': [],
        'positional_choices': {},
    }
    if not script_path:
        return empty
    try:
        source = Path(script_path).read_text(encoding='utf-8', errors='replace')
    except Exception:
        return empty
    if not str(source or '').strip():
        return empty
    try:
        tree = ast.parse(source, filename=script_path)
    except Exception:
        return empty

    alias_offsets = _discover_sys_argv_alias_offsets(tree)
    required_positions: Set[int] = set()
    position_names: Dict[int, str] = {}

    for node in ast.walk(tree):
        position = _extract_sys_argv_positional_index(node, alias_offsets)
        if position is not None:
            required_positions.add(int(position))

        target = None
        value = None
        if isinstance(node, ast.Assign) and len(list(node.targets or [])) == 1:
            target = node.targets[0]
            value = node.value
        elif isinstance(node, ast.AnnAssign):
            target = node.target
            value = node.value
        if target is not None and value is not None:
            position = _extract_sys_argv_positional_index(value, alias_offsets)
            if position is not None:
                name = _sanitize_positional_name(_extract_simple_name(target))
                if name:
                    position_names.setdefault(int(position), name)

        required_from_len = _extract_sys_argv_len_required_count(node, alias_offsets)
        if required_from_len is not None:
            for idx in range(1, int(required_from_len) + 1):
                required_positions.add(idx)

    if not required_positions:
        return empty

    usage_names = _extract_usage_placeholder_names(source)
    max_position = max(required_positions)
    required_positionals: List[str] = []
    for idx in range(1, int(max_position) + 1):
        if idx not in required_positions:
            continue
        name = position_names.get(idx) or (usage_names[idx - 1] if idx - 1 < len(usage_names) else '') or f'arg{idx}'
        required_positionals.append(_to_positional_required(name))

    return {
        'known_options': [],
        'required_options': [],
        'required_positionals': _merge_unique_args([], required_positionals),
        'positional_choices': {},
    }



def _extract_script_contract(script_path: str) -> Dict[str, object]:
    argparse_contract = _extract_argparse_contract(script_path)
    sys_argv_contract = _extract_sys_argv_contract(script_path)
    positional_choices: Dict[str, List[str]] = {}
    for source in (
        argparse_contract.get('positional_choices') or {},
        sys_argv_contract.get('positional_choices') or {},
    ):
        if not isinstance(source, dict):
            continue
        for key, value in source.items():
            name = str(key or '').strip()
            if not name:
                continue
            items = [str(item).strip() for item in (value or []) if str(item).strip()]
            if items:
                positional_choices[name] = _merge_unique_args(positional_choices.get(name, []), items)
    return {
        'known_options': _merge_unique_args([], list(argparse_contract.get('known_options') or []) + list(sys_argv_contract.get('known_options') or [])),
        'required_options': _merge_unique_args([], list(argparse_contract.get('required_options') or []) + list(sys_argv_contract.get('required_options') or [])),
        'required_positionals': _merge_unique_args([], list(argparse_contract.get('required_positionals') or []) + list(sys_argv_contract.get('required_positionals') or [])),
        'positional_choices': positional_choices,
    }


def _extract_argparse_contract(script_path: str) -> Dict[str, List[str]]:
    """
    解析脚本中的 argparse.add_argument 调用，提取：
    - known_options: 所有可选参数（含短参数和长参数）
    - required_options: 必填可选参数（优先长参数，不存在时使用短参数）
    - required_positionals: 必填位置参数（@pos:name）
    - positional_choices: 位置参数可选值（如 mode=run/self_test）
    """
    if not script_path:
        return {
            "known_options": [],
            "required_options": [],
            "required_positionals": [],
            "positional_choices": {},
        }
    try:
        with open(script_path, "r", encoding="utf-8", errors="replace") as handle:
            source = str(handle.read() or "")
    except Exception:
        return {
            "known_options": [],
            "required_options": [],
            "required_positionals": [],
            "positional_choices": {},
        }
    if not source.strip():
        return {
            "known_options": [],
            "required_options": [],
            "required_positionals": [],
            "positional_choices": {},
        }
    try:
        tree = ast.parse(source, filename=script_path)
    except Exception:
        return {
            "known_options": [],
            "required_options": [],
            "required_positionals": [],
            "positional_choices": {},
        }

    known_options: List[str] = []
    required_options: List[str] = []
    required_positionals: List[str] = []
    positional_choices: Dict[str, List[str]] = {}

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute):
            continue
        if str(func.attr or "").strip() != "add_argument":
            continue

        literal_args: List[str] = []
        for arg_node in list(node.args or []):
            text = str(_extract_python_constant_str(arg_node) or "").strip()
            if text:
                literal_args.append(text)
        if not literal_args:
            continue

        required_kw: Optional[bool] = None
        nargs_kw: str = ""
        choices_kw: List[str] = []
        for kw in list(node.keywords or []):
            if not isinstance(kw, ast.keyword):
                continue
            key = str(kw.arg or "").strip()
            if key == "required":
                required_kw = _extract_python_constant_bool(kw.value)
            elif key == "nargs":
                nargs_kw = str(_extract_python_constant_str(kw.value) or "").strip()
            elif key == "choices":
                choices_kw = _extract_python_constant_str_list(kw.value)

        option_flags = [item for item in literal_args if str(item or "").strip().startswith("-")]
        if option_flags:
            known_options = _merge_unique_args(known_options, option_flags)
            if required_kw is True:
                long_flags = [item for item in option_flags if str(item).startswith("--")]
                if long_flags:
                    required_options.append(str(long_flags[0]))
                else:
                    required_options.append(str(option_flags[0]))
            continue

        positional_name = str(literal_args[0] or "").strip()
        if not positional_name:
            continue
        if choices_kw:
            positional_choices[positional_name] = list(choices_kw)
        # positional 默认必填；nargs='?'/'*' 表示可选。
        if nargs_kw in {"?", "*"}:
            continue
        if required_kw is False:
            continue
        required_positionals.append(_to_positional_required(positional_name))

    return {
        "known_options": _merge_unique_args([], known_options),
        "required_options": _merge_unique_args([], required_options),
        "required_positionals": _merge_unique_args([], required_positionals),
        "positional_choices": {key: list(value) for key, value in positional_choices.items() if value},
    }


def _discover_required_script_optional_args(script_path: str) -> List[str]:
    """
    从脚本文本中提取 argparse.required=True 的可选参数（--flag）。
    """
    contract = _extract_script_contract(script_path)
    return _merge_unique_args([], contract.get("required_options") or [])


def _discover_script_optional_args(script_path: str) -> List[str]:
    """
    从脚本文本中提取 argparse 可选参数（--flag）。
    """
    contract = _extract_script_contract(script_path)
    return _merge_unique_args([], contract.get("known_options") or [])


def _to_positional_required(name: str) -> str:
    text = str(name or "").strip()
    if not text:
        return ""
    if text.startswith(_POSITIONAL_REQUIRED_PREFIX):
        return text
    return f"{_POSITIONAL_REQUIRED_PREFIX}{text}"


def _is_positional_required(required: str) -> bool:
    return str(required or "").strip().startswith(_POSITIONAL_REQUIRED_PREFIX)


def _display_required_arg(required: str) -> str:
    text = str(required or "").strip()
    if not text:
        return ""
    if _is_positional_required(text):
        name = text[len(_POSITIONAL_REQUIRED_PREFIX) :].strip() or "arg"
        return f"位置参数({name})"
    return text


def _discover_required_script_positional_args(script_path: str) -> List[str]:
    """
    从脚本文本提取 argparse 必填位置参数。
    """
    contract = _extract_script_contract(script_path)
    return _merge_unique_args([], contract.get("required_positionals") or [])


def _discover_script_positional_choices(script_path: str) -> Dict[str, List[str]]:
    contract = _extract_script_contract(script_path)
    raw = contract.get("positional_choices") or {}
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, List[str]] = {}
    for key, value in raw.items():
        name = str(key or "").strip()
        if not name:
            continue
        if isinstance(value, list):
            items = [str(item).strip() for item in value if str(item).strip()]
            if items:
                out[name] = items
    return out


def _select_preferred_positional_choice(choices: List[str]) -> str:
    values = [str(item).strip() for item in choices if str(item).strip()]
    if not values:
        return ""
    preferred_order = ("run", "execute", "main", "start", "fetch")
    lowered = {item.lower(): item for item in values}
    for key in preferred_order:
        if key in lowered:
            return lowered[key]
    for item in values:
        lowered_item = item.lower()
        if lowered_item not in {"self_test", "test", "dry_run", "dry-run", "check"}:
            return item
    return values[0]


def _extract_provided_positional_args(tokens: List[str]) -> List[str]:
    out: List[str] = []
    idx = 0
    while idx < len(tokens):
        token = str(tokens[idx] or "").strip()
        if not token:
            idx += 1
            continue
        if _is_cli_option_token(token):
            if "=" in token:
                idx += 1
                continue
            if idx + 1 < len(tokens):
                candidate = str(tokens[idx + 1] or "").strip()
                if candidate and not _is_cli_option_token(candidate):
                    idx += 2
                    continue
            idx += 1
            continue
        out.append(token)
        idx += 1
    return out


def _collect_missing_required_args(required_args: List[str], args_tokens: List[str]) -> List[str]:
    missing: List[str] = []
    positional_args = _extract_provided_positional_args(args_tokens)
    positional_required_seen = 0
    for item in required_args:
        required = str(item or "").strip()
        if not required:
            continue
        if _is_positional_required(required):
            positional_required_seen += 1
            if len(positional_args) < int(positional_required_seen):
                missing.append(required)
            continue
        if not _arg_is_provided(args_tokens, required):
            missing.append(required)
    return missing


def _extract_missing_required_columns(result: dict) -> List[str]:
    stderr = str((result or {}).get("stderr") or "")
    stdout = str((result or {}).get("stdout") or "")
    detail = f"{stderr}\n{stdout}"
    if not detail.strip():
        return []

    match = _CSV_REQUIRED_COLUMNS_RE.search(detail)
    if not match:
        return []
    body = str(match.group(1) or "").strip()
    if not body:
        return []
    items = [str(item or "").strip() for item in body.split(",")]
    out: List[str] = []
    for item in items:
        if not item:
            continue
        out.append(item)
    return _merge_unique_args([], out)


def _normalize_csv_header_key(value: object) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = text.replace("（", "(").replace("）", ")")
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fff_]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def _is_date_like_column_name(name: str) -> bool:
    raw = _normalize_csv_header_key(name)
    if not raw:
        return False
    return any(token in raw for token in _CSV_ALIAS_DATE_KEYS)


def _is_price_like_column_name(name: str) -> bool:
    raw = _normalize_csv_header_key(name)
    if not raw:
        return False
    return any(token in raw for token in _CSV_ALIAS_PRICE_KEYS)


def _candidate_alias_keys_for_required_column(required: str) -> List[str]:
    raw = _normalize_csv_header_key(required)
    if not raw:
        return []
    aliases: List[str] = [raw]
    if _is_date_like_column_name(raw):
        aliases.extend(_CSV_ALIAS_DATE_KEYS)
    if _is_price_like_column_name(raw):
        aliases.extend(_CSV_ALIAS_PRICE_KEYS)
    if raw == "price_cny_per_g":
        aliases.extend(
            [
                "price_cny_per_gram",
                "price",
                "close",
                "value",
                "amount",
                "cny_per_g",
                "价格",
                "金价",
            ]
        )
    if raw == "timestamp":
        aliases.extend(["date", "datetime", "time", "day", "日期", "时间"])
    return _merge_unique_args([], [_normalize_csv_header_key(item) for item in aliases])


def _resolve_csv_required_column_mapping(
    *,
    headers: List[str],
    missing_required_columns: List[str],
) -> Dict[str, str]:
    normalized_to_header: Dict[str, str] = {}
    for item in headers:
        normalized = _normalize_csv_header_key(item)
        if not normalized:
            continue
        if normalized not in normalized_to_header:
            normalized_to_header[normalized] = str(item)

    mapping: Dict[str, str] = {}
    used_sources: Set[str] = set()
    for required in missing_required_columns:
        required_name = str(required or "").strip()
        if not required_name:
            continue
        alias_keys = _candidate_alias_keys_for_required_column(required_name)
        selected = ""
        for key in alias_keys:
            candidate = str(normalized_to_header.get(key) or "").strip()
            if candidate and candidate not in used_sources:
                selected = candidate
                break
        if not selected and _is_date_like_column_name(required_name):
            for header in headers:
                candidate = str(header or "").strip()
                if not candidate or candidate in used_sources:
                    continue
                if _is_date_like_column_name(candidate):
                    selected = candidate
                    break
        if not selected and _is_price_like_column_name(required_name):
            for header in headers:
                candidate = str(header or "").strip()
                if not candidate or candidate in used_sources:
                    continue
                if _is_price_like_column_name(candidate):
                    selected = candidate
                    break
        if not selected:
            continue
        mapping[required_name] = selected
        used_sources.add(selected)
    return mapping


def _extract_csv_paths_from_command(payload: dict) -> List[str]:
    tokens = parse_command_tokens(payload.get("command"))
    if not tokens:
        return []

    out: List[str] = []
    idx = 0
    while idx < len(tokens):
        token = str(tokens[idx] or "").strip()
        if not token:
            idx += 1
            continue
        if token.startswith("--"):
            if "=" in token:
                _flag, value = token.split("=", 1)
                value_text = str(value or "").strip()
                if value_text.lower().endswith(".csv"):
                    out.append(value_text)
                idx += 1
                continue
            if idx + 1 < len(tokens):
                candidate = str(tokens[idx + 1] or "").strip()
                if candidate and not candidate.startswith("--") and candidate.lower().endswith(".csv"):
                    out.append(candidate)
                    idx += 2
                    continue
            idx += 1
            continue
        if token.lower().endswith(".csv"):
            out.append(token)
        idx += 1
    return _merge_unique_args([], out)


def _replace_command_csv_path(command: object, *, source_path: str, target_path: str) -> List[str]:
    source_norm = os.path.normcase(str(source_path or "").strip())
    tokens = parse_command_tokens(command)
    if not tokens:
        return []
    out: List[str] = []
    for token in tokens:
        current = str(token or "")
        if os.path.normcase(current.strip()) == source_norm:
            out.append(str(target_path))
        else:
            out.append(current)
    return out


def _build_retry_payload_with_csv_alias_mapping(
    *,
    payload: dict,
    result: dict,
    workdir: str,
) -> Tuple[Optional[dict], Dict[str, object]]:
    missing_columns = _extract_missing_required_columns(result)
    if not missing_columns:
        return None, {}

    csv_candidates = _extract_csv_paths_from_command(payload)
    if not csv_candidates:
        return None, {}

    selected_token = ""
    selected_abs = ""
    for candidate in csv_candidates:
        resolved = resolve_path_with_workdir(candidate, workdir)
        if not resolved or not os.path.exists(resolved):
            continue
        selected_token = candidate
        selected_abs = resolved
        break
    if not selected_abs:
        return None, {}

    try:
        with open(selected_abs, "r", encoding="utf-8", errors="replace", newline="") as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)
            headers = list(reader.fieldnames or [])
    except Exception:
        return None, {}

    if not headers:
        return None, {}

    mapping = _resolve_csv_required_column_mapping(
        headers=headers,
        missing_required_columns=missing_columns,
    )
    if not mapping:
        return None, {}

    unresolved = [item for item in missing_columns if str(item or "").strip() not in mapping]
    if unresolved:
        return None, {}

    base, ext = os.path.splitext(selected_abs)
    mapped_abs = f"{base}.alias_mapped{ext or '.csv'}"
    target_headers = list(headers)
    for required in missing_columns:
        required_name = str(required or "").strip()
        if required_name and required_name not in target_headers:
            target_headers.append(required_name)

    try:
        os.makedirs(os.path.dirname(mapped_abs), exist_ok=True)
        with open(mapped_abs, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=target_headers)
            writer.writeheader()
            for row in rows:
                out_row = dict(row or {})
                for required_name, source_name in mapping.items():
                    if str(required_name or "").strip() and str(required_name or "").strip() not in out_row:
                        out_row[required_name] = str(out_row.get(source_name) or "")
                    elif str(required_name or "").strip():
                        if not str(out_row.get(required_name) or "").strip():
                            out_row[required_name] = str(out_row.get(source_name) or "")
                writer.writerow(out_row)
    except Exception:
        return None, {}

    mapped_token = mapped_abs
    try:
        mapped_token = os.path.relpath(mapped_abs, os.path.abspath(workdir or os.getcwd())).replace("\\", "/")
    except Exception:
        mapped_token = mapped_abs

    patched = dict(payload or {})
    patched["command"] = _replace_command_csv_path(
        payload.get("command"),
        source_path=selected_token,
        target_path=mapped_token,
    )
    meta = {
        "trigger": "missing_required_columns",
        "csv_source": selected_token,
        "csv_mapped": mapped_token,
        "required_columns": list(missing_columns),
        "column_mapping": dict(mapping),
    }
    return patched, meta


def _extract_script_and_args_from_payload(payload: dict) -> Tuple[str, List[str]]:
    script = str(payload.get("script") or "").strip()
    command = payload.get("command")
    args = parse_command_tokens(command)
    if not script and len(args) >= 2 and _is_python_executable(str(args[0] or "").strip()):
        candidate = str(args[1] or "").strip()
        if candidate.lower().endswith(".py"):
            script = candidate
            return script, [str(item) for item in args[2:]]
    if script and len(args) >= 2 and _is_python_executable(str(args[0] or "").strip()):
        return script, [str(item) for item in args[2:]]
    return script, []


def _arg_is_provided(tokens: List[str], expected: str) -> bool:
    normalized_expected = str(expected or "").strip()
    if not normalized_expected:
        return True
    for token in tokens:
        current = str(token or "").strip()
        if current == normalized_expected:
            return True
        if normalized_expected.startswith("-") and current.startswith(normalized_expected + "="):
            return True
    return False


def _is_output_like_flag(flag: str) -> bool:
    raw = str(flag or "").strip().lower()
    if not raw:
        return False
    if raw in {"-o"}:
        return True
    return any(token in raw for token in ("--out", "--output", "result", "target"))


def _is_input_like_flag(flag: str) -> bool:
    raw = str(flag or "").strip().lower()
    if not raw:
        return False
    if raw in {"-i"}:
        return True
    return any(token in raw for token in ("--in", "--input", "source", "src", "from", "raw"))


def _is_name_like_flag(flag: str) -> bool:
    raw = str(flag or "").strip().lower()
    if not raw:
        return False
    if raw in {"-n"}:
        return True
    return any(token in raw for token in ("--name", "--filename", "--file-name", "--file_name"))


def _load_script_source_text(script_path: str) -> str:
    if not script_path:
        return ""
    try:
        with open(script_path, "r", encoding="utf-8", errors="replace") as handle:
            return str(handle.read() or "")
    except Exception:
        return ""


def _infer_script_output_extension(script_path: str) -> str:
    source = _load_script_source_text(script_path).lower()
    if not source:
        return ".json"
    if "dictwriter" in source or "csv." in source or ".writerow(" in source:
        return ".csv"
    if "json.dump" in source or "json.dumps" in source:
        return ".json"
    if "yaml.safe_dump" in source or ".yaml" in source or ".yml" in source:
        return ".yaml"
    return ".json"


def _infer_script_input_extensions(script_path: str) -> List[str]:
    source = _load_script_source_text(script_path).lower()
    exts: List[str] = []
    if "json.load" in source or ".json" in source:
        exts.append(".json")
    if "csv." in source or ".csv" in source:
        exts.append(".csv")
    if "yaml.safe_load" in source or ".yaml" in source or ".yml" in source:
        exts.append(".yaml")
        exts.append(".yml")
    if not exts:
        exts.extend([".json", ".csv", ".txt"])
    # 去重并保持顺序
    seen: Set[str] = set()
    out: List[str] = []
    for item in exts:
        current = str(item or "").strip().lower()
        if not current or current in seen:
            continue
        seen.add(current)
        out.append(current)
    return out


def _build_default_script_output_path(script: str, workdir: str) -> str:
    script_abs = resolve_path_with_workdir(script, workdir)
    base_name = os.path.splitext(os.path.basename(str(script_abs or script).strip()))[0] or "script"
    ext = _infer_script_output_extension(str(script_abs or script))

    workspace_dir = resolve_path_with_workdir("backend/.agent/workspace", workdir)
    if not workspace_dir:
        workspace_dir = os.path.dirname(str(script_abs or ""))
    if not workspace_dir:
        workspace_dir = os.path.abspath(workdir or os.getcwd())

    out_abs = os.path.abspath(os.path.join(workspace_dir, f"{base_name}_output{ext}"))
    workdir_abs = os.path.abspath(workdir or os.getcwd())
    try:
        rel = os.path.relpath(out_abs, workdir_abs).replace("\\", "/")
        return rel
    except Exception:
        return out_abs


def _build_default_name_arg_value(script: str) -> str:
    script_name = os.path.splitext(os.path.basename(str(script or "").strip()))[0]
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "_", script_name).strip("_")
    if not cleaned:
        cleaned = "artifact"
    return cleaned[:48]


def _pick_recent_artifact_paths(
    *,
    workdir: str,
    preferred_exts: Optional[Set[str]] = None,
    max_items: int = 3,
) -> List[str]:
    preferred = {str(item or "").strip().lower() for item in (preferred_exts or set()) if str(item or "").strip()}
    artifacts = _collect_arg_binding_artifacts(workdir, max_items=120, max_depth=4)
    if not artifacts:
        return []

    sortable: List[Tuple[int, str, str]] = []
    for item in artifacts:
        if not isinstance(item, dict):
            continue
        path = str(item.get("path") or "").strip()
        ext = str(item.get("ext") or "").strip().lower()
        if not path:
            continue
        if preferred and ext and ext not in preferred:
            continue
        try:
            mtime = int(item.get("mtime") or 0)
        except Exception:
            mtime = 0
        sortable.append((mtime, path, ext))

    sortable.sort(key=lambda row: int(row[0]), reverse=True)
    out: List[str] = []
    seen: Set[str] = set()
    for _mtime, path, _ext in sortable:
        if path in seen:
            continue
        seen.add(path)
        out.append(path)
        if len(out) >= int(max_items):
            break
    return out


def _extract_option_value_pairs(tokens: List[str]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    idx = 0
    while idx < len(tokens):
        token = str(tokens[idx] or "").strip()
        if not _is_cli_option_token(token):
            idx += 1
            continue
        if "=" in token:
            key, value = token.split("=", 1)
            key_text = str(key or "").strip()
            value_text = str(value or "").strip()
            if key_text and value_text:
                out.append((key_text, value_text))
            idx += 1
            continue
        next_value = ""
        if idx + 1 < len(tokens):
            candidate = str(tokens[idx + 1] or "").strip()
            if candidate and not _is_cli_option_token(candidate):
                next_value = candidate
                idx += 2
            else:
                idx += 1
        else:
            idx += 1
        if next_value:
            out.append((token, next_value))
    return out


def _filter_script_args_by_known_options(
    tokens: List[str],
    known_options: Set[str],
    *,
    strip_unknown_when_empty: bool = False,
) -> List[str]:
    if not known_options and not strip_unknown_when_empty:
        return list(tokens)
    out: List[str] = []
    idx = 0
    while idx < len(tokens):
        token = str(tokens[idx] or "").strip()
        if not _is_cli_option_token(token):
            out.append(token)
            idx += 1
            continue
        if "=" in token:
            key, _value = token.split("=", 1)
            if known_options and str(key or "").strip() in known_options:
                out.append(token)
            idx += 1
            continue
        if known_options and token in known_options:
            out.append(token)
            if idx + 1 < len(tokens):
                candidate = str(tokens[idx + 1] or "").strip()
                if candidate and not _is_cli_option_token(candidate):
                    out.append(candidate)
                    idx += 2
                    continue
            idx += 1
            continue
        # unknown option: 丢弃该 flag 及其 value（如存在）
        if idx + 1 < len(tokens):
            candidate = str(tokens[idx + 1] or "").strip()
            if candidate and not _is_cli_option_token(candidate):
                idx += 2
                continue
        idx += 1
    return out


def _semantic_tokens(text: str) -> Set[str]:
    raw = str(text or "").strip().lower()
    if not raw:
        return set()
    tokens = [item for item in re.split(r"[^a-z0-9]+", raw) if item]
    stopwords = {
        "arg",
        "args",
        "input",
        "output",
        "file",
        "path",
        "value",
        "data",
        "raw",
        "tmp",
    }
    out: Set[str] = set()
    for token in tokens:
        if len(token) <= 1:
            continue
        if token in stopwords:
            continue
        out.add(token)
    return out


def _score_arg_binding_candidate(*, required: str, source_option: str, value: str) -> int:
    required_tokens = _semantic_tokens(required)
    source_tokens = _semantic_tokens(source_option)
    value_tokens = _semantic_tokens(os.path.basename(str(value or "")))
    if not required_tokens:
        return 0

    score = 0
    overlap_source = required_tokens.intersection(source_tokens)
    overlap_value = required_tokens.intersection(value_tokens)
    score += int(len(overlap_source) * 4)
    score += int(len(overlap_value) * 3)

    required_text = str(required or "").strip().lower()
    source_text = str(source_option or "").strip().lower()
    value_text = str(value or "").strip().lower()
    if required_text and required_text in source_text:
        score += 4
    if required_text and required_text in value_text:
        score += 3
    return int(score)


def _pick_best_binding_candidate(
    *,
    required: str,
    candidates: List[Tuple[str, str]],
    used_indexes: Set[int],
    allow_zero_score: bool,
) -> Optional[Tuple[int, str, str, int]]:
    best: Optional[Tuple[int, str, str, int]] = None
    for idx, (source_option, value) in enumerate(candidates):
        if idx in used_indexes:
            continue
        score = _score_arg_binding_candidate(
            required=required,
            source_option=source_option,
            value=value,
        )
        if score <= 0 and not allow_zero_score:
            continue
        if best is None:
            best = (idx, source_option, value, score)
            continue
        if score > int(best[3]):
            best = (idx, source_option, value, score)
            continue
    return best


def _is_truthy(value: object, *, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _llm_script_arg_binding_enabled(context: Optional[dict]) -> bool:
    if isinstance(context, dict) and "enable_llm_script_arg_binding" in context:
        return _is_truthy(context.get("enable_llm_script_arg_binding"), default=False)

    env_value = os.getenv("ENABLE_LLM_SCRIPT_ARG_BINDING")
    if str(env_value or "").strip():
        return _is_truthy(env_value, default=False)

    # 测试环境默认关闭，避免单测依赖外部模型。
    if is_test_env():
        return False
    return True


def _collect_arg_binding_artifacts(workdir: str, *, max_items: int = 40, max_depth: int = 3) -> List[dict]:
    roots: List[str] = []
    for candidate in (
        "backend/.agent/workspace",
        ".agent/workspace",
        "data",
        "backend/data",
    ):
        resolved = resolve_path_with_workdir(candidate, workdir)
        if not resolved:
            continue
        absolute = os.path.abspath(resolved)
        if os.path.isdir(absolute):
            roots.append(absolute)

    dedup_roots: List[str] = []
    seen_root: Set[str] = set()
    for root in roots:
        key = os.path.normcase(root)
        if key in seen_root:
            continue
        seen_root.add(key)
        dedup_roots.append(root)

    artifacts: List[dict] = []
    seen_file: Set[str] = set()
    workdir_abs = os.path.abspath(workdir or os.getcwd())

    for root in dedup_roots:
        base_depth = root.rstrip(os.sep).count(os.sep)
        for current_dir, dirs, files in os.walk(root):
            depth = current_dir.rstrip(os.sep).count(os.sep) - base_depth
            if depth >= int(max_depth):
                dirs[:] = []

            for filename in sorted(files):
                absolute = os.path.abspath(os.path.join(current_dir, filename))
                key = os.path.normcase(absolute)
                if key in seen_file:
                    continue
                seen_file.add(key)
                try:
                    stat = os.stat(absolute)
                    rel = os.path.relpath(absolute, workdir_abs)
                    rel = rel.replace("\\", "/")
                    artifacts.append(
                        {
                            "path": rel,
                            "ext": str(os.path.splitext(filename)[1] or "").lower(),
                            "size": int(stat.st_size),
                            "mtime": int(stat.st_mtime),
                        }
                    )
                except Exception:
                    continue
                if len(artifacts) >= int(max_items):
                    return artifacts
    return artifacts


def _extract_first_json_object(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""

    start = raw.find("{")
    if start < 0:
        return ""

    depth = 0
    in_string = False
    escaped = False
    for idx in range(start, len(raw)):
        ch = raw[idx]
        if in_string:
            if escaped:
                escaped = False
                continue
            if ch == "\\":
                escaped = True
                continue
            if ch == "\"":
                in_string = False
            continue
        if ch == "\"":
            in_string = True
            continue
        if ch == "{":
            depth += 1
            continue
        if ch == "}":
            depth -= 1
            if depth == 0:
                return raw[start : idx + 1]
    return ""


def _build_llm_script_arg_prompt(
    *,
    script: str,
    required_args: List[str],
    known_options: List[str],
    provided_args: List[str],
    missing_args: List[str],
    expected_outputs: List[str],
    artifacts: List[dict],
) -> str:
    payload = {
        "script": script,
        "required_args": list(required_args),
        "known_options": list(known_options),
        "provided_args": list(provided_args),
        "missing_args": list(missing_args),
        "expected_outputs": list(expected_outputs),
        "artifacts": list(artifacts),
    }
    payload_json = json.dumps(payload, ensure_ascii=False, indent=2)
    return (
        "你是脚本参数绑定器。请把已有信息映射成可执行的脚本参数 args。\n"
        "输出要求：仅输出一个 JSON 对象，不要输出任何额外文本。\n"
        '固定格式：{"args":["--k","v"],"reason":"...","confidence":0.0}\n'
        "约束：\n"
        "1) args 只包含脚本参数，不要包含 python 可执行程序和脚本路径。\n"
        "2) 必须覆盖 missing_args；优先复用 provided_args 里的值。\n"
        "3) 仅使用 known_options 或 required_args 中存在的选项名，禁止杜撰新选项。\n"
        "4) 若 required_args 里出现 `@pos:<name>`，表示这是必填位置参数，args 中应直接放值，不要加 --flag。\n"
        "5) 若无法确定，请返回 args=[] 且给出 reason，confidence 设为 0。\n\n"
        "输入：\n"
        f"{payload_json}"
    )


def _try_llm_bind_script_missing_args(
    *,
    payload: dict,
    workdir: str,
    script: str,
    required_args: List[str],
    known_options: List[str],
    missing_args: List[str],
    provided_args: List[str],
    task_id: Optional[int],
    run_id: Optional[int],
    context: Optional[dict],
) -> Tuple[Optional[dict], Optional[dict]]:
    if not missing_args:
        return None, None
    if not _llm_script_arg_binding_enabled(context):
        return None, {"status": "disabled", "reason": "llm_script_arg_binding_disabled"}
    if task_id is None or run_id is None:
        return None, {"status": "skipped", "reason": "missing_task_or_run_id"}

    expected_outputs: List[str] = []
    raw_outputs = payload.get("expected_outputs")
    if isinstance(raw_outputs, list):
        for item in raw_outputs:
            text = str(item or "").strip()
            if text:
                expected_outputs.append(text)

    artifacts = _collect_arg_binding_artifacts(workdir)
    prompt = _build_llm_script_arg_prompt(
        script=script,
        required_args=required_args,
        known_options=known_options,
        provided_args=provided_args,
        missing_args=missing_args,
        expected_outputs=expected_outputs,
        artifacts=artifacts,
    )

    try:
        from backend.src.services.llm.llm_calls import create_llm_call

        llm_result = create_llm_call(
            {
                "task_id": int(task_id),
                "run_id": int(run_id),
                "prompt": prompt,
                "parameters": {
                    "temperature": 0,
                    "timeout_seconds": 20,
                },
            }
        )
    except Exception as exc:
        return None, {"status": "error", "reason": f"llm_call_exception:{exc}"}

    record = llm_result.get("record") if isinstance(llm_result, dict) else None
    record_id = None
    status = ""
    response_text = ""
    if isinstance(record, dict):
        try:
            if record.get("id") is not None:
                record_id = int(record.get("id"))
        except Exception:
            record_id = None
        status = str(record.get("status") or "").strip().lower()
        response_text = str(record.get("response") or "")

    if status == "error":
        return None, {
            "status": "error",
            "reason": str((record or {}).get("error") or "llm_record_error"),
            "record_id": record_id,
        }

    parsed = parse_json_value(response_text)
    if not isinstance(parsed, dict):
        embedded = _extract_first_json_object(response_text)
        parsed = parse_json_value(embedded) if embedded else None
    if not isinstance(parsed, dict):
        return None, {
            "status": "invalid_response",
            "reason": "llm_response_not_json_object",
            "record_id": record_id,
        }

    candidate_args = parsed.get("args")
    if isinstance(candidate_args, str):
        candidate_tokens = parse_command_tokens(candidate_args)
    elif isinstance(candidate_args, list):
        candidate_tokens = [str(item).strip() for item in candidate_args if str(item).strip()]
    else:
        candidate_tokens = []

    if not candidate_tokens:
        return None, {
            "status": "empty_args",
            "reason": str(parsed.get("reason") or "llm_returned_empty_args"),
            "record_id": record_id,
        }

    allowed_options = set(
        [item for item in required_args if str(item).strip().startswith("-")]
        + [item for item in known_options if str(item).strip().startswith("-")]
    )
    normalized_tokens = _filter_script_args_by_known_options(
        candidate_tokens,
        allowed_options,
        strip_unknown_when_empty=(not allowed_options and any(_is_positional_required(item) for item in required_args)),
    )

    missing_after = _collect_missing_required_args(required_args=required_args, args_tokens=normalized_tokens)
    if missing_after:
        return None, {
            "status": "contract_not_satisfied",
            "reason": f"missing_after_llm_bind:{','.join(missing_after)}",
            "record_id": record_id,
        }

    patched = dict(payload or {})
    patched["args"] = list(normalized_tokens)
    python_exec = str(sys.executable or "").strip() or "python"
    command_tokens = parse_command_tokens(payload.get("command"))
    if command_tokens and _is_python_executable(str(command_tokens[0] or "").strip()):
        python_exec = str(command_tokens[0] or "").strip() or python_exec
    patched["command"] = [python_exec, script] + list(normalized_tokens)

    confidence = 0.0
    try:
        confidence = float(parsed.get("confidence"))
    except Exception:
        confidence = 0.0
    if confidence < 0:
        confidence = 0.0
    if confidence > 1:
        confidence = 1.0

    return (
        {
            "patched_payload": patched,
            "args": list(normalized_tokens),
            "record_id": record_id,
            "reason": str(parsed.get("reason") or "").strip(),
            "confidence": confidence,
            "status": "applied",
        },
        None,
    )


def _try_autofill_script_missing_args(
    *,
    payload: dict,
    workdir: str,
    script: str,
    required_args: List[str],
    missing_args: List[str],
    provided_args: List[str],
    known_options: Optional[Set[str]] = None,
    context: Optional[dict] = None,
) -> Optional[dict]:
    if not missing_args:
        return None

    option_pairs = _extract_option_value_pairs(provided_args)

    options = set(known_options or [])
    script_abs = resolve_path_with_workdir(script, workdir)
    positional_choices = _discover_script_positional_choices(script_abs)
    if not options:
        options = set(_discover_script_optional_args(script_abs))

    input_candidates: List[Tuple[str, str]] = []
    output_candidates: List[Tuple[str, str]] = []
    for key, value in option_pairs:
        if _is_output_like_flag(key):
            output_candidates.append((key, value))
        else:
            input_candidates.append((key, value))

    expected_outputs = payload.get("expected_outputs")
    if isinstance(expected_outputs, list):
        for item in expected_outputs:
            path = str(item or "").strip()
            if path:
                output_candidates.append(("expected_outputs", path))

    missing_output_flags = [item for item in missing_args if (not _is_positional_required(item)) and _is_output_like_flag(item)]
    missing_input_flags = [
        item
        for item in missing_args
        if _is_positional_required(item) or _is_input_like_flag(item) or not _is_output_like_flag(item)
    ]

    if missing_output_flags and not output_candidates:
        default_output = _build_default_script_output_path(script, workdir)
        if default_output:
            output_candidates.append(("auto_default_output", default_output))

    if missing_input_flags and not input_candidates:
        preferred_input_exts = set(_infer_script_input_extensions(str(script_abs or script)))
        context_artifacts: List[str] = []
        default_url = str((context or {}).get("shell_default_url") or "").strip()
        if not default_url:
            default_url = str((context or {}).get("latest_external_url") or "").strip()
        if default_url:
            input_candidates.append(("latest_external_url", default_url))
        latest_script_artifacts = (context or {}).get("latest_script_artifacts")
        if isinstance(latest_script_artifacts, list):
            for item in latest_script_artifacts:
                if not isinstance(item, dict):
                    continue
                if item.get("exists") is False:
                    continue
                path = str(item.get("path") or "").strip()
                if not path:
                    continue
                resolved = resolve_path_with_workdir(path, workdir)
                if not resolved or not os.path.exists(resolved):
                    continue
                ext = str(os.path.splitext(path)[1] or "").lower()
                if preferred_input_exts and ext and ext not in preferred_input_exts:
                    continue
                context_artifacts.append(path)
        latest_write_path = str((context or {}).get("latest_file_write_path") or "").strip()
        if latest_write_path:
            resolved = resolve_path_with_workdir(latest_write_path, workdir)
            if resolved and os.path.exists(resolved):
                ext = str(os.path.splitext(latest_write_path)[1] or "").lower()
                if (not preferred_input_exts) or (not ext) or (ext in preferred_input_exts):
                    context_artifacts.append(latest_write_path)
        for path in _merge_unique_args([], context_artifacts)[:3]:
            input_candidates.append(("artifact_context", path))

    if missing_input_flags and not input_candidates:
        for path in _pick_recent_artifact_paths(
            workdir=workdir,
            preferred_exts=preferred_input_exts,
            max_items=3,
        ):
            input_candidates.append(("artifact_recent", path))

    if not input_candidates and not output_candidates:
        return None

    # 先按脚本真实契约裁剪未知参数，避免 argparse 因旧 flag 报 unrecognized。
    normalized_args = _filter_script_args_by_known_options(
        provided_args,
        options,
        strip_unknown_when_empty=(not options and any(_is_positional_required(item) for item in required_args)),
    )

    used_input_indexes: Set[int] = set()
    used_output_indexes: Set[int] = set()
    applied: List[dict] = []

    for required in required_args:
        if not required:
            continue
        if _is_positional_required(required):
            required_positional_total = sum(1 for item in required_args if _is_positional_required(item))
            provided_positionals = _extract_provided_positional_args(normalized_args)
            required_positional_so_far = 0
            for item in required_args:
                if _is_positional_required(str(item or "").strip()):
                    required_positional_so_far += 1
                if item == required:
                    break
            if required_positional_so_far <= len(provided_positionals):
                continue
            positional_name = str(required[len(_POSITIONAL_REQUIRED_PREFIX):] or "").strip()
            choice_value = _select_preferred_positional_choice(positional_choices.get(positional_name) or [])
            if choice_value:
                normalized_args.append(choice_value)
                applied.append(
                    {
                        "required": required,
                        "source_option": "argparse_choices",
                        "value": choice_value,
                        "score": 10,
                    }
                )
                continue
            selected = _pick_best_binding_candidate(
                required=required,
                candidates=input_candidates,
                used_indexes=used_input_indexes,
                allow_zero_score=(required_positional_total == 1),
            )
            if selected is None and input_candidates:
                selected = _pick_best_binding_candidate(
                    required=required,
                    candidates=input_candidates,
                    used_indexes=used_input_indexes,
                    allow_zero_score=True,
                )
            if selected is None:
                continue
            selected_idx, selected_key, selected_value, selected_score = selected
            used_input_indexes.add(int(selected_idx))
            if not selected_value:
                continue
            normalized_args.append(selected_value)
            applied.append(
                {
                    "required": required,
                    "source_option": selected_key,
                    "value": selected_value,
                    "score": int(selected_score),
                }
            )
            continue
        if _arg_is_provided(normalized_args, required):
            continue
        selected_key = ""
        selected_value = ""
        selected_score = 0
        if _is_output_like_flag(required):
            selected = _pick_best_binding_candidate(
                required=required,
                candidates=output_candidates,
                used_indexes=used_output_indexes,
                allow_zero_score=True,
            )
            if selected is not None:
                selected_idx, selected_key, selected_value, selected_score = selected
                used_output_indexes.add(int(selected_idx))
        else:
            selected = _pick_best_binding_candidate(
                required=required,
                candidates=input_candidates,
                used_indexes=used_input_indexes,
                allow_zero_score=(len(missing_input_flags) == 1),
            )
            if (
                selected is None
                and len(input_candidates) == len(missing_input_flags)
                and len(input_candidates) > 0
            ):
                # 当输入候选与缺失参数数量完全一致时，允许按顺序兜底一次，
                # 避免“低语义可辨识但一一对应明确”的场景被卡死。
                selected = _pick_best_binding_candidate(
                    required=required,
                    candidates=input_candidates,
                    used_indexes=used_input_indexes,
                    allow_zero_score=True,
                )
            if selected is not None:
                selected_idx, selected_key, selected_value, selected_score = selected
                used_input_indexes.add(int(selected_idx))
            elif _is_name_like_flag(required):
                selected_key = "auto_default_name"
                selected_value = _build_default_name_arg_value(script)
                selected_score = 1
        if not selected_value:
            continue
        normalized_args.extend([required, selected_value])
        applied.append(
            {
                "required": required,
                "source_option": selected_key,
                "value": selected_value,
                "score": int(selected_score),
            }
        )

    if not applied:
        return None

    patched = dict(payload or {})
    patched["args"] = list(normalized_args)
    python_exec = str(sys.executable or "").strip() or "python"
    command_tokens = parse_command_tokens(payload.get("command"))
    if command_tokens and _is_python_executable(str(command_tokens[0] or "").strip()):
        python_exec = str(command_tokens[0] or "").strip() or python_exec
    patched["command"] = [python_exec, script] + list(normalized_args)

    return {
        "patched_payload": patched,
        "applied": applied,
        "known_options": sorted(options),
        "original_args": list(provided_args),
        "normalized_args": list(normalized_args),
    }


def _preflight_script_arg_contract(
    payload: dict,
    workdir: str,
    *,
    task_id: Optional[int] = None,
    run_id: Optional[int] = None,
    context: Optional[dict] = None,
) -> Tuple[dict, Optional[str]]:
    script, script_args = _extract_script_and_args_from_payload(payload)
    if not script:
        return {}, None
    original_script_args = list(script_args)

    required_from_payload = []
    raw_required = payload.get("required_args")
    if _looks_like_nonempty_string_list(raw_required):
        required_from_payload = []
        for item in raw_required:
            text = str(item).strip()
            if not text:
                continue
            if text.startswith("-"):
                required_from_payload.append(text)
            else:
                required_from_payload.append(_to_positional_required(text))

    discover_required = payload.get("discover_required_args")
    if discover_required is None:
        discover_required = True
    required_auto: List[str] = []
    required_positional_auto: List[str] = []
    script_abs = resolve_path_with_workdir(script, workdir)
    if bool(discover_required):
        required_auto = _discover_required_script_optional_args(script_abs)
        required_positional_auto = _discover_required_script_positional_args(script_abs)
    known_options = _discover_script_optional_args(script_abs)

    # 无论是否存在缺失参数，都先用脚本真实可识别选项裁剪未知 flag，
    # 避免运行阶段直接触发 argparse "unrecognized arguments"。
    if script_args and (known_options or required_positional_auto):
        normalized_initial_args = _filter_script_args_by_known_options(
            script_args,
            set(known_options),
            strip_unknown_when_empty=(not known_options and bool(required_positional_auto)),
        )
        if normalized_initial_args != script_args:
            patched_payload = dict(payload or {})
            patched_payload["args"] = list(normalized_initial_args)
            python_exec = str(sys.executable or "").strip() or "python"
            command_tokens = parse_command_tokens(payload.get("command"))
            if command_tokens and _is_python_executable(str(command_tokens[0] or "").strip()):
                python_exec = str(command_tokens[0] or "").strip() or python_exec
            patched_payload["command"] = [python_exec, script] + list(normalized_initial_args)
            payload.clear()
            payload.update(patched_payload)
            _script, script_args = _extract_script_and_args_from_payload(payload)
            if _script != script:
                script_args = list(normalized_initial_args)

    discovered_required = _merge_unique_args(required_auto, required_positional_auto)
    # 以脚本真实契约为准：只在无法发现脚本契约时回退 payload.required_args。
    if bool(discover_required) and discovered_required:
        required = list(discovered_required)
    else:
        required = _merge_unique_args(required_from_payload, discovered_required)
    if not required:
        return {
            "mode": "script_run",
            "script": script,
            "required_args": [],
            "provided_args": list(script_args),
            "missing_args": [],
        }, None

    def _collect_missing(args_tokens: List[str]) -> List[str]:
        return _collect_missing_required_args(required_args=required, args_tokens=args_tokens)

    missing = _collect_missing(script_args)
    autofill_meta = None
    llm_bind_meta = None
    llm_bind_failure = None
    if missing:
        autofill_meta = _try_autofill_script_missing_args(
            payload=payload,
            workdir=workdir,
            script=script,
            required_args=required,
            missing_args=missing,
            provided_args=original_script_args,
            known_options=set(known_options),
            context=context,
        )
        if isinstance(autofill_meta, dict):
            patched_payload = autofill_meta.get("patched_payload")
            if isinstance(patched_payload, dict):
                payload.clear()
                payload.update(patched_payload)
                _script, script_args = _extract_script_and_args_from_payload(payload)
                # 仅在脚本路径一致时使用更新后的 args 重新校验，避免意外漂移。
                if _script == script:
                    missing = _collect_missing(script_args)
        if missing:
            llm_bind_meta, llm_bind_failure = _try_llm_bind_script_missing_args(
                payload=payload,
                workdir=workdir,
                script=script,
                required_args=required,
                known_options=list(known_options),
                missing_args=missing,
                provided_args=original_script_args,
                task_id=task_id,
                run_id=run_id,
                context=context,
            )
            if isinstance(llm_bind_meta, dict):
                patched_payload = llm_bind_meta.get("patched_payload")
                if isinstance(patched_payload, dict):
                    payload.clear()
                    payload.update(patched_payload)
                    _script, script_args = _extract_script_and_args_from_payload(payload)
                    if _script == script:
                        missing = _collect_missing(script_args)

    contract = {
        "mode": "script_run",
        "script": script,
        "required_args": required,
        "known_options": list(known_options),
        "provided_args": list(script_args),
        "missing_args": missing,
    }
    if isinstance(autofill_meta, dict):
        contract["autofill"] = {
            "applied": list(autofill_meta.get("applied") or []),
            "known_options": list(autofill_meta.get("known_options") or []),
        }
    if isinstance(llm_bind_meta, dict):
        contract["llm_autofill"] = {
            "status": "applied",
            "record_id": llm_bind_meta.get("record_id"),
            "args": list(llm_bind_meta.get("args") or []),
            "reason": str(llm_bind_meta.get("reason") or ""),
            "confidence": llm_bind_meta.get("confidence"),
        }
    elif isinstance(llm_bind_failure, dict):
        contract["llm_autofill"] = {
            "status": str(llm_bind_failure.get("status") or "failed"),
            "record_id": llm_bind_failure.get("record_id"),
            "reason": str(llm_bind_failure.get("reason") or ""),
        }
    if missing:
        autofill_tail = ""
        if isinstance(autofill_meta, dict):
            applied = autofill_meta.get("applied") or []
            if isinstance(applied, list) and applied:
                autofill_tail = f"；已尝试自动补齐: {applied}"
        llm_tail = ""
        if isinstance(llm_bind_failure, dict):
            reason = str(llm_bind_failure.get("reason") or "").strip()
            if reason:
                llm_tail = f"；LLM补齐未生效: {reason}"
        return contract, format_task_error(
            code="script_args_missing",
            message=(
                f"{ERROR_MESSAGE_COMMAND_FAILED}: 脚本参数缺失（{', '.join(_display_required_arg(item) for item in missing)}）；"
                f"脚本={script}{autofill_tail}{llm_tail}"
            ),
        )
    return contract, None


def _maybe_apply_stdin_from_context(payload: dict, context: Optional[dict]) -> Tuple[dict, int]:
    patched = dict(payload or {})
    if not bool(patched.get("stdin_from_context")):
        return patched, 0

    current_stdin = patched.get("stdin")
    if isinstance(current_stdin, str) and current_stdin.strip():
        return patched, 0

    parse_text = str((context or {}).get("latest_parse_input_text") or "").strip()
    if not parse_text:
        return patched, 0

    patched["stdin"] = parse_text
    return patched, len(parse_text)


def _looks_like_declared_output_path(value: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    lowered = text.lower()
    if lowered in {"-", "stdout", "stderr", "/dev/stdout", "/dev/stderr"}:
        return False
    if text.startswith("http://") or text.startswith("https://"):
        return False
    if any(sep in text for sep in ("/", "\\")):
        return True
    if os.path.splitext(text)[1]:
        return True
    if text.startswith("."):
        return True
    return False


def _collect_declared_output_paths(payload: dict) -> List[str]:
    out: List[str] = []
    raw_outputs = payload.get("expected_outputs")
    if isinstance(raw_outputs, list):
        for item in raw_outputs:
            text = str(item or "").strip()
            if text:
                out.append(text)

    script, script_args = _extract_script_and_args_from_payload(payload)
    if script_args:
        candidate_tokens = list(script_args)
    else:
        raw_args = payload.get("args")
        if isinstance(raw_args, list):
            candidate_tokens = [str(item).strip() for item in raw_args if str(item).strip()]
        elif isinstance(raw_args, str) and raw_args.strip():
            candidate_tokens = parse_command_tokens(raw_args)
        else:
            candidate_tokens = []
        if not candidate_tokens and not script:
            candidate_tokens = parse_command_tokens(payload.get("command"))

    for key, value in _extract_option_value_pairs(candidate_tokens):
        if not _is_output_like_flag(key):
            continue
        if not _looks_like_declared_output_path(value):
            continue
        out.append(str(value).strip())

    return _merge_unique_args([], out)


def _collect_declared_output_artifacts(
    payload: dict,
    workdir: str,
    *,
    declared_paths: Optional[List[str]] = None,
) -> Tuple[List[dict], List[str]]:
    artifacts: List[dict] = []
    missing: List[str] = []
    output_paths = list(declared_paths) if isinstance(declared_paths, list) else _collect_declared_output_paths(payload)
    for rel_path in output_paths:
        absolute_path = resolve_path_with_workdir(rel_path, workdir)
        exists = bool(absolute_path) and os.path.exists(absolute_path)
        item = {
            "path": rel_path,
            "absolute_path": absolute_path,
            "exists": exists,
            "size": None,
        }
        if exists:
            try:
                item["size"] = int(os.path.getsize(absolute_path))
            except Exception:
                item["size"] = None
        else:
            missing.append(rel_path)
        artifacts.append(item)
    return artifacts, missing


def _collect_csv_artifact_quality_failures(
    artifacts: List[dict],
    *,
    context: Optional[dict] = None,
) -> List[str]:
    strict = (context or {}).get("enforce_csv_artifact_quality")
    if strict is None:
        strict = True
    if not bool(strict):
        return []

    failures: List[str] = []
    for item in artifacts:
        if not isinstance(item, dict):
            continue
        rel_path = str(item.get("path") or "").strip()
        abs_path = str(item.get("absolute_path") or "").strip()
        if not rel_path or not abs_path:
            continue
        if not abs_path.lower().endswith('.csv'):
            continue
        if item.get("exists") is False or not os.path.exists(abs_path):
            continue
        stats = load_csv_quality_stats(abs_path)
        item["csv_quality"] = stats
        rows_total = int(stats.get("rows_total") or 0)
        numeric_rows = int(stats.get("numeric_rows") or 0)
        placeholder_ratio = float(stats.get("placeholder_ratio") or 0.0)
        root_issues: List[str] = []
        if rows_total <= 0:
            root_issues.append("rows_insufficient")
        if numeric_rows <= 0:
            root_issues.append("numeric_rows_insufficient")
        if rows_total > 0 and placeholder_ratio >= 1.0:
            root_issues.append("placeholder_ratio_high")
        if root_issues:
            slim_stats = {"issues": root_issues}
            failures.append(build_csv_quality_failure_text(rel_path, slim_stats))
    return failures


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
    payload, script_payload_error = _build_script_command_payload(payload, ctx)
    if script_payload_error:
        raise ValueError(script_payload_error)

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
    explicit_expected_outputs: List[str] = []
    raw_expected_outputs = payload.get("expected_outputs")
    if isinstance(raw_expected_outputs, list):
        for item in raw_expected_outputs:
            text_item = str(item or "").strip()
            if text_item:
                explicit_expected_outputs.append(text_item)
    original_declared_output_paths = _collect_declared_output_paths(payload)
    if not has_exec_permission(workdir):
        raise ValueError(ERROR_MESSAGE_PERMISSION_DENIED)

    script_arg_contract, script_contract_error = _preflight_script_arg_contract(
        payload,
        workdir,
        task_id=task_id,
        run_id=run_id,
        context=ctx,
    )
    if script_contract_error:
        raise ValueError(script_contract_error)

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

    payload, explicit_stdin_len = _maybe_apply_stdin_from_context(payload=payload, context=ctx)
    payload, auto_attached_stdin_len = _maybe_attach_context_stdin_before_run(payload=payload, context=ctx)
    result, error_message = run_shell_command(payload)
    if error_message:
        raise ValueError(error_message)
    if not isinstance(result, dict):
        raise ValueError(ERROR_MESSAGE_COMMAND_FAILED)
    if (auto_attached_stdin_len > 0 or explicit_stdin_len > 0) and isinstance(result, dict):
        try:
            result = dict(result)
            result["auto_stdin_attached"] = bool(auto_attached_stdin_len > 0)
            result["auto_stdin_chars"] = int(auto_attached_stdin_len)
            if explicit_stdin_len > 0:
                result["stdin_from_context"] = True
                result["stdin_from_context_chars"] = int(explicit_stdin_len)
        except Exception:
            pass

    ok = bool(result.get("ok"))
    if not ok:
        retry_payload: Optional[dict] = None
        retry_url = ""
        retry_stdin_len = 0
        retry_trigger = ""
        retry_meta: Dict[str, object] = {}
        retry_materialized_input_path = ""

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


        if not isinstance(retry_payload, dict):
            retry_payload, retry_materialized_input_path = _build_retry_payload_with_materialized_context_file(
                payload=payload,
                result=result,
                context=ctx,
                workdir=workdir,
            )
            if isinstance(retry_payload, dict):
                retry_trigger = "missing_input_file"
                retry_reason = "missing_input_file"
        if not isinstance(retry_payload, dict):
            retry_payload, retry_meta = _build_retry_payload_with_csv_alias_mapping(
                payload=payload,
                result=result,
                workdir=workdir,
            )
            if isinstance(retry_payload, dict):
                retry_trigger = "missing_required_columns"
                retry_reason = "missing_required_columns"

        if isinstance(retry_payload, dict):
            retry_result, retry_error = run_shell_command(retry_payload)
            if not retry_error and isinstance(retry_result, dict) and bool(retry_result.get("ok")):
                merged_result = dict(retry_result)
                auto_retry_payload = {
                    "trigger": retry_trigger or "auto_retry",
                    "reason": retry_reason or retry_trigger or "auto_retry",
                    "fallback_url": retry_url,
                    "stdin_chars": int(retry_stdin_len or 0),
                    "materialized_input_path": retry_materialized_input_path,
                    "initial_returncode": result.get("returncode"),
                    "initial_stdout": str(result.get("stdout") or ""),
                    "initial_stderr": str(result.get("stderr") or ""),
                    "retry_command": retry_payload.get("command"),
                }
                if retry_meta:
                    auto_retry_payload.update(dict(retry_meta))
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

        # 需要 API Key：归类为可换源的 source failure，避免在同一脚本上反复重试。
        if any(
            marker in combined_lower
            for marker in (
                "missing_access_key",
                "api access key",
                "api key",
                "access key",
                "requires an api key",
            )
        ):
            raise ValueError(
                format_task_error(
                    code="missing_api_key",
                    message=f"{ERROR_MESSAGE_COMMAND_FAILED}: 外部源需要 API Key（{detail or 'missing_access_key'}）",
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

        if any(
            marker in combined_lower
            for marker in (
                "未解析出任何价格数据",
                "未解析到任何价格数据",
                "no data parsed",
                "no price data parsed",
                "no structured data extracted",
            )
        ):
            raise ValueError(
                format_task_error(
                    code="no_structured_data_extracted",
                    message=f"{ERROR_MESSAGE_COMMAND_FAILED}: 未从当前样本中解析出可用结构化数据（{detail or 'no structured data extracted'}）",
                )
            )

        raise ValueError(f"{ERROR_MESSAGE_COMMAND_FAILED}:{detail}" if detail else ERROR_MESSAGE_COMMAND_FAILED)

    if script_arg_contract:
        try:
            result = dict(result)
            result["script_contract"] = dict(script_arg_contract)
        except Exception:
            pass

    parse_json_output = bool(payload.get("parse_json_output"))
    if parse_json_output:
        stdout_text = str(result.get("stdout") or "").strip()
        parsed_obj = parse_json_value(stdout_text)
        if parsed_obj is None:
            raise ValueError(
                format_task_error(
                    code="script_output_not_json",
                    message=f"{ERROR_MESSAGE_COMMAND_FAILED}: 脚本输出不是合法 JSON",
                )
            )
        result["parsed_output"] = parsed_obj
        emit_as = str(payload.get("emit_as") or "").strip()
        if emit_as and isinstance(ctx, dict):
            ctx[emit_as] = parsed_obj
            result["emitted_context_key"] = emit_as

    final_declared_output_paths = _collect_declared_output_paths(payload)
    original_declared_set = {str(item) for item in original_declared_output_paths}
    stable_declared_outputs = [item for item in final_declared_output_paths if item in original_declared_set]
    declared_output_paths = _merge_unique_args(explicit_expected_outputs, stable_declared_outputs)
    artifacts, missing_outputs = _collect_declared_output_artifacts(
        payload,
        workdir,
        declared_paths=declared_output_paths,
    )
    if artifacts:
        result["artifacts"] = artifacts
    if missing_outputs:
        raise ValueError(
            format_task_error(
                code="missing_expected_artifact",
                message=(
                    f"{ERROR_MESSAGE_COMMAND_FAILED}: 缺少预期产物（{', '.join(missing_outputs)}）"
                ),
            )
        )

    csv_quality_failures = _collect_csv_artifact_quality_failures(artifacts, context=ctx)
    if csv_quality_failures:
        raise ValueError(
            format_task_error(
                code="csv_artifact_quality_failed",
                message=(
                    f"{ERROR_MESSAGE_COMMAND_FAILED}: CSV 产物质量未通过（{' | '.join(csv_quality_failures)}）"
                ),
            )
        )

    return result, None
