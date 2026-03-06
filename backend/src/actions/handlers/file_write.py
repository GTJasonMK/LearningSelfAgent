import ast
import os
import re
from typing import Optional, Tuple

from backend.src.common.task_error_codes import format_task_error

from backend.src.actions.handlers.file_action_common import (
    ensure_write_permission_for_action,
    normalize_encoding,
    require_action_path,
)
from backend.src.actions.file_write import write_text_file
from backend.src.common.csv_artifact_quality import load_csv_quality_stats_from_text

# 仅拦截“明显模拟数据”标记，避免对正常业务词汇误报。
_SIMULATED_DATA_PATTERNS = (
    re.compile(r"\b(simulated?|synthetic|fabricated|mock|fake|dummy)\b", re.IGNORECASE),
    re.compile(r"(模拟数据|虚构数据|示例数据|假数据|随机生成|测试数据)"),
)

# 当前仅对业务 CSV 启用前置门禁，避免误伤代码/文档写入。
_BUSINESS_DATA_EXTENSIONS = {".csv"}
_SCRIPT_QUALITY_EXTENSIONS = {".py"}
_SCRIPT_QUALITY_EXEMPT_FILENAMES = {"__init__.py"}
_SCRIPT_PLACEHOLDER_PATTERNS = (
    re.compile(r"\b(todo|fixme|placeholder|skeleton|stub|tbd|to be implemented)\b", re.IGNORECASE),
    re.compile(r"\b(minimal executable|example only|sample source|assumed data structure|assumed response shape)\b", re.IGNORECASE),
    re.compile(r"(待实现|后续实现|占位|骨架|先留空|后续根据实际|根据真实数据调整|根据实际抓取结果调整|示例来源|假设数据结构|假设返回结构|假设响应结构|实际结构需根据观测调整|需根据观测调整|需根据实际响应调整|需根据返回结构调整|期望格式|先尝试常见|当前观测|根据观测到的实际数据结构调整|可能是 JSON 或 HTML|可能是单个对象或嵌套结构|实际过滤应.*这里先|未识别结构.*供调试)"),
)
_SCRIPT_STRONG_PLACEHOLDER_PATTERNS = (
    re.compile(r"\b(assumed data structure|assumed response shape)\b", re.IGNORECASE),
    re.compile(r"(假设数据结构|假设返回结构|假设响应结构|实际结构需根据观测调整|需根据观测调整|需根据实际响应调整|需根据返回结构调整|期望格式|先尝试常见|当前观测|根据观测到的实际数据结构调整|可能是 JSON 或 HTML|实际过滤应.*这里先)"),
)
_SCRIPT_PRINTLIKE_CALLS = {
    "print",
    "pprint",
    "logging.debug",
    "logging.info",
    "logging.warning",
    "logging.error",
    "logger.debug",
    "logger.info",
    "logger.warning",
    "logger.error",
}
_SCRIPT_ALWAYS_TRIVIAL_CALLS = {
    "main",
    "exit",
    "quit",
    "sys.exit",
    "sys.path.insert",
}


def _is_business_data_path(path: str) -> bool:
    ext = os.path.splitext(str(path or "").strip().lower())[1]
    return ext in _BUSINESS_DATA_EXTENSIONS



def _is_python_script_path(path: str) -> bool:
    raw_path = str(path or "").strip()
    if not raw_path:
        return False
    filename = os.path.basename(raw_path).strip().lower()
    if filename in _SCRIPT_QUALITY_EXEMPT_FILENAMES:
        return False
    ext = os.path.splitext(filename)[1]
    return ext in _SCRIPT_QUALITY_EXTENSIONS



def _detect_simulated_marker(text: object) -> Optional[str]:
    raw = str(text or "")
    if not raw.strip():
        return None
    for pattern in _SIMULATED_DATA_PATTERNS:
        match = pattern.search(raw)
        if match:
            return str(match.group(0) or "").strip() or None
    return None



def _detect_script_placeholder_marker(text: object) -> Optional[str]:
    raw = str(text or "")
    if not raw.strip():
        return None
    for pattern in _SCRIPT_PLACEHOLDER_PATTERNS:
        match = pattern.search(raw)
        if match:
            return str(match.group(0) or "").strip() or None
    return None


def _is_strong_script_placeholder_marker(marker: object) -> bool:
    raw = str(marker or "").strip()
    if not raw:
        return False
    for pattern in _SCRIPT_STRONG_PLACEHOLDER_PATTERNS:
        if pattern.search(raw):
            return True
    return False



def _collect_context_evidence_text(context: Optional[dict]) -> str:
    if not isinstance(context, dict):
        return ""

    chunks = []

    parse_text = str(context.get("latest_parse_input_text") or "").strip()
    if parse_text:
        chunks.append(parse_text)

    observations = context.get("observations")
    if isinstance(observations, list):
        for item in observations[-3:]:
            line = str(item or "").strip()
            if line:
                chunks.append(line)

    auto_retry = context.get("latest_shell_auto_retry")
    if isinstance(auto_retry, dict):
        for key in ("initial_stdout", "initial_stderr", "fallback_url", "trigger"):
            value = str(auto_retry.get(key) or "").strip()
            if value:
                chunks.append(value)

    return "\n".join(chunks)



def _sample_looks_like_html(text: object) -> bool:
    raw = str(text or '').lstrip().lower()
    if not raw:
        return False
    if raw.startswith('<!doctype html') or raw.startswith('<html'):
        return True
    return '<html' in raw[:1200] and '</html>' in raw[:4000]


_HTML_SCRIPT_SIGNAL_TOKENS = (
    'beautifulsoup',
    'bs4',
    'html.parser',
    'htmlparser',
    'lxml',
    'xpath',
    're.search',
    're.findall',
    're.finditer',
    '.find_all(',
    '.select(',
    'splitlines(',
    'raw_text',
    'raw_html',
)


def _script_handles_html_sample(text: object) -> bool:
    lowered = str(text or '').lower()
    if not lowered.strip():
        return False
    return any(token in lowered for token in _HTML_SCRIPT_SIGNAL_TOKENS)


def _derive_script_sample_grounding_issue(content: str, context: Optional[dict]) -> Optional[str]:
    if not isinstance(context, dict):
        return None
    sample = str(context.get('latest_parse_input_text') or '').strip()
    if not sample:
        return None
    lowered = str(content or '').lower()
    json_only = ('json.loads(' in lowered or 'json.load(' in lowered)
    if _sample_looks_like_html(sample) and json_only and not _script_handles_html_sample(content):
        return '当前真实样本是 HTML，但脚本仍按 JSON 结构假设解析，缺少面向 HTML 的提取逻辑'
    return None


def _maybe_warn_business_data_write(path: str, content: str, context: Optional[dict]) -> Optional[str]:
    if not _is_business_data_path(path):
        return None

    if isinstance(context, dict):
        # 与现有上下文开关对齐：默认开启，只有显式 False 才关闭。
        enabled = context.get("enforce_business_data_source_guard")
        if enabled is None:
            enabled = context.get("enforce_csv_artifact_quality", True)
        if enabled is False:
            return None

    content_marker = _detect_simulated_marker(content)
    context_marker = _detect_simulated_marker(_collect_context_evidence_text(context))
    if not content_marker and not context_marker:
        return None

    marker = content_marker or context_marker or "simulated"
    return (
        "file_write 检测到业务 CSV 可能来自低可信来源/模拟数据标记"
        f"（{marker}）。建议先完成真实数据抓取与解析，再落盘结果。"
    )



def _validate_business_data_write(path: str, content: str) -> Optional[str]:
    """
    业务 CSV 写入硬校验：
    - 不能为空；
    - 不能只有表头；
    - 至少包含一行可解析的数值数据。
    """
    if not _is_business_data_path(path):
        return None

    csv_text = str(content or '').strip()
    if not csv_text:
        return 'file_write 拒绝写入空 CSV：请先完成数据抓取/换算后再写入结果文件。'

    stats = load_csv_quality_stats_from_text(csv_text)
    rows_total = int(stats.get('rows_total') or 0)
    numeric_rows = int(stats.get('numeric_rows') or 0)

    if rows_total <= 0:
        return format_task_error(
            code='csv_artifact_quality_failed',
            message='file_write.csv 拒绝写入空 CSV 或仅含表头的 CSV：请先抓取并解析至少一行真实数据后再落盘。',
        )
    if numeric_rows <= 0:
        return format_task_error(
            code='csv_artifact_quality_failed',
            message='file_write.csv 缺少可解析的数值行：请先完成真实数据抓取/换算，再写入结果文件。',
        )
    return None



def _detect_code_like_content_kind(content: str) -> Optional[str]:
    raw = str(content or "")
    if not raw.strip():
        return None

    stripped = raw.lstrip()
    first_line = stripped.splitlines()[0].strip() if stripped.splitlines() else ""
    lowered = stripped.lower()
    markers = (
        "#!/usr/bin/env python",
        "from __future__ import",
        'if __name__ == "__main__":',
        "raise systemexit",
        "import argparse",
        "import csv",
        "import json",
        "def main(",
        "class ",
        "def ",
    )
    if first_line.startswith("#!"):
        return "script"
    if any(marker in lowered for marker in markers):
        return "script"
    return None



def _looks_like_csv_content(content: str) -> bool:
    raw = str(content or "").strip()
    if not raw:
        return False
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    if len(lines) < 2:
        return False
    header = lines[0]
    first_row = lines[1]
    return ("," in header) and ("," in first_row)



def _normalize_csv_text_for_compare(content: str) -> str:
    lines = [str(line or "").strip() for line in str(content or "").splitlines() if str(line or "").strip()]
    return "\n".join(lines)



def _python_call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return str(node.id or "")
    if isinstance(node, ast.Attribute):
        prefix = _python_call_name(node.value)
        suffix = str(node.attr or "")
        if prefix and suffix:
            return f"{prefix}.{suffix}"
        return suffix
    return ""



def _is_python_main_guard(test: ast.AST) -> bool:
    if not isinstance(test, ast.Compare):
        return False
    if len(test.ops) != 1 or len(test.comparators) != 1:
        return False
    if not isinstance(test.ops[0], ast.Eq):
        return False

    left = test.left
    right = test.comparators[0]
    pairs = ((left, right), (right, left))
    for first, second in pairs:
        if isinstance(first, ast.Name) and first.id == "__name__":
            if isinstance(second, ast.Constant) and second.value == "__main__":
                return True
    return False



def _python_expr_has_substantive_signal(node: Optional[ast.AST]) -> bool:
    if node is None:
        return False
    if isinstance(node, ast.Call):
        call_name = _python_call_name(node.func)
        arg_signal = any(_python_expr_has_substantive_signal(arg) for arg in node.args)
        kw_signal = any(_python_expr_has_substantive_signal(kw.value) for kw in node.keywords)
        if call_name in _SCRIPT_ALWAYS_TRIVIAL_CALLS:
            return False
        if call_name in _SCRIPT_PRINTLIKE_CALLS:
            return arg_signal or kw_signal
        return True
    if isinstance(node, ast.Constant):
        return False
    if isinstance(node, ast.Name):
        return False
    if isinstance(node, ast.Attribute):
        return False
    if isinstance(node, ast.Subscript):
        return True
    if isinstance(node, (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)):
        return True
    if isinstance(node, ast.NamedExpr):
        return _python_expr_has_substantive_signal(node.value)
    if isinstance(node, ast.UnaryOp):
        return _python_expr_has_substantive_signal(node.operand)
    if isinstance(node, ast.BinOp):
        return _python_expr_has_substantive_signal(node.left) or _python_expr_has_substantive_signal(node.right)
    if isinstance(node, ast.BoolOp):
        return any(_python_expr_has_substantive_signal(value) for value in node.values)
    if isinstance(node, ast.Compare):
        if _python_expr_has_substantive_signal(node.left):
            return True
        return any(_python_expr_has_substantive_signal(comp) for comp in node.comparators)
    if isinstance(node, ast.IfExp):
        return any(
            _python_expr_has_substantive_signal(part)
            for part in (node.test, node.body, node.orelse)
        )
    if isinstance(node, ast.JoinedStr):
        return any(_python_expr_has_substantive_signal(value) for value in node.values)
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        return any(_python_expr_has_substantive_signal(item) for item in node.elts)
    if isinstance(node, ast.Dict):
        return any(_python_expr_has_substantive_signal(item) for item in node.keys if item is not None) or any(
            _python_expr_has_substantive_signal(item) for item in node.values
        )
    return True



def _python_block_has_substantive_signal(items: list[ast.stmt]) -> bool:
    return any(_python_stmt_has_substantive_signal(item) for item in items)



def _python_expr_is_empty_stub_value(node: Optional[ast.AST]) -> bool:
    if node is None:
        return True
    if isinstance(node, ast.Constant):
        return node.value in (None, False, 0, "")
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        return len(node.elts) == 0
    if isinstance(node, ast.Dict):
        return len(node.keys) == 0 and len(node.values) == 0
    return False


def _python_trim_docstring(items: list[ast.stmt]) -> list[ast.stmt]:
    if items and isinstance(items[0], ast.Expr):
        value = getattr(items[0], "value", None)
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            return list(items[1:])
    return list(items)


def _python_function_is_stub(node: ast.AST) -> bool:
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return False
    body = _python_trim_docstring(list(node.body))
    if not body:
        return True
    if any(_python_stmt_has_substantive_signal(item) for item in body):
        return False
    for item in body:
        if isinstance(item, ast.Pass):
            continue
        if isinstance(item, ast.Return) and _python_expr_is_empty_stub_value(item.value):
            continue
        if isinstance(item, ast.Raise):
            exc = item.exc
            if isinstance(exc, ast.Call) and _python_call_name(exc.func) == "NotImplementedError":
                continue
        if isinstance(item, ast.Expr) and isinstance(item.value, ast.Call):
            call_name = _python_call_name(item.value.func)
            if call_name in _SCRIPT_PRINTLIKE_CALLS or call_name in _SCRIPT_ALWAYS_TRIVIAL_CALLS:
                continue
        return False
    return True


def _collect_python_stub_function_names(tree: ast.Module) -> list[str]:
    names: list[str] = []
    for item in list(tree.body):
        if _python_function_is_stub(item):
            names.append(str(getattr(item, "name", "") or "stub"))
    return names


def _python_stmt_has_substantive_signal(node: ast.stmt) -> bool:
    if isinstance(node, (ast.Import, ast.ImportFrom, ast.Pass, ast.Global, ast.Nonlocal, ast.Break, ast.Continue)):
        return False
    if isinstance(node, ast.Expr):
        if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            return False
        return _python_expr_has_substantive_signal(node.value)
    if isinstance(node, ast.Return):
        return _python_expr_has_substantive_signal(node.value)
    if isinstance(node, ast.Assign):
        return _python_expr_has_substantive_signal(node.value)
    if isinstance(node, ast.AnnAssign):
        return _python_expr_has_substantive_signal(node.value)
    if isinstance(node, ast.AugAssign):
        return _python_expr_has_substantive_signal(node.value)
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return _python_block_has_substantive_signal(node.body)
    if isinstance(node, ast.If):
        if _is_python_main_guard(node.test):
            return _python_block_has_substantive_signal(node.body + node.orelse)
        return _python_expr_has_substantive_signal(node.test) or _python_block_has_substantive_signal(node.body + node.orelse)
    if isinstance(node, (ast.For, ast.AsyncFor)):
        return _python_expr_has_substantive_signal(node.iter) or _python_block_has_substantive_signal(node.body + node.orelse)
    if isinstance(node, ast.While):
        return _python_expr_has_substantive_signal(node.test) or _python_block_has_substantive_signal(node.body + node.orelse)
    if isinstance(node, (ast.With, ast.AsyncWith)):
        if any(_python_expr_has_substantive_signal(item.context_expr) for item in node.items):
            return True
        return _python_block_has_substantive_signal(node.body)
    if isinstance(node, ast.Try):
        for handler in node.handlers:
            if handler.type is not None and _python_expr_has_substantive_signal(handler.type):
                return True
            if _python_block_has_substantive_signal(handler.body):
                return True
        return _python_block_has_substantive_signal(node.body + node.orelse + node.finalbody)
    if isinstance(node, ast.Raise):
        exc = node.exc
        if isinstance(exc, ast.Call) and _python_call_name(exc.func) == "NotImplementedError":
            return False
        return exc is not None
    if isinstance(node, ast.Assert):
        return _python_expr_has_substantive_signal(node.test) or _python_expr_has_substantive_signal(node.msg)
    if isinstance(node, ast.Delete):
        return True
    return True



def _validate_python_script_quality(path: str, content: str, context: Optional[dict]) -> Optional[str]:
    if not _is_python_script_path(path):
        return None

    if isinstance(context, dict):
        enabled = context.get("enforce_script_quality_guard")
        if enabled is False:
            return None

    text = str(content or "")
    if not text.strip():
        return format_task_error(
            code="file_write_placeholder_script",
            message="file_write.py 拒绝写入空脚本；请直接写入可执行的真实逻辑。",
        )

    try:
        tree = ast.parse(text)
    except SyntaxError as exc:
        detail = f"第 {int(exc.lineno or 0)} 行存在语法错误" if getattr(exc, "lineno", None) else "存在语法错误"
        return format_task_error(
            code="file_write_python_syntax_error",
            message=f"file_write.py 写入内容不是有效的 Python 脚本：{detail}。",
        )

    placeholder_marker = _detect_script_placeholder_marker(text)
    has_substantive_logic = _python_block_has_substantive_signal(list(tree.body))
    stub_functions = _collect_python_stub_function_names(tree)
    sample_grounding_issue = _derive_script_sample_grounding_issue(text, context)
    reasons = []
    if placeholder_marker and (not has_substantive_logic or _is_strong_script_placeholder_marker(placeholder_marker)):
        reasons.append(f"检测到占位标记：{placeholder_marker}")
    if stub_functions:
        reasons.append("检测到占位函数：" + ", ".join(stub_functions[:3]))
    if not has_substantive_logic:
        reasons.append("脚本仅包含导入、说明性 print、main 包装或常量返回，缺少真实输入处理/数据转换/结果产出逻辑")
    if sample_grounding_issue:
        reasons.append(sample_grounding_issue)
    if not reasons:
        return None
    return format_task_error(
        code="file_write_placeholder_script",
        message=(
            "file_write.py 拒绝写入明显占位脚本；"
            + "；".join(reasons)
            + "。请先补齐可执行的真实逻辑，再把该步骤标记为完成。"
        ),
    )



def _validate_business_data_source_binding(path: str, content: str, context: Optional[dict]) -> Optional[str]:
    if not _is_business_data_path(path):
        return None
    csv_text = str(content or "").strip()
    if not csv_text:
        return None
    if not _looks_like_csv_content(csv_text):
        return None
    if not isinstance(context, dict):
        return format_task_error(
            code="business_data_source_missing",
            message="file_write.csv 缺少最近真实观测绑定；请先通过 tool_call/shell_command 生成 CSV，再落盘结果。",
        )

    source_text = str(context.get("latest_parse_input_text") or "").strip()
    if not source_text:
        return format_task_error(
            code="business_data_source_missing",
            message="file_write.csv 缺少最近真实观测绑定；请先通过 tool_call/shell_command 生成 CSV，再落盘结果。",
        )
    if not _looks_like_csv_content(source_text):
        return format_task_error(
            code="business_data_source_not_csv",
            message=(
                "file_write.csv 只允许写入最近真实解析得到的 CSV 文本；"
                "当前最近观测不是 CSV，请先继续抓取或使用 shell_command/tool_call 生成 CSV。"
            ),
        )

    if _normalize_csv_text_for_compare(csv_text) != _normalize_csv_text_for_compare(source_text):
        return format_task_error(
            code="business_data_source_not_grounded",
            message=(
                "file_write.csv content 与最近真实解析结果不一致；"
                "请直接复用最近 CSV 结果，或先通过 shell_command/tool_call 生成新的 CSV。"
            ),
        )
    return None



def validate_file_write_payload_semantics(path: str, content: str) -> Optional[str]:
    ext = os.path.splitext(str(path or "").strip().lower())[1]
    code_kind = _detect_code_like_content_kind(content)

    if ext == ".csv" and code_kind:
        return format_task_error(
            code="file_write_content_path_mismatch",
            message=(
                "file_write.path 指向 CSV 结果文件，但 content 看起来是脚本代码；"
                "请先把脚本写入 .py 文件，再在脚本执行成功后写入 CSV 结果。"
            ),
        )

    if ext == ".py" and _looks_like_csv_content(content) and not code_kind:
        return format_task_error(
            code="file_write_content_path_mismatch",
            message=(
                "file_write.path 指向 Python 脚本，但 content 看起来是 CSV 数据；"
                "请把 CSV 写入结果文件，把脚本写入 .py 文件。"
            ),
        )

    return None



def execute_file_write(
    payload: dict,
    *,
    context: Optional[dict] = None,
) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行 file_write：写入文本文件。
    """
    path = require_action_path(payload, "file_write")
    permission_error = ensure_write_permission_for_action(path, "file_write")
    if permission_error:
        return None, permission_error

    content = payload.get("content")
    if content is None:
        content = ""
    if not isinstance(content, str):
        raise ValueError("file_write.content 必须是字符串")

    semantic_error = validate_file_write_payload_semantics(path=path, content=content)
    if semantic_error:
        return None, semantic_error

    script_quality_error = _validate_python_script_quality(path=path, content=content, context=context)
    if script_quality_error:
        return None, script_quality_error

    # 开发阶段严格模式：业务 CSV 空写入一律失败，避免把“空产物”标记为成功步骤。
    validation_error = _validate_business_data_write(path=path, content=content)
    if validation_error:
        return None, validation_error

    source_binding_error = _validate_business_data_source_binding(path=path, content=content, context=context)
    if source_binding_error:
        return None, source_binding_error

    warnings = []
    warn_text = _maybe_warn_business_data_write(path=path, content=content, context=context)
    if warn_text:
        warnings.append(warn_text)
        if isinstance(context, dict):
            items = context.get("quality_warnings")
            if not isinstance(items, list):
                items = []
            items.append(warn_text)
            context["quality_warnings"] = items

    encoding = normalize_encoding(payload.get("encoding"))

    result = write_text_file(path=path, content=content, encoding=encoding)
    if warnings:
        try:
            result = dict(result or {})
        except Exception:
            result = {"path": str(path), "bytes": 0}
        result["warnings"] = warnings
    return result, None
