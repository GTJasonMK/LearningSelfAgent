from __future__ import annotations

import json
from typing import Any, Callable, List, Sequence


_FAILURE_DEBUG_KEYWORDS = (
    "failed",
    "error",
    "exception",
    "missing",
    "warning",
    "失败",
    "错误",
    "异常",
    "缺失",
)


def truncate_inline_text(value: object, max_chars: int = 180) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    limit = max(1, int(max_chars))
    if len(raw) <= limit:
        return raw
    return f"{raw[: max(0, limit - 1)]}…"


def read_row_value(row_obj: object, key: str):
    if isinstance(row_obj, dict):
        return row_obj.get(key)
    try:
        return row_obj[key]
    except (TypeError, IndexError, KeyError):
        return None


def extract_step_error_text(
    step_row: object,
    *,
    read_value: Callable[[object, str], Any] = read_row_value,
) -> str:
    direct = truncate_inline_text(read_value(step_row, "error"), 180)
    if direct:
        return direct

    result_raw = str(read_value(step_row, "result") or "").strip()
    if not result_raw:
        return ""

    try:
        parsed = json.loads(result_raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return truncate_inline_text(result_raw, 180)

    if isinstance(parsed, dict):
        for key in ("error", "stderr", "message"):
            picked = truncate_inline_text(parsed.get(key), 180)
            if picked:
                return picked

    return truncate_inline_text(result_raw, 180)


def build_failed_step_lines_from_rows(
    rows: Sequence[object],
    *,
    read_value: Callable[[object, str], Any] = read_row_value,
    max_items: int = 6,
) -> List[str]:
    lines: List[str] = []
    for row in rows or []:
        status = str(read_value(row, "status") or "").strip().lower()
        if status != "failed":
            continue
        step_order = read_value(row, "step_order")
        order_text = str(step_order) if step_order is not None else "?"
        title = str(read_value(row, "title") or "").strip()
        error_text = extract_step_error_text(row, read_value=read_value) or "未记录错误详情"
        lines.append(f"- step#{order_text} {title or '(未命名步骤)'} -> {error_text}")
        if len(lines) >= int(max_items):
            break
    return lines


def build_failure_debug_lines_from_rows(
    rows: Sequence[object],
    *,
    debug_output_type: str,
    read_value: Callable[[object, str], Any] = read_row_value,
    max_items: int = 3,
) -> List[str]:
    matched: List[str] = []
    for row in rows or []:
        output_type = str(read_value(row, "output_type") or "").strip().lower()
        if output_type != str(debug_output_type or "").strip().lower():
            continue
        content = truncate_inline_text(read_value(row, "content"), 180)
        if not content:
            continue
        lowered = content.lower()
        if not any(keyword in lowered for keyword in _FAILURE_DEBUG_KEYWORDS):
            continue
        matched.append(f"- {content}")
        if len(matched) >= int(max_items):
            break
    return list(reversed(matched))


def has_text_output_in_rows(
    rows: Sequence[object],
    *,
    text_output_type: str,
    read_value: Callable[[object, str], Any] = read_row_value,
) -> bool:
    for row in rows or []:
        output_type = str(read_value(row, "output_type") or "").strip().lower()
        if output_type != str(text_output_type or "").strip().lower():
            continue
        content = str(read_value(row, "content") or "").strip()
        if content:
            return True
    return False


def safe_collect_failed_step_lines(
    *,
    task_id: int,
    run_id: int,
    list_steps_for_run: Callable[..., Sequence[object]],
    read_value: Callable[[object, str], Any] = read_row_value,
    handled_errors: tuple[type[BaseException], ...] = (Exception,),
    max_items: int = 6,
) -> List[str]:
    try:
        rows = list_steps_for_run(task_id=int(task_id), run_id=int(run_id))
    except handled_errors:
        return []
    return build_failed_step_lines_from_rows(rows=rows, read_value=read_value, max_items=max_items)


def safe_collect_failure_debug_lines(
    *,
    task_id: int,
    run_id: int,
    list_outputs_for_run: Callable[..., Sequence[object]],
    debug_output_type: str,
    read_value: Callable[[object, str], Any] = read_row_value,
    handled_errors: tuple[type[BaseException], ...] = (Exception,),
    max_items: int = 3,
    limit: int = 30,
) -> List[str]:
    try:
        rows = list_outputs_for_run(
            task_id=int(task_id),
            run_id=int(run_id),
            order="DESC",
            limit=int(limit),
        )
    except handled_errors:
        return []
    return build_failure_debug_lines_from_rows(
        rows=rows,
        debug_output_type=debug_output_type,
        read_value=read_value,
        max_items=max_items,
    )


def safe_has_text_output(
    *,
    task_id: int,
    run_id: int,
    list_outputs_for_run: Callable[..., Sequence[object]],
    text_output_type: str,
    read_value: Callable[[object, str], Any] = read_row_value,
    handled_errors: tuple[type[BaseException], ...] = (Exception,),
    limit: int = 20,
) -> bool:
    try:
        rows = list_outputs_for_run(
            task_id=int(task_id),
            run_id=int(run_id),
            order="DESC",
            limit=int(limit),
        )
    except handled_errors:
        return False
    return has_text_output_in_rows(rows=rows, text_output_type=text_output_type, read_value=read_value)


def build_failed_task_output_content(
    *,
    task_id: int,
    run_id: int,
    failed_steps: List[str],
    debug_lines: List[str],
) -> str:
    normalized_failed_steps = list(failed_steps or [])
    normalized_debug_lines = list(debug_lines or [])
    if not normalized_failed_steps:
        normalized_failed_steps = ["- 无（可能在规划阶段或执行初始化阶段失败）"]
    if not normalized_debug_lines:
        normalized_debug_lines = ["- 无（未捕获到额外 debug 证据）"]
    lines = [
        "【失败总结】",
        f"- task_id: {int(task_id)}",
        f"- run_id: {int(run_id)}",
        "- 结论：本次执行未完成，状态为 failed。",
        "",
        "[失败步骤]",
        *normalized_failed_steps,
        "",
        "[关键证据]",
        *normalized_debug_lines,
        "",
        "[建议下一步]",
        "- 优先修复首个失败步骤，再重试执行。",
        "- 若失败来自外部依赖，请补齐输入并增加校验步骤。",
    ]
    return "\n".join(lines).strip()
