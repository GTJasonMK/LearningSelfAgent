from __future__ import annotations

from typing import Dict, Mapping, Optional, Tuple

from backend.src.common.utils import coerce_int


DEFAULT_CONTEXT_SECTION_BUDGETS: Dict[str, int] = {
    "observations": 2400,
    "recent_source_failures": 1000,
    "graph": 2200,
    "tools": 1400,
    "skills": 2600,
    "solutions": 2600,
    "memories": 1200,
}
CONTEXT_BUDGET_PIPELINE_VERSION = 1


def trim_text_for_budget(text: object, max_chars: int) -> str:
    raw = str(text or "")
    limit = coerce_int(max_chars, default=0)
    if limit <= 0:
        return ""
    if len(raw) <= limit:
        return raw
    reserve = min(48, max(16, limit // 10))
    body_limit = max(0, limit - reserve)
    trimmed = raw[:body_limit].rstrip()
    truncated = len(raw) - len(trimmed)
    return f"{trimmed}\n...(已截断 {coerce_int(truncated, default=0)} 字符)"


def compress_text_soft(text: object, *, max_chars: int) -> str:
    """
    轻量压缩（第三阶段）：
    - 折叠连续空行；
    - 删除行尾空白；
    - 若仍超限，按预算截断。
    """
    raw = str(text or "")
    if not raw:
        return ""
    lines = [str(line or "").rstrip() for line in raw.splitlines()]
    compact_lines = []
    previous_blank = False
    limit = coerce_int(max_chars, default=0)
    for line in lines:
        is_blank = not str(line).strip()
        if is_blank and previous_blank:
            continue
        compact_lines.append(line)
        previous_blank = is_blank
    compact = "\n".join(compact_lines).strip()
    if limit <= 0:
        return ""
    if len(compact) <= limit:
        return compact
    return trim_text_for_budget(compact, limit)


def resolve_context_budgets(*, budgets: Optional[Mapping[str, int]] = None) -> Dict[str, int]:
    base = dict(DEFAULT_CONTEXT_SECTION_BUDGETS)
    if isinstance(budgets, Mapping):
        for key, value in budgets.items():
            normalized = coerce_int(value, default=0)
            if normalized > 0:
                base[str(key)] = normalized
    return base


def apply_context_budget_pipeline(
    sections: Mapping[str, object],
    *,
    budgets: Optional[Mapping[str, int]] = None,
) -> Tuple[Dict[str, str], Dict[str, object]]:
    """
    上下文预算三段式：
    1) load：对象统一转文本并记录原始长度；
    2) trim：按 section budget 截断；
    3) compress：对仍接近预算上限的 section 做轻量压缩。
    """
    budget_map = resolve_context_budgets(budgets=budgets)
    loaded: Dict[str, str] = {}
    raw_lengths: Dict[str, int] = {}
    for key, value in sections.items():
        name = str(key)
        text = str(value or "")
        loaded[name] = text
        raw_lengths[name] = len(text)

    trimmed: Dict[str, str] = {}
    trimmed_lengths: Dict[str, int] = {}
    for name, text in loaded.items():
        if name not in budget_map:
            trimmed[name] = text
        else:
            trimmed[name] = trim_text_for_budget(text, coerce_int(budget_map[name], default=0))
        trimmed_lengths[name] = len(str(trimmed[name] or ""))

    compressed: Dict[str, str] = {}
    compressed_lengths: Dict[str, int] = {}
    for name, text in trimmed.items():
        limit = coerce_int(budget_map.get(name, 0), default=0)
        soft_threshold = coerce_int(limit * 0.9, default=0)
        if limit > 0 and len(str(text or "")) >= soft_threshold:
            out = compress_text_soft(text, max_chars=limit)
        else:
            out = str(text or "")
        compressed[name] = out
        compressed_lengths[name] = len(out)

    meta: Dict[str, object] = {
        "version": coerce_int(CONTEXT_BUDGET_PIPELINE_VERSION, default=1),
        "stages": ["load", "trim", "compress"],
        "budgets": budget_map,
        "raw_lengths": raw_lengths,
        "trimmed_lengths": trimmed_lengths,
        "compressed_lengths": compressed_lengths,
    }
    return compressed, meta


def apply_context_budgets(
    sections: Mapping[str, object],
    *,
    budgets: Optional[Mapping[str, int]] = None,
) -> Dict[str, str]:
    out, _meta = apply_context_budget_pipeline(sections, budgets=budgets)
    return out
