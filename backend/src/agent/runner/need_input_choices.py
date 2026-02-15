# -*- coding: utf-8 -*-
from typing import List, Optional

from backend.src.constants import (
    AGENT_KNOWLEDGE_SUFFICIENCY_KIND,
    AGENT_TASK_FEEDBACK_KIND,
    AGENT_USER_PROMPT_CHOICES_MAX,
)

_YES_NO_CHOICES: List[dict] = [
    {"label": "是", "value": "是"},
    {"label": "否", "value": "否"},
]

_KNOWLEDGE_SUFFICIENCY_CHOICES: List[dict] = [
    {"label": "按当前信息继续", "value": "请按当前已知信息继续执行，并明确列出关键假设。"},
    {"label": "先给我澄清问题", "value": "请先给我需要补充的关键问题列表，我再补充。"},
]

_YES_NO_QUESTION_HINTS: tuple = (
    "是否",
    "可否",
    "能否",
    "要不要",
    "确认",
    "同意",
    "满意",
)


def normalize_need_input_choices(raw_choices, *, limit: Optional[int] = None) -> List[dict]:
    if not isinstance(raw_choices, list):
        return []

    max_items = int(limit or AGENT_USER_PROMPT_CHOICES_MAX or 12)
    if max_items <= 0:
        max_items = 12

    out: List[dict] = []
    seen: set = set()
    for raw_item in raw_choices:
        label = ""
        value = ""
        if isinstance(raw_item, str):
            label = str(raw_item or "").strip()
            value = label
        elif isinstance(raw_item, dict):
            label = str(raw_item.get("label") or "").strip()
            raw_value = raw_item.get("value")
            value = str(raw_value if raw_value is not None else label).strip()
        if not label or not value:
            continue
        key = (label, value)
        if key in seen:
            continue
        seen.add(key)
        out.append({"label": label, "value": value})
        if len(out) >= max_items:
            break
    return out


def _looks_like_yes_no_question(question: str) -> bool:
    text = str(question or "").strip()
    if not text:
        return False
    if any(token in text for token in _YES_NO_QUESTION_HINTS):
        return True
    return text.endswith(("吗", "吗？", "吗?"))


def build_default_need_input_choices(*, question: str, kind: Optional[str]) -> List[dict]:
    normalized_kind = str(kind or "").strip().lower()
    if normalized_kind == AGENT_TASK_FEEDBACK_KIND:
        return [dict(item) for item in _YES_NO_CHOICES]
    if normalized_kind == AGENT_KNOWLEDGE_SUFFICIENCY_KIND:
        return [dict(item) for item in _KNOWLEDGE_SUFFICIENCY_CHOICES]
    if _looks_like_yes_no_question(str(question or "")):
        return [dict(item) for item in _YES_NO_CHOICES]
    return []


def resolve_need_input_choices(
    *,
    raw_choices,
    question: str,
    kind: Optional[str],
    limit: Optional[int] = None,
) -> List[dict]:
    normalized = normalize_need_input_choices(raw_choices, limit=limit)
    if normalized:
        return normalized
    return build_default_need_input_choices(question=str(question or ""), kind=kind)
