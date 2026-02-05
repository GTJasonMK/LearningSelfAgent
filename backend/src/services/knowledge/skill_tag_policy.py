from __future__ import annotations

import re
from typing import Any, List, Optional, Tuple

from backend.src.constants import (
    SKILL_TAG_ALLOWED_KEY_PREFIXES,
    SKILL_TAG_INT_KEY_PREFIXES,
    SKILL_TAG_MAX_ITEMS,
    SKILL_TAG_MAX_LEN,
    SKILL_TAG_MODE_VALUES,
)


_KEY_RE = re.compile(r"^[a-z][a-z0-9_]*$")


def _dedupe_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for it in items:
        if it in seen:
            continue
        seen.add(it)
        out.append(it)
    return out


def normalize_skill_tags(
    tags: Any,
    *,
    strict_keys: bool = False,
    max_items: int = SKILL_TAG_MAX_ITEMS,
) -> Tuple[List[str], List[str]]:
    """
    规范化 skills_items.tags（docs/agent：用于检索/溯源）。

    规则（默认偏“宽松治理”，避免误删有效标签）：
    - 去空白、去重复、限制单条长度与总条数；
    - key:value：key 统一小写，校验 key 形态；对 int 类型 key 做 int 归一化；
    - mode: 归一化为 do/think/chat；不合法时丢弃；
    - 未知 key：
      - strict_keys=False：保留但记 issue（方便人工治理）
      - strict_keys=True：丢弃并记 issue

    Returns:
        (normalized_tags, issues)
    """
    issues: List[str] = []

    raw_list: List[Any]
    if tags is None:
        raw_list = []
    elif isinstance(tags, list):
        raw_list = tags
    else:
        raw_list = [tags]

    normalized: List[str] = []

    for raw in raw_list:
        try:
            text = str(raw)
        except Exception:
            issues.append("tag_non_string")
            continue
        text = text.strip()
        if not text:
            continue
        if "\n" in text or "\r" in text:
            issues.append("tag_contains_newline")
            continue
        if len(text) > int(SKILL_TAG_MAX_LEN):
            issues.append("tag_too_long")
            continue

        if ":" not in text:
            # plain tag：统一小写，避免重复（例如 Solution vs solution）
            normalized.append(text.lower())
            continue

        key_raw, value_raw = text.split(":", 1)
        key = str(key_raw or "").strip().lower()
        value = str(value_raw or "").strip()
        if not key or not value:
            issues.append("tag_empty_kv")
            continue
        if not _KEY_RE.match(key):
            issues.append(f"tag_bad_key:{key}")
            continue

        if key not in set(SKILL_TAG_ALLOWED_KEY_PREFIXES):
            issues.append(f"tag_unknown_key:{key}")
            if strict_keys:
                continue

        if key in set(SKILL_TAG_INT_KEY_PREFIXES):
            try:
                iv = int(value)
            except Exception:
                issues.append(f"tag_int_expected:{key}")
                continue
            if iv <= 0:
                issues.append(f"tag_int_non_positive:{key}")
                continue
            normalized.append(f"{key}:{iv}")
            continue

        if key == "mode":
            mode = value.lower()
            if mode not in set(SKILL_TAG_MODE_VALUES):
                issues.append("tag_invalid_mode")
                continue
            normalized.append(f"mode:{mode}")
            continue

        if key == "domain":
            # domain_id 允许 a.b.c / misc / data-clean 之类；不强制 regex，只做最小清洗
            normalized.append(f"domain:{value}")
            continue

        if key == "tool_name":
            # 允许空格/符号：仅做最小 trim
            normalized.append(f"tool_name:{value}")
            continue

        normalized.append(f"{key}:{value}")

    # 限制总条数 + 去重
    normalized = _dedupe_keep_order(normalized)[: max(0, int(max_items))]
    return normalized, issues

