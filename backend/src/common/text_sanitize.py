# -*- coding: utf-8 -*-
"""轻量文本清洗辅助。"""

from __future__ import annotations

import re

_ILLUSTRATIVE_PAREN_RE = re.compile(
    r"[（(][^()（）]{0,120}(?:例如|比如|譬如|e\.g\.?|eg\b|for example|such as)[^()（）]{0,120}[)）]",
    re.IGNORECASE,
)
_ILLUSTRATIVE_INLINE_RE = re.compile(
    r"(?:，|,|:|：|\s)(?:例如|比如|譬如|e\.g\.?|eg\b|for example|such as)\s*[^，。；;,.()（）]{1,80}",
    re.IGNORECASE,
)


def strip_illustrative_example_clauses(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    previous = None
    current = text
    while previous != current:
        previous = current
        current = _ILLUSTRATIVE_PAREN_RE.sub("", current)
        current = _ILLUSTRATIVE_INLINE_RE.sub("", current)
        current = re.sub(r"\s+", " ", current).strip()
        current = re.sub(r"[（(]\s*[)）]", "", current).strip()
        current = re.sub(r"\s+([,，。；;])", r"\1", current).strip()
    return current


def contains_illustrative_example_clause(value: str) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    lowered = raw.lower()
    if _ILLUSTRATIVE_PAREN_RE.search(raw) or _ILLUSTRATIVE_INLINE_RE.search(raw):
        return True
    return any(token in lowered for token in ("例如", "比如", "譬如", "e.g.", "for example", "such as"))
