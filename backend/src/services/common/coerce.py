from __future__ import annotations

from typing import Any, Optional, Sequence


def to_int(value: Any) -> int:
    return int(value)


def to_optional_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    return int(value)


def to_int_or_default(value: Any, *, default: int) -> int:
    return int(value or default)


def to_text(value: Any) -> str:
    return str(value or "")


def to_optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def to_non_empty_optional_text(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def to_non_empty_texts(values: Sequence[Any]) -> list[str]:
    out: list[str] = []
    for value in values or []:
        text = str(value or "").strip()
        if text:
            out.append(text)
    return out


def to_int_list(values: Sequence[Any], *, ignore_errors: bool = False) -> list[int]:
    out: list[int] = []
    for value in values or []:
        if value is None:
            continue
        if ignore_errors:
            try:
                out.append(int(value))
            except Exception:
                continue
        else:
            out.append(int(value))
    return out
