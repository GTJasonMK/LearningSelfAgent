from __future__ import annotations

import sqlite3
import time
from typing import Any, Callable, List, Optional, Sequence, Tuple, TypeVar

T = TypeVar("T")


def normalize_non_empty_texts(values: Sequence[Any]) -> List[str]:
    """
    统一把输入序列转换为“非空字符串列表”。
    """
    items: List[str] = []
    for value in values or []:
        text = str(value or "").strip()
        if text:
            items.append(text)
    return items


def in_clause_placeholders(items: Sequence[Any]) -> Optional[str]:
    """
    根据参数数量生成 SQL IN 子句占位符（如 `?,?,?`）。
    """
    count = len(items or [])
    if count <= 0:
        return None
    return ",".join(["?"] * count)


def is_sqlite_locked_error(exc: BaseException) -> bool:
    """
    判断异常是否属于 SQLite `database is locked` 场景。
    """
    return isinstance(exc, sqlite3.OperationalError) and "locked" in str(exc or "").lower()


def sqlite_retry_sleep_seconds(attempt_index: int, *, base_delay_seconds: float = 0.05) -> float:
    """
    统一 SQLite 锁冲突重试退避时长（线性退避）。
    """
    try:
        index = int(attempt_index)
    except Exception:
        index = 0
    if index < 0:
        index = 0
    try:
        base = float(base_delay_seconds)
    except Exception:
        base = 0.05
    if base <= 0:
        base = 0.05
    return float(base) * float(index + 1)


def run_with_sqlite_locked_retry(
    op: Callable[[], T],
    *,
    attempts: int = 3,
    base_delay_seconds: float = 0.05,
    sleep_func: Callable[[float], None] = time.sleep,
) -> T:
    """
    统一封装 SQLite `locked` 场景的轻量重试。
    """
    try:
        max_attempts = int(attempts)
    except Exception:
        max_attempts = 3
    if max_attempts <= 0:
        max_attempts = 1
    last_exc: Optional[BaseException] = None
    for attempt in range(0, max_attempts):
        try:
            return op()
        except sqlite3.OperationalError as exc:
            last_exc = exc
            if is_sqlite_locked_error(exc) and attempt < max_attempts - 1:
                sleep_func(sqlite_retry_sleep_seconds(attempt, base_delay_seconds=base_delay_seconds))
                continue
            raise
    raise RuntimeError(str(last_exc))


class WhereBuilder:
    """
    轻量 WHERE 构建器，用于减少重复的 conditions/params 拼接代码。

    约束：
    - clause/column 必须由代码写死（不要拼接用户输入），避免 SQL 注入；
    - 参数始终走 sqlite 的 `?` 占位符。
    """

    def __init__(self) -> None:
        self._conditions: List[str] = []
        self._params: List[Any] = []

    def add(self, clause: str, *values: Any) -> "WhereBuilder":
        self._conditions.append(clause)
        if values:
            self._params.extend(values)
        return self

    def eq(self, column: str, value: Any) -> "WhereBuilder":
        if value is None:
            return self
        self._conditions.append(f"{column} = ?")
        self._params.append(value)
        return self

    def build(self) -> Tuple[str, List[Any]]:
        if not self._conditions:
            return "", []
        return f"WHERE {' AND '.join(self._conditions)}", list(self._params)

