from __future__ import annotations

from typing import Any, List, Tuple


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

