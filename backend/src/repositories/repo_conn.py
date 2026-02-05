from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator, Optional

from backend.src.storage import get_connection


@contextmanager
def provide_connection(conn: Optional[sqlite3.Connection] = None) -> Iterator[sqlite3.Connection]:
    """
    统一仓储层连接管理：
    - conn 为空：自动打开/提交/关闭（通过 storage.get_connection）
    - conn 非空：复用外部事务连接（不在这里提交/关闭）
    """
    if conn is None:
        with get_connection() as inner:
            yield inner
        return
    yield conn

