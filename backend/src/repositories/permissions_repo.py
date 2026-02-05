from __future__ import annotations

import sqlite3
from typing import Optional

from backend.src.constants import SINGLETON_ROW_ID
from backend.src.repositories.repo_conn import provide_connection


def get_permissions_store(*, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    sql = "SELECT allowed_paths, allowed_ops, disabled_actions, disabled_tools FROM permissions_store WHERE id = ?"
    params = (int(SINGLETON_ROW_ID),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()
