from __future__ import annotations

import json
import sqlite3
from typing import List, Optional, Sequence, Tuple

from backend.src.common.utils import now_iso
from backend.src.repositories.repo_conn import provide_connection


def create_expectation(
    *,
    goal: str,
    criteria: Sequence[str],
    created_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[int, str]:
    created = created_at or now_iso()
    criteria_json = json.dumps(list(criteria or []), ensure_ascii=False)
    sql = "INSERT INTO expectations (goal, criteria, created_at) VALUES (?, ?, ?)"
    params = (goal, criteria_json, created)
    with provide_connection(conn) as inner:
        cursor = inner.execute(sql, params)
        return int(cursor.lastrowid), created


def get_expectation(
    *, expectation_id: int, conn: Optional[sqlite3.Connection] = None
) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM expectations WHERE id = ?"
    params = (int(expectation_id),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def list_expectations(
    *, offset: int, limit: int, conn: Optional[sqlite3.Connection] = None
) -> List[sqlite3.Row]:
    sql = "SELECT * FROM expectations ORDER BY id ASC LIMIT ? OFFSET ?"
    params = (int(limit), int(offset))
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())
