from __future__ import annotations

import sqlite3
from typing import List, Optional, Tuple

from backend.src.common.utils import now_iso
from backend.src.repositories.repo_conn import provide_connection


def create_skill_validation(
    *,
    skill_id: int,
    task_id: Optional[int],
    run_id: Optional[int],
    status: str,
    notes: Optional[str],
    created_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[int, str]:
    created = created_at or now_iso()
    sql = (
        "INSERT INTO skill_validation_records (skill_id, task_id, run_id, status, notes, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )
    params = (int(skill_id), task_id, run_id, status, notes, created)
    with provide_connection(conn) as inner:
        cursor = inner.execute(sql, params)
        return int(cursor.lastrowid), created


def list_skill_validations(
    *,
    skill_id: int,
    offset: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    sql = "SELECT * FROM skill_validation_records WHERE skill_id = ? ORDER BY id ASC LIMIT ? OFFSET ?"
    params = (int(skill_id), int(limit), int(offset))
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def get_skill_validation(
    *, record_id: int, conn: Optional[sqlite3.Connection] = None
) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM skill_validation_records WHERE id = ?"
    params = (int(record_id),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()
