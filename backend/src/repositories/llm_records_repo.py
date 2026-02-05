from __future__ import annotations

import sqlite3
from typing import List, Optional, Sequence

from backend.src.common.utils import now_iso
from backend.src.repositories.repo_conn import provide_connection


def list_llm_records(
    *,
    task_id: Optional[int],
    run_id: Optional[int],
    offset: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    conditions: List[str] = []
    params: List = []
    if task_id is not None:
        conditions.append("task_id = ?")
        params.append(int(task_id))
    if run_id is not None:
        conditions.append("run_id = ?")
        params.append(int(run_id))
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.extend([int(limit), int(offset)])

    sql = f"SELECT * FROM llm_records {where_clause} ORDER BY id ASC LIMIT ? OFFSET ?"
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def list_llm_records_for_task(
    *,
    task_id: int,
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    sql = "SELECT * FROM llm_records WHERE task_id = ? ORDER BY id ASC"
    params = (int(task_id),)
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def get_llm_record(*, record_id: int, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM llm_records WHERE id = ?"
    params = (int(record_id),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def create_llm_record(
    *,
    prompt: str,
    response: str,
    task_id: Optional[int],
    run_id: Optional[int],
    status: str,
    created_at: Optional[str] = None,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> int:
    created = created_at or now_iso()
    updated = updated_at or created
    sql = (
        "INSERT INTO llm_records (prompt, response, task_id, run_id, status, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    params: Sequence = (prompt, response, task_id, run_id, status, created, updated)
    with provide_connection(conn) as inner:
        cursor = inner.execute(sql, params)
        return int(cursor.lastrowid)
