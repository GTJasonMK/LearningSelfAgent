from __future__ import annotations

import sqlite3
from typing import Any, List, Optional

from backend.src.repositories.repo_conn import provide_connection


def list_graph_extract_tasks(
    *,
    task_id: Optional[int],
    run_id: Optional[int],
    status: Optional[str],
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    with provide_connection(conn) as inner:
        conditions = []
        params: List[Any] = []
        if task_id is not None:
            conditions.append("task_id = ?")
            params.append(int(task_id))
        if run_id is not None:
            conditions.append("run_id = ?")
            params.append(int(run_id))
        if status is not None:
            conditions.append("status = ?")
            params.append(str(status))
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(int(limit))
        sql = f"SELECT * FROM graph_extract_tasks {where_clause} ORDER BY id DESC LIMIT ?"
        return list(inner.execute(sql, params).fetchall())


def get_graph_extract_task(
    *,
    extract_id: int,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM graph_extract_tasks WHERE id = ?"
    params = (int(extract_id),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()
