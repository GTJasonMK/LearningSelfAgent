from __future__ import annotations

import json
import sqlite3
from typing import List, Optional, Sequence

from backend.src.common.utils import now_iso
from backend.src.repositories.repo_conn import provide_connection


def list_search_records(*, conn: Optional[sqlite3.Connection] = None) -> List[sqlite3.Row]:
    sql = "SELECT * FROM search_records ORDER BY id ASC"
    with provide_connection(conn) as inner:
        return list(inner.execute(sql).fetchall())


def get_search_record(*, record_id: int, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM search_records WHERE id = ?"
    params = (int(record_id),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def create_search_record(
    *,
    query: str,
    sources: Sequence[str],
    result_count: int,
    task_id: Optional[int],
    created_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> int:
    created = created_at or now_iso()
    sql = "INSERT INTO search_records (query, sources, result_count, task_id, created_at) VALUES (?, ?, ?, ?, ?)"
    params: Sequence = (query, json.dumps(list(sources or []), ensure_ascii=False), int(result_count), task_id, created)
    with provide_connection(conn) as inner:
        cursor = inner.execute(sql, params)
        return int(cursor.lastrowid)
