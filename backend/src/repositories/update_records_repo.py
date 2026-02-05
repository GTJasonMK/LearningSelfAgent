from __future__ import annotations

import sqlite3
from typing import List, Optional, Sequence, Tuple

from backend.src.common.utils import now_iso
from backend.src.repositories.repo_conn import provide_connection


def create_update_record(
    *,
    status: str,
    notes: Optional[str],
    created_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[int, str]:
    created = created_at or now_iso()
    sql = "INSERT INTO update_records (status, notes, created_at) VALUES (?, ?, ?)"
    params: Sequence = (status, notes, created)
    with provide_connection(conn) as inner:
        cursor = inner.execute(sql, params)
        return int(cursor.lastrowid), created


def update_update_record(
    *,
    record_id: int,
    status: str,
    notes: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    fields = ["status = ?"]
    params: list = [str(status or "")]
    if notes is not None:
        fields.append("notes = ?")
        params.append(notes)
    params.append(int(record_id))
    sql = f"UPDATE update_records SET {', '.join(fields)} WHERE id = ?"
    with provide_connection(conn) as inner:
        inner.execute(sql, params)


def list_update_records(
    *,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    sql = "SELECT * FROM update_records ORDER BY id DESC LIMIT ?"
    params = (int(limit),)
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())
