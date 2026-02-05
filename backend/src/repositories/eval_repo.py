from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional, Sequence, Tuple

from backend.src.common.utils import now_iso
from backend.src.repositories.repo_conn import provide_connection


def create_eval_record(
    *,
    status: str,
    score: Optional[float],
    notes: Optional[str],
    task_id: Optional[int],
    expectation_id: Optional[int],
    created_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[int, str]:
    created = created_at or now_iso()
    sql = "INSERT INTO eval_records (status, score, notes, task_id, expectation_id, created_at) VALUES (?, ?, ?, ?, ?, ?)"
    params = (status, score, notes, task_id, expectation_id, created)
    with provide_connection(conn) as inner:
        cursor = inner.execute(sql, params)
        return int(cursor.lastrowid), created


def get_eval_record(*, eval_id: int, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM eval_records WHERE id = ?"
    params = (int(eval_id),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def list_eval_records_by_task(
    *, task_id: int, conn: Optional[sqlite3.Connection] = None
) -> List[sqlite3.Row]:
    sql = "SELECT * FROM eval_records WHERE task_id = ? ORDER BY id ASC"
    params = (int(task_id),)
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def create_eval_criterion_record(
    *,
    eval_id: int,
    criterion: str,
    status: str,
    notes: Optional[str],
    created_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[int, str]:
    created = created_at or now_iso()
    sql = "INSERT INTO eval_criteria_records (eval_id, criterion, status, notes, created_at) VALUES (?, ?, ?, ?, ?)"
    params = (int(eval_id), criterion, status, notes, created)
    with provide_connection(conn) as inner:
        cursor = inner.execute(sql, params)
        return int(cursor.lastrowid), created


def create_eval_criteria_bulk(
    *,
    eval_id: int,
    items: Sequence[Dict[str, Any]],
    created_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> str:
    """
    批量写入 eval_criteria_records（用于自动评估）。
    """
    created = created_at or now_iso()
    if not items:
        return created

    rows = []
    for item in items:
        criterion = str(item.get("criterion") or "").strip()
        status = str(item.get("status") or "").strip()
        if not criterion or not status:
            continue
        rows.append((int(eval_id), criterion, status, item.get("notes"), created))

    if not rows:
        return created

    sql = "INSERT INTO eval_criteria_records (eval_id, criterion, status, notes, created_at) VALUES (?, ?, ?, ?, ?)"
    with provide_connection(conn) as inner:
        inner.executemany(sql, rows)
        return created


def list_eval_criteria_by_eval_id(
    *,
    eval_id: int,
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    sql = "SELECT * FROM eval_criteria_records WHERE eval_id = ? ORDER BY id ASC"
    params = (int(eval_id),)
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def list_eval_criteria_by_eval_ids(
    *,
    eval_ids: Sequence[int],
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    ids = [int(i) for i in (eval_ids or []) if i is not None]
    if not ids:
        return []
    placeholders = ",".join(["?"] * len(ids))
    sql = f"SELECT * FROM eval_criteria_records WHERE eval_id IN ({placeholders}) ORDER BY id ASC"
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, ids).fetchall())


def get_eval_latest_summary(*, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    sql = "SELECT status, score, notes FROM eval_records ORDER BY id DESC LIMIT 1"
    with provide_connection(conn) as inner:
        return inner.execute(sql).fetchone()
