from __future__ import annotations

import sqlite3
from typing import Optional, Tuple

from backend.src.common.sql import run_with_sqlite_locked_retry
from backend.src.common.utils import now_iso
from backend.src.repositories.repo_conn import provide_connection


def _list_task_outputs_rows(
    *,
    task_id: int,
    run_id: Optional[int],
    order: str = "ASC",
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> list[sqlite3.Row]:
    direction = "DESC" if str(order or "").strip().upper() == "DESC" else "ASC"
    where = ["task_id = ?"]
    params: list = [int(task_id)]
    if run_id is not None:
        where.append("run_id = ?")
        params.append(int(run_id))
    sql = "SELECT * FROM task_outputs " f"WHERE {' AND '.join(where)} " f"ORDER BY id {direction}"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    if offset is not None:
        sql += " OFFSET ?"
        params.append(int(offset))
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def create_task_output(
    *,
    task_id: int,
    run_id: Optional[int],
    output_type: str,
    content: str,
    created_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[int, str]:
    """
    创建 task_outputs 记录并返回 (output_id, created_at)。
    """
    created = created_at or now_iso()
    sql = "INSERT INTO task_outputs (task_id, run_id, output_type, content, created_at) VALUES (?, ?, ?, ?, ?)"
    params = (
        int(task_id),
        int(run_id) if run_id is not None else None,
        output_type,
        str(content or ""),
        created,
    )

    def _insert_once() -> int:
        with provide_connection(conn) as inner:
            cursor = inner.execute(sql, params)
            return int(cursor.lastrowid)

    # 外部事务连接：不在这里做重试（避免隐藏上层事务语义）。
    if conn is not None:
        return _insert_once(), created
    return run_with_sqlite_locked_retry(_insert_once, attempts=3, base_delay_seconds=0.05), created


def get_task_output(*, output_id: int, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM task_outputs WHERE id = ?"
    params = (int(output_id),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def list_task_outputs(
    *,
    task_id: int,
    run_id: Optional[int],
    offset: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
) -> list[sqlite3.Row]:
    return _list_task_outputs_rows(
        task_id=int(task_id),
        run_id=int(run_id) if run_id is not None else None,
        order="ASC",
        limit=int(limit),
        offset=int(offset),
        conn=conn,
    )


def list_task_outputs_for_task(
    *,
    task_id: int,
    conn: Optional[sqlite3.Connection] = None,
) -> list[sqlite3.Row]:
    """
    按 id 顺序返回某个 task 的全部 outputs（不分页），用于 records 导出/时间线回放。
    """
    return _list_task_outputs_rows(
        task_id=int(task_id),
        run_id=None,
        order="ASC",
        limit=None,
        offset=None,
        conn=conn,
    )


def list_task_outputs_for_run(
    *,
    task_id: int,
    run_id: int,
    limit: Optional[int] = None,
    order: str = "ASC",
    conn: Optional[sqlite3.Connection] = None,
) -> list[sqlite3.Row]:
    """
    按 id 顺序返回某个 run 的 outputs（可选 limit）。
    """
    return _list_task_outputs_rows(
        task_id=int(task_id),
        run_id=int(run_id),
        order=order,
        limit=int(limit) if limit is not None else None,
        offset=None,
        conn=conn,
    )
