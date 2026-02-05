from __future__ import annotations

import sqlite3
import time
from typing import Optional, Tuple

from backend.src.common.utils import now_iso
from backend.src.repositories.repo_conn import provide_connection


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

    last_exc: Optional[BaseException] = None
    for attempt in range(0, 3):
        try:
            with provide_connection(conn) as inner:
                cursor = inner.execute(sql, params)
                output_id = int(cursor.lastrowid)
            return output_id, created
        except sqlite3.OperationalError as exc:
            last_exc = exc
            # 外部事务连接：不在这里做重试（避免隐藏上层事务语义）。
            if conn is not None:
                raise
            if "locked" in str(exc or "").lower() and attempt < 2:
                time.sleep(0.05 * (attempt + 1))
                continue
            raise
        except BaseException as exc:  # noqa: BLE001
            last_exc = exc
            raise
    raise RuntimeError(str(last_exc))


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
    where = ["task_id = ?"]
    params = [int(task_id)]
    if run_id is not None:
        where.append("run_id = ?")
        params.append(int(run_id))
    params.extend([int(limit), int(offset)])
    sql = (
        "SELECT * FROM task_outputs "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY id ASC "
        "LIMIT ? OFFSET ?"
    )
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def list_task_outputs_for_task(
    *,
    task_id: int,
    conn: Optional[sqlite3.Connection] = None,
) -> list[sqlite3.Row]:
    """
    按 id 顺序返回某个 task 的全部 outputs（不分页），用于 records 导出/时间线回放。
    """
    sql = "SELECT * FROM task_outputs WHERE task_id = ? ORDER BY id ASC"
    params = (int(task_id),)
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


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
    direction = "DESC" if str(order or "").strip().upper() == "DESC" else "ASC"
    sql = (
        "SELECT * FROM task_outputs "
        "WHERE task_id = ? AND run_id = ? "
        f"ORDER BY id {direction}"
    )
    params: list = [int(task_id), int(run_id)]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))

    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())
