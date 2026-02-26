from __future__ import annotations

import sqlite3
from typing import Optional, Sequence, Tuple

from backend.src.common.sql import in_clause_placeholders, normalize_non_empty_texts
from backend.src.common.utils import now_iso
from backend.src.constants import (
    STATUS_CANCELLED,
    STATUS_DONE,
    STATUS_FAILED,
    STATUS_RUNNING,
    STATUS_WAITING,
)
from backend.src.repositories.repo_conn import provide_connection


def _stop_tasks(
    *,
    from_status: str,
    to_status: str,
    task_id: Optional[int],
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    where_clause = "status = ?"
    params: list[object] = [str(to_status or "")]
    if task_id is not None:
        where_clause = "id = ? AND status = ?"
        params.extend([int(task_id), str(from_status or "")])
    else:
        params.append(str(from_status or ""))
    sql = f"UPDATE tasks SET status = ?, finished_at = NULL WHERE {where_clause}"
    with provide_connection(conn) as inner:
        inner.execute(sql, params)


def create_task(
    *,
    title: str,
    status: str,
    expectation_id: Optional[int] = None,
    started_at: Optional[str] = None,
    finished_at: Optional[str] = None,
    created_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[int, str]:
    """
    创建 tasks 记录并返回 (task_id, created_at)。
    """
    created = created_at or now_iso()
    with provide_connection(conn) as inner:
        cursor = inner.execute(
            "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
            (title, status, created, expectation_id, started_at, finished_at),
        )
        task_id = int(cursor.lastrowid)
    return task_id, created


def get_task(*, task_id: int, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM tasks WHERE id = ?"
    params = (int(task_id),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def task_exists(*, task_id: int, conn: Optional[sqlite3.Connection] = None) -> bool:
    row = get_task(task_id=task_id, conn=conn)
    return bool(row and row["id"])


def count_tasks(*, conn: Optional[sqlite3.Connection] = None) -> int:
    sql = "SELECT COUNT(*) AS count FROM tasks"
    with provide_connection(conn) as inner:
        row = inner.execute(sql).fetchone()
    return int(row["count"]) if row else 0


def count_tasks_by_status(*, status: str, conn: Optional[sqlite3.Connection] = None) -> int:
    sql = "SELECT COUNT(*) AS count FROM tasks WHERE status = ?"
    params = (str(status or ""),)
    with provide_connection(conn) as inner:
        row = inner.execute(sql, params).fetchone()
    return int(row["count"]) if row else 0


def stop_all_running_tasks(
    *,
    from_status: str,
    to_status: str,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """
    批量把 tasks 从 running 类状态收敛到 stopped（任务未完成，不写 finished_at）。
    """
    _stop_tasks(
        from_status=from_status,
        to_status=to_status,
        task_id=None,
        conn=conn,
    )


def stop_task_if_running(
    *,
    task_id: int,
    from_status: str,
    to_status: str,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    _stop_tasks(
        from_status=from_status,
        to_status=to_status,
        task_id=int(task_id),
        conn=conn,
    )


def fetch_current_task_title_by_run_statuses(
    *,
    statuses: Sequence[str],
    limit: int = 1,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[str]:
    """
    以 task_runs.status 为准判断“当前任务”，避免 tasks.status 历史脏数据导致误判。
    """
    items = normalize_non_empty_texts(statuses or [])
    if not items:
        return None
    placeholders = in_clause_placeholders(items)
    if not placeholders:
        return None
    sql = (
        "SELECT t.title AS title "
        "FROM task_runs r "
        "JOIN tasks t ON t.id = r.task_id "
        f"WHERE r.status IN ({placeholders}) "
        "ORDER BY r.id DESC "
        "LIMIT ?"
    )
    params = list(items) + [int(limit)]
    with provide_connection(conn) as inner:
        row = inner.execute(sql, params).fetchone()
    return str(row["title"]) if row and row["title"] is not None else None


def list_tasks(
    *,
    start_created_at: Optional[str],
    end_created_at: Optional[str],
    conn: Optional[sqlite3.Connection] = None,
) -> list[sqlite3.Row]:
    where = []
    params = []
    if start_created_at and end_created_at:
        where.append("created_at >= ? AND created_at < ?")
        params.extend([start_created_at, end_created_at])

    sql = "SELECT * FROM tasks"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC, id DESC"

    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def update_task(
    *,
    task_id: int,
    status: Optional[str] = None,
    title: Optional[str] = None,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[sqlite3.Row]:
    """
    更新 tasks（字段为 None 表示不修改），并根据 status 自动补齐 started_at/finished_at。
    """
    updated = updated_at or now_iso()

    with provide_connection(conn) as inner:
        row = get_task(task_id=task_id, conn=inner)
        if not row:
            return None

        fields = []
        params = []
        if status is not None:
            fields.append("status = ?")
            params.append(status)
            # started_at 仅在首次进入 running/waiting 时写入
            if status in {STATUS_RUNNING, STATUS_WAITING} and row["started_at"] is None:
                fields.append("started_at = ?")
                params.append(updated)
            # finished_at 在任务终态写入
            if status in {STATUS_DONE, STATUS_CANCELLED, STATUS_FAILED}:
                fields.append("finished_at = ?")
                params.append(updated)
        if title is not None:
            fields.append("title = ?")
            params.append(title)

        if fields:
            params.append(int(task_id))
            inner.execute(
                f"UPDATE tasks SET {', '.join(fields)} WHERE id = ?",
                params,
            )

        return get_task(task_id=task_id, conn=inner)
