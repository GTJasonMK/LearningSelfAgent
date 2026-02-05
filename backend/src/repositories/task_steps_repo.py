from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Optional, Tuple

from backend.src.common.utils import now_iso
from backend.src.constants import (
    STEP_STATUS_DONE,
    STEP_STATUS_FAILED,
    STEP_STATUS_PLANNED,
    STEP_STATUS_RUNNING,
    STEP_STATUS_SKIPPED,
)
from backend.src.repositories.repo_conn import provide_connection


@dataclass(frozen=True)
class TaskStepCreateParams:
    """
    task_steps 创建参数（避免 10+ 参数的长签名）。
    """

    task_id: int
    run_id: Optional[int]
    title: str
    status: str
    detail: Optional[str] = None
    result: Optional[str] = None
    error: Optional[str] = None
    attempts: Optional[int] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    step_order: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    executor: Optional[str] = None


def create_task_step(
    params: TaskStepCreateParams,
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[int, str, str]:
    """
    创建 task_steps 记录并返回 (step_id, created_at, updated_at)。

    说明：
    - Repository 层只负责“落库与字段约束”，不承载业务决策；
    - 支持传入外部 conn：用于上层把 tasks/run/steps 放进同一事务。
    """
    created = params.created_at or now_iso()
    updated = params.updated_at or created

    sql = (
        "INSERT INTO task_steps "
        "(task_id, run_id, title, status, executor, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    sql_params = (
        int(params.task_id),
        int(params.run_id) if params.run_id is not None else None,
        params.title,
        params.status,
        str(params.executor) if params.executor is not None else None,
        params.detail,
        params.result,
        params.error,
        int(params.attempts) if params.attempts is not None else None,
        params.started_at,
        params.finished_at,
        int(params.step_order) if params.step_order is not None else None,
        created,
        updated,
    )

    legacy_sql = (
        "INSERT INTO task_steps "
        "(task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    legacy_params = (
        int(params.task_id),
        int(params.run_id) if params.run_id is not None else None,
        params.title,
        params.status,
        params.detail,
        params.result,
        params.error,
        int(params.attempts) if params.attempts is not None else None,
        params.started_at,
        params.finished_at,
        int(params.step_order) if params.step_order is not None else None,
        created,
        updated,
    )

    with provide_connection(conn) as inner:
        try:
            cursor = inner.execute(sql, sql_params)
        except sqlite3.OperationalError as exc:
            # 兼容旧库：executor 列可能尚未迁移完成
            msg = str(exc or "")
            if "no column named executor" in msg:
                cursor = inner.execute(legacy_sql, legacy_params)
            else:
                raise
        step_id = int(cursor.lastrowid)
    return step_id, created, updated


def get_task_step(*, step_id: int, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM task_steps WHERE id = ?"
    params = (int(step_id),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def count_task_steps_by_status(*, status: str, conn: Optional[sqlite3.Connection] = None) -> int:
    sql = "SELECT COUNT(*) AS count FROM task_steps WHERE status = ?"
    params = (str(status or ""),)
    with provide_connection(conn) as inner:
        row = inner.execute(sql, params).fetchone()
    return int(row["count"]) if row else 0


def count_task_steps_running_for_run(
    *,
    task_id: int,
    run_id: int,
    running_status: str,
    conn: Optional[sqlite3.Connection] = None,
) -> int:
    sql = "SELECT COUNT(*) AS count FROM task_steps WHERE task_id = ? AND run_id = ? AND status = ?"
    params = (int(task_id), int(run_id), str(running_status or ""))
    with provide_connection(conn) as inner:
        row = inner.execute(sql, params).fetchone()
    return int(row["count"]) if row else 0


def get_max_step_order_for_run_by_status(
    *,
    task_id: int,
    run_id: int,
    status: str,
    conn: Optional[sqlite3.Connection] = None,
) -> int:
    sql = "SELECT MAX(step_order) AS max_order FROM task_steps WHERE task_id = ? AND run_id = ? AND status = ?"
    params = (int(task_id), int(run_id), str(status or ""))
    with provide_connection(conn) as inner:
        row = inner.execute(sql, params).fetchone()
    return int(row["max_order"] or 0) if row else 0


def get_last_non_planned_step_for_run(
    *,
    task_id: int,
    run_id: int,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[sqlite3.Row]:
    """
    找到某个 run 中“最后一个 status != planned 的步骤”（按 step_order/id 排序）。

    设计目的（与 docs/agent 对齐）：
    - resume 断点定位应以 task_steps 为准，而不是仅依赖 agent_state.step_order；
    - 兼容异常退出/stop-running 将 running/waiting 回退为 planned 的场景。
    """
    sql = (
        "SELECT * FROM task_steps "
        "WHERE task_id = ? AND run_id = ? AND step_order IS NOT NULL AND status != ? "
        "ORDER BY step_order DESC, id DESC "
        "LIMIT 1"
    )
    params = (int(task_id), int(run_id), STEP_STATUS_PLANNED)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def reset_all_running_steps_to_planned(
    *,
    from_status: str,
    to_status: str,
    updated_at: str,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    sql = "UPDATE task_steps SET status = ?, updated_at = ? WHERE status = ?"
    params = (str(to_status or ""), str(updated_at or ""), str(from_status or ""))
    with provide_connection(conn) as inner:
        inner.execute(sql, params)


def reset_running_steps_to_planned_for_run(
    *,
    task_id: int,
    run_id: int,
    from_status: str,
    to_status: str,
    updated_at: str,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    sql = "UPDATE task_steps SET status = ?, updated_at = ? WHERE task_id = ? AND run_id = ? AND status = ?"
    params = (
        str(to_status or ""),
        str(updated_at or ""),
        int(task_id),
        int(run_id),
        str(from_status or ""),
    )
    with provide_connection(conn) as inner:
        inner.execute(sql, params)


def list_task_steps(
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
        "SELECT * FROM task_steps "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY step_order IS NULL, step_order ASC, id ASC "
        "LIMIT ? OFFSET ?"
    )
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def list_task_steps_for_task(
    *,
    task_id: int,
    conn: Optional[sqlite3.Connection] = None,
) -> list[sqlite3.Row]:
    """
    按 step_order/id 顺序返回某个 task 的全部步骤（不分页），用于执行链路。
    """
    sql = "SELECT * FROM task_steps WHERE task_id = ? ORDER BY step_order IS NULL, step_order ASC, id ASC"
    params = (int(task_id),)
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def list_task_steps_for_run(
    *,
    task_id: int,
    run_id: int,
    conn: Optional[sqlite3.Connection] = None,
) -> list[sqlite3.Row]:
    """
    按 step_order/id 顺序返回某个 task/run 的全部步骤（不分页），用于后处理/回放。
    """
    sql = (
        "SELECT * FROM task_steps "
        "WHERE task_id = ? AND run_id = ? "
        "ORDER BY step_order IS NULL, step_order ASC, id ASC"
    )
    params = (int(task_id), int(run_id))
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def update_task_step(
    *,
    step_id: int,
    title: Optional[str] = None,
    status: Optional[str] = None,
    detail: Optional[str] = None,
    result: Optional[str] = None,
    error: Optional[str] = None,
    step_order: Optional[int] = None,
    run_id: Optional[int] = None,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[sqlite3.Row]:
    """
    通用更新：字段为 None 表示不修改；返回更新后的行。
    """
    fields = []
    params = []
    if title is not None:
        fields.append("title = ?")
        params.append(title)
    if status is not None:
        fields.append("status = ?")
        params.append(status)
    if detail is not None:
        fields.append("detail = ?")
        params.append(detail)
    if result is not None:
        fields.append("result = ?")
        params.append(result)
    if error is not None:
        fields.append("error = ?")
        params.append(error)
    if step_order is not None:
        fields.append("step_order = ?")
        params.append(int(step_order))
    if run_id is not None:
        fields.append("run_id = ?")
        params.append(int(run_id))

    updated = updated_at or now_iso()

    with provide_connection(conn) as inner:
        row = get_task_step(step_id=step_id, conn=inner)
        if not row:
            return None
        if fields:
            fields.append("updated_at = ?")
            params.append(updated)
            params.append(int(step_id))
            inner.execute(
                f"UPDATE task_steps SET {', '.join(fields)} WHERE id = ?",
                params,
            )
        return get_task_step(step_id=step_id, conn=inner)


def mark_task_step_running(
    *,
    step_id: int,
    run_id: int,
    attempts: int,
    started_at: str,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> str:
    """
    标记步骤为 running，并更新 attempts/run_id/started_at/updated_at。
    """
    updated = updated_at or now_iso()
    sql = (
        "UPDATE task_steps "
        "SET status = ?, run_id = ?, attempts = ?, started_at = COALESCE(started_at, ?), updated_at = ? "
        "WHERE id = ?"
    )
    params = (STEP_STATUS_RUNNING, int(run_id), int(attempts), started_at, updated, int(step_id))
    with provide_connection(conn) as inner:
        inner.execute(sql, params)
        return updated


def mark_task_step_done(
    *,
    step_id: int,
    result: Optional[str],
    finished_at: str,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> str:
    """
    标记步骤为 done，并写入 result/finished_at/updated_at。
    """
    updated = updated_at or finished_at or now_iso()
    sql = "UPDATE task_steps SET status = ?, result = ?, finished_at = ?, updated_at = ? WHERE id = ?"
    params = (STEP_STATUS_DONE, result, finished_at, updated, int(step_id))
    with provide_connection(conn) as inner:
        inner.execute(sql, params)
        return updated


def mark_task_step_failed(
    *,
    step_id: int,
    error: str,
    finished_at: str,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> str:
    """
    标记步骤为 failed，并写入 error/finished_at/updated_at。
    """
    updated = updated_at or finished_at or now_iso()
    sql = "UPDATE task_steps SET status = ?, error = ?, finished_at = ?, updated_at = ? WHERE id = ?"
    params = (STEP_STATUS_FAILED, error, finished_at, updated, int(step_id))
    with provide_connection(conn) as inner:
        inner.execute(sql, params)
        return updated


def mark_task_step_skipped(
    *,
    step_id: int,
    error: Optional[str],
    finished_at: str,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> str:
    """
    标记步骤为 skipped（用于 on_failure=skip），并写入 error/finished_at/updated_at。
    """
    updated = updated_at or finished_at or now_iso()
    sql = "UPDATE task_steps SET status = ?, error = ?, finished_at = ?, updated_at = ? WHERE id = ?"
    params = (STEP_STATUS_SKIPPED, error, finished_at, updated, int(step_id))
    with provide_connection(conn) as inner:
        inner.execute(sql, params)
        return updated
