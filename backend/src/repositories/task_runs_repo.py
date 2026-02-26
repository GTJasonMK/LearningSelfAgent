from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional, Tuple

from backend.src.common.sql import in_clause_placeholders, normalize_non_empty_texts
from backend.src.common.utils import now_iso, parse_json_dict
from backend.src.constants import (
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_RUNNING,
    RUN_STATUS_STOPPED,
    RUN_STATUS_WAITING,
)
from backend.src.repositories.repo_conn import provide_connection


def _status_items_and_placeholders(statuses: list[str]) -> tuple[list[str], Optional[str]]:
    items = normalize_non_empty_texts(statuses or [])
    return items, in_clause_placeholders(items)


def _stop_task_runs(
    *,
    from_status: str,
    to_status: str,
    stopped_at: str,
    run_id: Optional[int],
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    where_clause = "status = ?"
    params: list[Any] = [str(to_status or ""), str(stopped_at or ""), str(stopped_at or "")]
    if run_id is not None:
        where_clause = "id = ? AND status = ?"
        params.extend([int(run_id), str(from_status or "")])
    else:
        params.append(str(from_status or ""))
    sql = f"UPDATE task_runs SET status = ?, finished_at = ?, updated_at = ? WHERE {where_clause}"
    with provide_connection(conn) as inner:
        inner.execute(sql, params)


def create_task_run(
    *,
    task_id: int,
    status: str,
    summary: Optional[str] = None,
    started_at: Optional[str] = None,
    finished_at: Optional[str] = None,
    created_at: Optional[str] = None,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[int, str, str]:
    """
    创建 task_runs 记录并返回 (run_id, created_at, updated_at)。
    """
    created = created_at or now_iso()
    updated = updated_at or created
    with provide_connection(conn) as inner:
        cursor = inner.execute(
            "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (task_id, status, summary, started_at, finished_at, created, updated),
        )
        run_id = int(cursor.lastrowid)
    return run_id, created, updated


def get_task_run(*, run_id: int, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM task_runs WHERE id = ?"
    params = (int(run_id),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def count_task_runs_by_status(*, status: str, conn: Optional[sqlite3.Connection] = None) -> int:
    sql = "SELECT COUNT(*) AS count FROM task_runs WHERE status = ?"
    params = (str(status or ""),)
    with provide_connection(conn) as inner:
        row = inner.execute(sql, params).fetchone()
    return int(row["count"]) if row else 0


def stop_all_running_task_runs(
    *,
    from_status: str,
    to_status: str,
    stopped_at: str,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """
    批量把 task_runs 从 running 类状态收敛到 stopped（本次尝试结束，因此写 finished_at）。
    """
    _stop_task_runs(
        from_status=from_status,
        to_status=to_status,
        stopped_at=stopped_at,
        run_id=None,
        conn=conn,
    )


def stop_task_run_if_running(
    *,
    run_id: int,
    from_status: str,
    to_status: str,
    stopped_at: str,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    _stop_task_runs(
        from_status=from_status,
        to_status=to_status,
        stopped_at=stopped_at,
        run_id=int(run_id),
        conn=conn,
    )


def list_task_runs(
    *,
    task_id: int,
    offset: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
) -> list[sqlite3.Row]:
    sql = "SELECT * FROM task_runs WHERE task_id = ? ORDER BY id ASC LIMIT ? OFFSET ?"
    params = (int(task_id), int(limit), int(offset))
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def list_task_runs_for_task(
    *,
    task_id: int,
    conn: Optional[sqlite3.Connection] = None,
) -> list[sqlite3.Row]:
    """
    按 id 顺序返回某个 task 的全部 runs（不分页），用于 records 导出/时间线回放。
    """
    sql = "SELECT * FROM task_runs WHERE task_id = ? ORDER BY id ASC"
    params = (int(task_id),)
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def list_agent_runs_missing_reviews(
    *,
    statuses: list[str],
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
) -> list[sqlite3.Row]:
    """
    列出“Agent run 且没有评估记录”的 runs（用于启动时补齐/兜底）。

    说明：
    - 仅筛 summary LIKE 'agent_%' 的 run；
    - 使用 NOT EXISTS 避免 N+1 查询；
    - order by id desc：优先补齐最近的 runs。
    """
    items, placeholders = _status_items_and_placeholders(statuses)
    if not items or not placeholders:
        return []
    if int(limit) <= 0:
        return []

    sql = (
        "SELECT r.* "
        "FROM task_runs r "
        "WHERE r.summary LIKE 'agent_%' "
        f"AND r.status IN ({placeholders}) "
        "AND NOT EXISTS (SELECT 1 FROM agent_review_records a WHERE a.run_id = r.id) "
        "ORDER BY r.id DESC "
        "LIMIT ?"
    )
    params = (*items, int(limit))
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def update_task_run(
    *,
    run_id: int,
    status: Optional[str] = None,
    summary: Optional[str] = None,
    agent_plan: Optional[Any] = None,
    agent_state: Optional[Any] = None,
    clear_finished_at: bool = False,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[sqlite3.Row]:
    """
    更新 task_runs 指定字段，并根据 status 自动补齐 started_at/finished_at。
    """
    updated = updated_at or now_iso()

    def _json_value(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return json.dumps({"value": str(value)}, ensure_ascii=False)

    with provide_connection(conn) as inner:
        row = get_task_run(run_id=run_id, conn=inner)
        if not row:
            return None

        fields = []
        params = []

        if status is not None:
            fields.append("status = ?")
            params.append(status)

            # status 相关字段需要基于“旧 row”判断（是否补 started_at/finished_at）
            if status in {RUN_STATUS_RUNNING, RUN_STATUS_WAITING} and row["started_at"] is None:
                fields.append("started_at = ?")
                params.append(updated)
            if status in {RUN_STATUS_DONE, RUN_STATUS_FAILED, RUN_STATUS_STOPPED}:
                fields.append("finished_at = ?")
                params.append(updated)

        if summary is not None:
            fields.append("summary = ?")
            params.append(summary)
        if agent_plan is not None:
            fields.append("agent_plan = ?")
            params.append(_json_value(agent_plan))
        if agent_state is not None:
            fields.append("agent_state = ?")
            params.append(_json_value(agent_state))

        # docs/agent：task_runs 需要持久化 mode（do/think）。
        # 约定：mode 以 agent_state["mode"] 为准（不重复引入上层传参）。
        mode_value: Optional[str] = None
        if agent_state is not None:
            if isinstance(agent_state, dict):
                mode_value = str(agent_state.get("mode") or "").strip() or None
            elif isinstance(agent_state, str):
                parsed = parse_json_dict(agent_state)
                if parsed:
                    mode_value = str(parsed.get("mode") or "").strip() or None

        fields_without_mode = list(fields)
        params_without_mode = list(params)
        if mode_value:
            fields.append("mode = ?")
            params.append(str(mode_value))

        if clear_finished_at and (status is None or status in {RUN_STATUS_RUNNING, RUN_STATUS_WAITING}):
            # 继续执行 stopped run 时需要清空 finished_at，否则 UI 会误判为“已结束”
            fields.append("finished_at = NULL")

        if fields:
            def _exec_update(update_fields: list[str], update_params: list[Any]) -> None:
                update_fields = list(update_fields)
                update_params = list(update_params)
                update_fields.append("updated_at = ?")
                update_params.append(updated)
                update_params.append(int(run_id))
                inner.execute(
                    f"UPDATE task_runs SET {', '.join(update_fields)} WHERE id = ?",
                    update_params,
                )

            try:
                _exec_update(fields, params)
            except sqlite3.OperationalError as exc:
                # 兼容旧库：mode 列可能尚未迁移完成
                if mode_value and "no such column: mode" in str(exc or ""):
                    _exec_update(fields_without_mode, params_without_mode)
                else:
                    raise

        # 无论是否更新字段，都返回最新 row（方便调用方直接使用）
        return get_task_run(run_id=run_id, conn=inner)


def fetch_agent_run_with_task_title_by_statuses(
    *,
    statuses: list[str],
    limit: int = 1,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[sqlite3.Row]:
    """
    获取最近一条 Agent run（summary LIKE 'agent_%'）并附带 tasks.title。
    """
    items, placeholders = _status_items_and_placeholders(statuses)
    if not items or not placeholders:
        return None
    sql = (
        "SELECT r.*, t.title AS task_title "
        "FROM task_runs r "
        "JOIN tasks t ON t.id = r.task_id "
        "WHERE (r.summary LIKE 'agent_%') "
        f"AND r.status IN ({placeholders}) "
        "ORDER BY r.id DESC "
        "LIMIT ?"
    )
    params = items + [int(limit)]
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def fetch_latest_agent_run_with_task_title(
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[sqlite3.Row]:
    """
    获取最近一条 Agent run（不限定 status），并附带 tasks.title。
    """
    sql = (
        "SELECT r.*, t.title AS task_title "
        "FROM task_runs r "
        "JOIN tasks t ON t.id = r.task_id "
        "WHERE (r.summary LIKE 'agent_%') "
        "ORDER BY r.id DESC "
        "LIMIT 1"
    )
    with provide_connection(conn) as inner:
        return inner.execute(sql).fetchone()


def get_task_run_with_task_title(
    *,
    run_id: int,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[sqlite3.Row]:
    sql = (
        "SELECT r.*, t.title AS task_title "
        "FROM task_runs r "
        "JOIN tasks t ON t.id = r.task_id "
        "WHERE r.id = ?"
    )
    params = (int(run_id),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()
