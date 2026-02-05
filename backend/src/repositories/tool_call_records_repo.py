from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from backend.src.common.utils import now_iso
from backend.src.repositories.repo_conn import provide_connection


@dataclass(frozen=True)
class ToolCallRecordCreateParams:
    """
    tool_call_records 创建参数。
    """

    tool_id: int
    task_id: Optional[int]
    skill_id: Optional[int]
    run_id: Optional[int]
    reuse: int
    reuse_status: Optional[str]
    reuse_notes: Optional[str]
    input: str
    output: str
    created_at: Optional[str] = None


def get_tool_reuse_stats(
    *,
    tool_id: int,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[int, int]:
    """
    返回 (calls, reuse_calls)。
    """
    sql = "SELECT COUNT(*) AS calls, COALESCE(SUM(reuse), 0) AS reuse_calls FROM tool_call_records WHERE tool_id = ?"
    params = (int(tool_id),)
    with provide_connection(conn) as inner:
        row = inner.execute(sql, params).fetchone()
    calls = int(row["calls"]) if row else 0
    reuse_calls = int(row["reuse_calls"]) if row else 0
    return calls, reuse_calls


def create_tool_call_record(
    params: ToolCallRecordCreateParams,
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[int, str]:
    """
    写入一条 tool_call_records 并返回 (record_id, created_at)。
    """
    created = params.created_at or now_iso()
    sql = (
        "INSERT INTO tool_call_records "
        "(tool_id, task_id, skill_id, run_id, reuse, reuse_status, reuse_notes, input, output, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    sql_params = (
        int(params.tool_id),
        int(params.task_id) if params.task_id is not None else None,
        int(params.skill_id) if params.skill_id is not None else None,
        int(params.run_id) if params.run_id is not None else None,
        int(params.reuse),
        params.reuse_status,
        params.reuse_notes,
        str(params.input or ""),
        str(params.output or ""),
        created,
    )
    with provide_connection(conn) as inner:
        cursor = inner.execute(sql, sql_params)
        return int(cursor.lastrowid), created


def get_tool_reuse_stats_map(
    *,
    tool_ids: Sequence[int],
    conn: Optional[sqlite3.Connection] = None,
) -> Dict[int, Dict[str, int]]:
    """
    批量聚合 tool_call_records：返回 {tool_id: {"calls": n, "reuse_calls": m}}。
    """
    ids = [int(i) for i in tool_ids if i is not None]
    if not ids:
        return {}

    placeholders = ",".join(["?"] * len(ids))
    sql = (
        "SELECT tool_id, COUNT(*) AS calls, COALESCE(SUM(reuse), 0) AS reuse_calls "
        f"FROM tool_call_records WHERE tool_id IN ({placeholders}) GROUP BY tool_id"
    )
    with provide_connection(conn) as inner:
        rows = inner.execute(sql, ids).fetchall()

    out: Dict[int, Dict[str, int]] = {}
    for row in rows:
        out[int(row["tool_id"])] = {
            "calls": int(row["calls"]) if row["calls"] is not None else 0,
            "reuse_calls": int(row["reuse_calls"]) if row["reuse_calls"] is not None else 0,
        }
    return out


def get_tool_reuse_quality_map(
    *,
    tool_ids: Sequence[int],
    since: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Dict[int, Dict[str, int]]:
    """
    批量聚合 tool_call_records 的“质量信号”：
    - calls/reuse_calls：复用次数
    - pass_calls/fail_calls/unknown_calls：reuse_status 分布（可用于计算成功率）

    说明：
    - since 非空时，仅统计 created_at >= since 的记录（用于“最近成功率”）。
    """
    ids = [int(i) for i in tool_ids if i is not None]
    if not ids:
        return {}

    placeholders = ",".join(["?"] * len(ids))
    where = [f"tool_id IN ({placeholders})"]
    params: List = list(ids)
    if since:
        where.append("created_at >= ?")
        params.append(str(since))
    where_clause = " AND ".join(where)

    sql = (
        "SELECT tool_id, "
        "COUNT(*) AS calls, "
        "COALESCE(SUM(reuse), 0) AS reuse_calls, "
        "COALESCE(SUM(CASE WHEN reuse_status = 'pass' THEN 1 ELSE 0 END), 0) AS pass_calls, "
        "COALESCE(SUM(CASE WHEN reuse_status = 'fail' THEN 1 ELSE 0 END), 0) AS fail_calls, "
        "COALESCE(SUM(CASE WHEN reuse_status IS NULL OR reuse_status = 'unknown' THEN 1 ELSE 0 END), 0) AS unknown_calls "
        f"FROM tool_call_records WHERE {where_clause} "
        "GROUP BY tool_id"
    )
    with provide_connection(conn) as inner:
        rows = inner.execute(sql, params).fetchall()

    out: Dict[int, Dict[str, int]] = {}
    for row in rows:
        out[int(row["tool_id"])] = {
            "calls": int(row["calls"]) if row["calls"] is not None else 0,
            "reuse_calls": int(row["reuse_calls"]) if row["reuse_calls"] is not None else 0,
            "pass_calls": int(row["pass_calls"]) if row["pass_calls"] is not None else 0,
            "fail_calls": int(row["fail_calls"]) if row["fail_calls"] is not None else 0,
            "unknown_calls": int(row["unknown_calls"]) if row["unknown_calls"] is not None else 0,
        }
    return out


def get_skill_reuse_quality_map(
    *,
    skill_ids: Sequence[int],
    since: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Dict[int, Dict[str, int]]:
    """
    批量聚合 skill_id 维度的“质量信号”（来自 tool_call_records.skill_id）。

    说明：
    - 只统计 skill_id IS NOT NULL 的记录；
    - since 非空时，仅统计 created_at >= since 的记录（用于“最近成功率”）。
    """
    ids = [int(i) for i in skill_ids if i is not None]
    if not ids:
        return {}

    placeholders = ",".join(["?"] * len(ids))
    where = ["skill_id IS NOT NULL", f"skill_id IN ({placeholders})"]
    params: List = list(ids)
    if since:
        where.append("created_at >= ?")
        params.append(str(since))
    where_clause = " AND ".join(where)

    sql = (
        "SELECT skill_id, "
        "COUNT(*) AS calls, "
        "COALESCE(SUM(reuse), 0) AS reuse_calls, "
        "COALESCE(SUM(CASE WHEN reuse_status = 'pass' THEN 1 ELSE 0 END), 0) AS pass_calls, "
        "COALESCE(SUM(CASE WHEN reuse_status = 'fail' THEN 1 ELSE 0 END), 0) AS fail_calls, "
        "COALESCE(SUM(CASE WHEN reuse_status IS NULL OR reuse_status = 'unknown' THEN 1 ELSE 0 END), 0) AS unknown_calls "
        f"FROM tool_call_records WHERE {where_clause} "
        "GROUP BY skill_id"
    )
    with provide_connection(conn) as inner:
        rows = inner.execute(sql, params).fetchall()

    out: Dict[int, Dict[str, int]] = {}
    for row in rows:
        out[int(row["skill_id"])] = {
            "calls": int(row["calls"]) if row["calls"] is not None else 0,
            "reuse_calls": int(row["reuse_calls"]) if row["reuse_calls"] is not None else 0,
            "pass_calls": int(row["pass_calls"]) if row["pass_calls"] is not None else 0,
            "fail_calls": int(row["fail_calls"]) if row["fail_calls"] is not None else 0,
            "unknown_calls": int(row["unknown_calls"]) if row["unknown_calls"] is not None else 0,
        }
    return out


def list_tool_call_records(
    *,
    task_id: Optional[int],
    run_id: Optional[int],
    tool_id: Optional[int],
    reuse_status: Optional[str],
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
    if tool_id is not None:
        conditions.append("tool_id = ?")
        params.append(int(tool_id))
    if reuse_status is not None:
        conditions.append("reuse_status = ?")
        params.append(reuse_status)

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.extend([int(limit), int(offset)])
    sql = f"SELECT * FROM tool_call_records {where_clause} ORDER BY id ASC LIMIT ? OFFSET ?"
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def list_tool_call_records_for_task(
    *,
    task_id: int,
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    sql = "SELECT * FROM tool_call_records WHERE task_id = ? ORDER BY id ASC"
    params = (int(task_id),)
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def summarize_tool_reuse(
    *,
    task_id: Optional[int],
    run_id: Optional[int],
    tool_id: Optional[int],
    reuse_status: Optional[str],
    unknown_status_value: str,
    reuse_true_value: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[Optional[sqlite3.Row], List[sqlite3.Row], List[sqlite3.Row], List[sqlite3.Row]]:
    """
    工具复用统计聚合（供 /records/tools/reuse 使用）。
    返回：(summary_row, status_rows, tool_rows, tool_status_rows)
    """
    with provide_connection(conn) as inner:
        conditions = []
        params: List = []
        if task_id is not None:
            conditions.append("task_id = ?")
            params.append(int(task_id))
        if run_id is not None:
            conditions.append("run_id = ?")
            params.append(int(run_id))
        if tool_id is not None:
            conditions.append("tool_id = ?")
            params.append(int(tool_id))
        if reuse_status is not None:
            if reuse_status == unknown_status_value:
                conditions.append("(reuse_status = ? OR reuse_status IS NULL)")
                params.append(unknown_status_value)
            else:
                conditions.append("reuse_status = ?")
                params.append(reuse_status)
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        status_conditions = list(conditions)
        status_params = list(params)
        status_conditions.append("reuse = ?")
        status_params.append(int(reuse_true_value))
        status_where_clause = f"WHERE {' AND '.join(status_conditions)}" if status_conditions else ""

        summary_row = inner.execute(
            f"SELECT COUNT(*) AS calls, COALESCE(SUM(reuse), 0) AS reuse_calls FROM tool_call_records {where_clause}",
            params,
        ).fetchone()
        status_rows = inner.execute(
            "SELECT COALESCE(reuse_status, ?) AS status, COUNT(*) AS calls "
            "FROM tool_call_records "
            f"{status_where_clause} "
            "GROUP BY COALESCE(reuse_status, ?) "
            "ORDER BY calls DESC",
            [unknown_status_value, *status_params, unknown_status_value],
        ).fetchall()
        tool_rows = inner.execute(
            f"SELECT tool_id, COUNT(*) AS calls, COALESCE(SUM(reuse), 0) AS reuse_calls "
            f"FROM tool_call_records {where_clause} "
            "GROUP BY tool_id ORDER BY reuse_calls DESC, calls DESC LIMIT ?",
            params + [int(limit)],
        ).fetchall()
        tool_status_rows = inner.execute(
            "SELECT tool_id, COALESCE(reuse_status, ?) AS status, COUNT(*) AS calls "
            "FROM tool_call_records "
            f"{status_where_clause} "
            "GROUP BY tool_id, COALESCE(reuse_status, ?) "
            "ORDER BY tool_id ASC",
            [unknown_status_value, *status_params, unknown_status_value],
        ).fetchall()
        return summary_row, list(status_rows), list(tool_rows), list(tool_status_rows)


def summarize_skill_reuse(
    *,
    task_id: Optional[int],
    run_id: Optional[int],
    tool_id: Optional[int],
    reuse_status: Optional[str],
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[Optional[sqlite3.Row], List[sqlite3.Row]]:
    """
    技能复用统计聚合（供 /records/skills/reuse 使用）。
    返回：(total_row, rows)
    """
    with provide_connection(conn) as inner:
        conditions = ["skill_id IS NOT NULL"]
        params: List = []
        if task_id is not None:
            conditions.append("task_id = ?")
            params.append(int(task_id))
        if run_id is not None:
            conditions.append("run_id = ?")
            params.append(int(run_id))
        if tool_id is not None:
            conditions.append("tool_id = ?")
            params.append(int(tool_id))
        if reuse_status is not None:
            conditions.append("reuse_status = ?")
            params.append(reuse_status)
        where_clause = f"WHERE {' AND '.join(conditions)}"

        total_row = inner.execute(
            f"SELECT COUNT(*) AS calls, COALESCE(SUM(reuse), 0) AS reuse_calls FROM tool_call_records {where_clause}",
            params,
        ).fetchone()
        rows = inner.execute(
            f"SELECT skill_id, COUNT(*) AS calls, COALESCE(SUM(reuse), 0) AS reuse_calls "
            f"FROM tool_call_records {where_clause} "
            "GROUP BY skill_id ORDER BY reuse_calls DESC, calls DESC LIMIT ?",
            params + [int(limit)],
        ).fetchall()
        return total_row, list(rows)


def list_tool_calls_with_tool_name_by_run(
    *,
    run_id: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    """
    供评估/回放使用：带 tools_items.name 的 tool_call_records 列表。
    """
    sql = (
        "SELECT r.id, r.tool_id, t.name AS tool_name, r.input, r.output, r.reuse, r.reuse_status, r.reuse_notes, r.created_at "
        "FROM tool_call_records r "
        "LEFT JOIN tools_items t ON t.id = r.tool_id "
        "WHERE r.run_id = ? "
        "ORDER BY r.id ASC "
        "LIMIT ?"
    )
    params = (int(run_id), int(limit))
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def get_tool_call_record(
    *, record_id: int, conn: Optional[sqlite3.Connection] = None
) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM tool_call_records WHERE id = ?"
    params = (int(record_id),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def update_tool_call_record_validation(
    *,
    record_id: int,
    reuse_status: str,
    reuse_notes: Optional[str],
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[sqlite3.Row]:
    with provide_connection(conn) as inner:
        row = get_tool_call_record(record_id=record_id, conn=inner)
        if not row:
            return None
        inner.execute(
            "UPDATE tool_call_records SET reuse_status = ?, reuse_notes = ? WHERE id = ?",
            (reuse_status, reuse_notes, int(record_id)),
        )
        return get_tool_call_record(record_id=record_id, conn=inner)
