from __future__ import annotations

import json
import sqlite3
from typing import Any, List, Optional, Sequence

from backend.src.common.utils import now_iso
from backend.src.repositories.repo_conn import provide_connection


def create_agent_review_record(
    *,
    task_id: int,
    run_id: int,
    status: str,
    pass_score: Optional[float] = None,
    pass_threshold: Optional[float] = None,
    distill_status: Optional[str] = None,
    distill_score: Optional[float] = None,
    distill_threshold: Optional[float] = None,
    distill_notes: Optional[str] = None,
    distill_evidence_refs: Optional[Sequence[Any]] = None,
    summary: str,
    issues: Sequence[Any],
    next_actions: Sequence[Any],
    skills: Sequence[Any],
    created_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> int:
    created = created_at or now_iso()
    sql = (
        "INSERT INTO agent_review_records "
        "(task_id, run_id, status, pass_score, pass_threshold, distill_status, distill_score, distill_threshold, distill_notes, distill_evidence_refs, summary, issues, next_actions, skills, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    params = (
        int(task_id),
        int(run_id),
        status,
        float(pass_score) if pass_score is not None else None,
        float(pass_threshold) if pass_threshold is not None else None,
        str(distill_status) if distill_status is not None else None,
        float(distill_score) if distill_score is not None else None,
        float(distill_threshold) if distill_threshold is not None else None,
        str(distill_notes) if distill_notes is not None else None,
        json.dumps(list(distill_evidence_refs or []), ensure_ascii=False),
        summary,
        json.dumps(list(issues or []), ensure_ascii=False),
        json.dumps(list(next_actions or []), ensure_ascii=False),
        json.dumps(list(skills or []), ensure_ascii=False),
        created,
    )
    with provide_connection(conn) as inner:
        try:
            cursor = inner.execute(sql, params)
        except sqlite3.OperationalError as exc:
            # 兼容旧库：pass_score/distill_status 等列可能尚未迁移完成
            msg = str(exc or "")
            if (
                "no column named pass_score" in msg
                or "no column named distill_status" in msg
                or "no column named distill_evidence_refs" in msg
            ):
                legacy_sql = (
                    "INSERT INTO agent_review_records "
                    "(task_id, run_id, status, summary, issues, next_actions, skills, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                )
                legacy_params = (
                    int(task_id),
                    int(run_id),
                    status,
                    summary,
                    json.dumps(list(issues or []), ensure_ascii=False),
                    json.dumps(list(next_actions or []), ensure_ascii=False),
                    json.dumps(list(skills or []), ensure_ascii=False),
                    created,
                )
                cursor = inner.execute(legacy_sql, legacy_params)
            else:
                raise
        return int(cursor.lastrowid)


def update_agent_review_record(
    *,
    review_id: int,
    status: Optional[str] = None,
    pass_score: Optional[float] = None,
    pass_threshold: Optional[float] = None,
    distill_status: Optional[str] = None,
    distill_score: Optional[float] = None,
    distill_threshold: Optional[float] = None,
    distill_notes: Optional[str] = None,
    distill_evidence_refs: Optional[Sequence[Any]] = None,
    summary: Optional[str] = None,
    issues: Optional[Sequence[Any]] = None,
    next_actions: Optional[Sequence[Any]] = None,
    skills: Optional[Sequence[Any]] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[sqlite3.Row]:
    """
    更新 agent_review_records 指定字段，并返回最新记录。
    """
    rid = int(review_id)
    if rid <= 0:
        return None

    fields = []
    params: list[Any] = []

    if status is not None:
        fields.append("status = ?")
        params.append(str(status))
    if pass_score is not None:
        fields.append("pass_score = ?")
        params.append(float(pass_score))
    if pass_threshold is not None:
        fields.append("pass_threshold = ?")
        params.append(float(pass_threshold))
    if distill_status is not None:
        fields.append("distill_status = ?")
        params.append(str(distill_status))
    if distill_score is not None:
        fields.append("distill_score = ?")
        params.append(float(distill_score))
    if distill_threshold is not None:
        fields.append("distill_threshold = ?")
        params.append(float(distill_threshold))
    if distill_notes is not None:
        fields.append("distill_notes = ?")
        params.append(str(distill_notes))
    if distill_evidence_refs is not None:
        fields.append("distill_evidence_refs = ?")
        params.append(json.dumps(list(distill_evidence_refs or []), ensure_ascii=False))
    if summary is not None:
        fields.append("summary = ?")
        params.append(str(summary))
    if issues is not None:
        fields.append("issues = ?")
        params.append(json.dumps(list(issues or []), ensure_ascii=False))
    if next_actions is not None:
        fields.append("next_actions = ?")
        params.append(json.dumps(list(next_actions or []), ensure_ascii=False))
    if skills is not None:
        fields.append("skills = ?")
        params.append(json.dumps(list(skills or []), ensure_ascii=False))

    with provide_connection(conn) as inner:
        if fields:
            params.append(rid)
            try:
                inner.execute(
                    f"UPDATE agent_review_records SET {', '.join(fields)} WHERE id = ?",
                    params,
                )
            except sqlite3.OperationalError as exc:
                # 兼容旧库：pass_score/distill_status 等列可能尚未迁移完成
                msg = str(exc or "")
                if "no such column" in msg:
                    filtered_fields: list[str] = []
                    filtered_params: list[Any] = []
                    for f, p in zip(fields, params[:-1]):
                        # 仅保留旧字段（status/summary/issues/next_actions/skills）
                        if f.startswith(
                            (
                                "pass_score",
                                "pass_threshold",
                                "distill_status",
                                "distill_score",
                                "distill_threshold",
                                "distill_notes",
                                "distill_evidence_refs",
                            )
                        ):
                            continue
                        filtered_fields.append(f)
                        filtered_params.append(p)
                    filtered_params.append(rid)
                    if filtered_fields:
                        inner.execute(
                            f"UPDATE agent_review_records SET {', '.join(filtered_fields)} WHERE id = ?",
                            filtered_params,
                        )
                else:
                    raise
        return get_agent_review(review_id=rid, conn=inner)


def list_agent_reviews(
    *,
    offset: int,
    limit: int,
    task_id: Optional[int],
    run_id: Optional[int],
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    with provide_connection(conn) as inner:
        where = []
        params: list[Any] = []
        if task_id is not None:
            where.append("task_id = ?")
            params.append(int(task_id))
        if run_id is not None:
            where.append("run_id = ?")
            params.append(int(run_id))
        where_clause = f"WHERE {' AND '.join(where)}" if where else ""
        sql = (
            f"SELECT id, task_id, run_id, status, distill_status, summary, created_at FROM agent_review_records {where_clause} "
            "ORDER BY id DESC LIMIT ? OFFSET ?"
        )
        rows = inner.execute(sql, (*params, int(limit), int(offset))).fetchall()
        return list(rows)


def get_agent_review(*, review_id: int, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM agent_review_records WHERE id = ?"
    params = (int(review_id),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def get_latest_agent_review_id_for_run(
    *,
    run_id: int,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[int]:
    """
    获取某个 run 最新的一条评估记录 id（用于去重：每个 run 默认只自动评估一次）。
    """
    sql = "SELECT id FROM agent_review_records WHERE run_id = ? ORDER BY id DESC LIMIT 1"
    params = (int(run_id),)
    with provide_connection(conn) as inner:
        row = inner.execute(sql, params).fetchone()
    return int(row["id"]) if row and row["id"] is not None else None
