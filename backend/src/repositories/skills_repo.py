from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from backend.src.common.utils import now_iso
from backend.src.services.search.fts_search import build_fts_or_query, fts_table_exists
from backend.src.repositories.repo_conn import provide_connection


@dataclass(frozen=True)
class SkillCreateParams:
    """
    skills_items 创建参数（避免 15+ 参数的"爆炸式函数签名"）。
    """

    name: str
    description: Optional[str] = None
    scope: Optional[str] = None
    category: Optional[str] = None
    tags: Sequence[Any] = ()
    triggers: Sequence[Any] = ()
    aliases: Sequence[Any] = ()
    source_path: Optional[str] = None
    prerequisites: Sequence[Any] = ()
    inputs: Sequence[Any] = ()
    outputs: Sequence[Any] = ()
    steps: Sequence[Any] = ()
    failure_modes: Sequence[Any] = ()
    validation: Sequence[Any] = ()
    version: Optional[str] = None
    task_id: Optional[int] = None
    created_at: Optional[str] = None
    # Phase 2 新增字段
    domain_id: Optional[str] = None
    skill_type: Optional[str] = "methodology"  # methodology / solution
    status: Optional[str] = "approved"  # draft / approved / deprecated
    source_task_id: Optional[int] = None
    source_run_id: Optional[int] = None


def list_skills(*, conn: Optional[sqlite3.Connection] = None) -> List[sqlite3.Row]:
    sql = "SELECT * FROM skills_items ORDER BY id ASC"
    with provide_connection(conn) as inner:
        return list(inner.execute(sql).fetchall())


def get_skill(*, skill_id: int, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM skills_items WHERE id = ?"
    params = (int(skill_id),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def skill_exists(*, skill_id: int, conn: Optional[sqlite3.Connection] = None) -> bool:
    sql = "SELECT id FROM skills_items WHERE id = ?"
    params = (int(skill_id),)
    with provide_connection(conn) as inner:
        row = inner.execute(sql, params).fetchone()
        return bool(row and row["id"])


def create_skill(params: SkillCreateParams, *, conn: Optional[sqlite3.Connection] = None) -> int:
    created = params.created_at or now_iso()
    sql = (
        "INSERT INTO skills_items "
        "(name, created_at, description, scope, category, tags, triggers, aliases, source_path, prerequisites, inputs, outputs, steps, failure_modes, validation, version, task_id, domain_id, skill_type, status, source_task_id, source_run_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    sql_params = (
        params.name,
        created,
        params.description,
        params.scope,
        params.category,
        json.dumps(list(params.tags or []), ensure_ascii=False),
        json.dumps(list(params.triggers or []), ensure_ascii=False),
        json.dumps(list(params.aliases or []), ensure_ascii=False),
        params.source_path,
        json.dumps(list(params.prerequisites or []), ensure_ascii=False),
        json.dumps(list(params.inputs or []), ensure_ascii=False),
        json.dumps(list(params.outputs or []), ensure_ascii=False),
        json.dumps(list(params.steps or []), ensure_ascii=False),
        json.dumps(list(params.failure_modes or []), ensure_ascii=False),
        json.dumps(list(params.validation or []), ensure_ascii=False),
        params.version,
        params.task_id,
        params.domain_id,
        params.skill_type or "methodology",
        params.status or "approved",
        params.source_task_id,
        params.source_run_id,
    )
    with provide_connection(conn) as inner:
        cursor = inner.execute(sql, sql_params)
        return int(cursor.lastrowid)


def delete_skill(*, skill_id: int, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    """
    删除 skills_items，并返回被删除的行（若不存在则返回 None）。
    """
    with provide_connection(conn) as inner:
        row = get_skill(skill_id=skill_id, conn=inner)
        if not row:
            return None
        inner.execute("DELETE FROM skills_items WHERE id = ?", (int(skill_id),))
        return row


def update_skill(
    *,
    skill_id: int,
    name: Optional[str] = None,
    description: Optional[str] = None,
    scope: Optional[str] = None,
    category: Optional[str] = None,
    tags: Optional[Sequence[Any]] = None,
    triggers: Optional[Sequence[Any]] = None,
    aliases: Optional[Sequence[Any]] = None,
    source_path: Optional[str] = None,
    prerequisites: Optional[Sequence[Any]] = None,
    inputs: Optional[Sequence[Any]] = None,
    outputs: Optional[Sequence[Any]] = None,
    steps: Optional[Sequence[Any]] = None,
    failure_modes: Optional[Sequence[Any]] = None,
    validation: Optional[Sequence[Any]] = None,
    version: Optional[str] = None,
    task_id: Optional[int] = None,
    domain_id: Optional[str] = None,
    skill_type: Optional[str] = None,
    status: Optional[str] = None,
    source_task_id: Optional[int] = None,
    source_run_id: Optional[int] = None,
    change_notes: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[sqlite3.Row]:
    """
    更新 skills_items 指定字段，并返回更新后的行（若不存在则 None）。
    """
    fields: List[str] = []
    params: List[Any] = []
    if name is not None:
        fields.append("name = ?")
        params.append(name)
    if description is not None:
        fields.append("description = ?")
        params.append(description)
    if scope is not None:
        fields.append("scope = ?")
        params.append(scope)
    if category is not None:
        fields.append("category = ?")
        params.append(category)
    if tags is not None:
        fields.append("tags = ?")
        params.append(json.dumps(list(tags), ensure_ascii=False))
    if triggers is not None:
        fields.append("triggers = ?")
        params.append(json.dumps(list(triggers), ensure_ascii=False))
    if aliases is not None:
        fields.append("aliases = ?")
        params.append(json.dumps(list(aliases), ensure_ascii=False))
    if source_path is not None:
        fields.append("source_path = ?")
        params.append(source_path)
    if prerequisites is not None:
        fields.append("prerequisites = ?")
        params.append(json.dumps(list(prerequisites), ensure_ascii=False))
    if inputs is not None:
        fields.append("inputs = ?")
        params.append(json.dumps(list(inputs), ensure_ascii=False))
    if outputs is not None:
        fields.append("outputs = ?")
        params.append(json.dumps(list(outputs), ensure_ascii=False))
    if steps is not None:
        fields.append("steps = ?")
        params.append(json.dumps(list(steps), ensure_ascii=False))
    if failure_modes is not None:
        fields.append("failure_modes = ?")
        params.append(json.dumps(list(failure_modes), ensure_ascii=False))
    if validation is not None:
        fields.append("validation = ?")
        params.append(json.dumps(list(validation), ensure_ascii=False))
    if version is not None:
        fields.append("version = ?")
        params.append(version)
    if task_id is not None:
        fields.append("task_id = ?")
        params.append(int(task_id))
    if domain_id is not None:
        fields.append("domain_id = ?")
        params.append(str(domain_id))
    if skill_type is not None:
        fields.append("skill_type = ?")
        params.append(str(skill_type))
    if status is not None:
        fields.append("status = ?")
        params.append(str(status))
    if source_task_id is not None:
        fields.append("source_task_id = ?")
        params.append(int(source_task_id))
    if source_run_id is not None:
        fields.append("source_run_id = ?")
        params.append(int(source_run_id))

    with provide_connection(conn) as inner:
        existing = get_skill(skill_id=skill_id, conn=inner)
        if not existing:
            return None
        # 版本记录：仅在版本号发生变化时写入快照，支持“一键回滚到上一版本”。
        # 说明：
        # - 版本记录不要求每次 update 都写（避免膨胀），仅对“语义化版本迭代”留痕；
        # - previous_snapshot 存 raw DB 字段（JSON 字符串保持原样），回滚时可精确恢复。
        if version is not None:
            previous_version = existing["version"]
            next_version = version
            try:
                previous_text = str(previous_version or "")
                next_text = str(next_version or "")
            except Exception:
                previous_text = str(previous_version or "")
                next_text = str(next_version or "")
            if previous_text != next_text and next_text:
                try:
                    snapshot = {k: existing[k] for k in existing.keys()}
                    inner.execute(
                        "INSERT INTO skill_version_records (skill_id, previous_version, next_version, previous_snapshot, change_notes, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            int(skill_id),
                            previous_version,
                            next_version,
                            json.dumps(snapshot, ensure_ascii=False),
                            str(change_notes or "").strip() or None,
                            now_iso(),
                        ),
                    )
                except Exception:
                    # 版本记录失败不应阻塞主流程（保持可用性）
                    pass
        if fields:
            params.append(int(skill_id))
            inner.execute(
                f"UPDATE skills_items SET {', '.join(fields)} WHERE id = ?",
                params,
            )
        return get_skill(skill_id=skill_id, conn=inner)


def search_skills_fts_or_like(*, q: str, limit: int = 10, conn: Optional[sqlite3.Connection] = None) -> List[sqlite3.Row]:
    """
    技能检索：优先 FTS5，回退 LIKE。
    """
    with provide_connection(conn) as inner:
        fts_query = build_fts_or_query(q, limit=limit)
        if fts_query and fts_table_exists(inner, "skills_items_fts"):
            return list(
                inner.execute(
                    """
                    SELECT s.*
                    FROM skills_items_fts f
                    JOIN skills_items s ON s.id = f.rowid
                    WHERE skills_items_fts MATCH ?
                    ORDER BY bm25(skills_items_fts) ASC, s.id DESC
                    LIMIT ?
                    """,
                    (fts_query, int(limit)),
                ).fetchall()
            )

        pattern = f"%{q}%"
        return list(
            inner.execute(
                "SELECT * FROM skills_items WHERE name LIKE ? OR description LIKE ? OR scope LIKE ? ORDER BY id ASC LIMIT ?",
                (pattern, pattern, pattern, int(limit)),
            ).fetchall()
        )


def list_skill_catalog_source(*, conn: Optional[sqlite3.Connection] = None) -> List[sqlite3.Row]:
    """
    catalog 聚合所需的最小列集合（category/tags/skill_type/status）。
    """
    sql = "SELECT category, tags, skill_type, status FROM skills_items"
    with provide_connection(conn) as inner:
        return list(inner.execute(sql).fetchall())


def search_skills_filtered_like(
    *,
    q: Optional[str],
    category: Optional[str],
    tag: Optional[str],
    skill_type: Optional[str],
    status: Optional[str],
    limit: int,
    offset: int,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[int, List[sqlite3.Row]]:
    """
    技能检索（LIKE 过滤版）：用于 /skills/search（面向 UI/Agent 的筛选）。
    """
    with provide_connection(conn) as inner:
        conditions: List[str] = []
        params: List[Any] = []

        if q:
            pattern = f"%{q}%"
            extra = ""
            extra_params: List[str] = []
            try:
                if any(ord(ch) > 127 for ch in q):
                    escaped = json.dumps(q, ensure_ascii=True)[1:-1]
                    escaped_pattern = f"%{escaped}%"
                    extra = " OR tags LIKE ? OR triggers LIKE ?"
                    extra_params = [escaped_pattern, escaped_pattern]
            except Exception:
                extra = ""
                extra_params = []

            conditions.append(
                "("
                "name LIKE ? OR description LIKE ? OR scope LIKE ? OR category LIKE ? OR tags LIKE ? OR triggers LIKE ?"
                f"{extra}"
                ")"
            )
            params.extend([pattern, pattern, pattern, pattern, pattern, pattern] + extra_params)

        if category:
            cat = category.strip()
            if cat:
                conditions.append("(category = ? OR category LIKE ?)")
                params.append(cat)
                params.append(f"{cat}.%")

        if tag:
            t = tag.strip()
            if t:
                v1 = json.dumps(t, ensure_ascii=False)
                v2 = json.dumps(t, ensure_ascii=True)
                conditions.append("(tags LIKE ? OR tags LIKE ?)")
                params.append(f"%{v1}%")
                params.append(f"%{v2}%")

        if skill_type:
            st = skill_type.strip()
            if st:
                conditions.append("skill_type = ?")
                params.append(st)

        if status:
            st = status.strip().lower()
            if st:
                conditions.append("LOWER(status) = ?")
                params.append(st)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        total_row = inner.execute(
            f"SELECT COUNT(*) AS count FROM skills_items {where_clause}",
            params,
        ).fetchone()
        rows = inner.execute(
            f"SELECT * FROM skills_items {where_clause} ORDER BY id DESC LIMIT ? OFFSET ?",
            params + [int(limit), int(offset)],
        ).fetchall()
        total = int(total_row["count"]) if total_row else 0
        return total, list(rows)


# 有效的技能状态值
VALID_SKILL_STATUSES = {"draft", "approved", "deprecated", "abandoned"}


def update_skill_status(
    *,
    skill_id: int,
    status: str,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[sqlite3.Row]:
    """
    更新技能状态（draft/approved/deprecated/abandoned）。

    状态转换规则：
    - draft → approved：技能通过审核，可参与常规检索
    - approved → deprecated：技能已过时，不再参与检索
    - draft → abandoned：评估失败的草稿，保留供溯源，不参与后续检索
    - deprecated → approved：重新启用已过时的技能
    - draft → deprecated：直接废弃未审核的草稿

    参数：
    - skill_id: 技能 ID
    - status: 目标状态（draft/approved/deprecated/abandoned）

    返回：
    - 更新后的技能行（若不存在或状态无效则返回 None）
    """
    status = status.strip().lower()
    if status not in VALID_SKILL_STATUSES:
        return None

    with provide_connection(conn) as inner:
        existing = get_skill(skill_id=skill_id, conn=inner)
        if not existing:
            return None

        inner.execute(
            "UPDATE skills_items SET status = ? WHERE id = ?",
            (status, int(skill_id)),
        )
        return get_skill(skill_id=skill_id, conn=inner)


def list_skills_by_status(
    *,
    status: str,
    limit: int = 100,
    offset: int = 0,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[int, List[sqlite3.Row]]:
    """
    按状态列出技能（用于管理界面查看 draft/deprecated 技能）。
    """
    status = status.strip().lower()
    if status not in VALID_SKILL_STATUSES:
        return 0, []

    with provide_connection(conn) as inner:
        total_row = inner.execute(
            "SELECT COUNT(*) AS count FROM skills_items WHERE status = ?",
            (status,),
        ).fetchone()
        rows = inner.execute(
            "SELECT * FROM skills_items WHERE status = ? ORDER BY id DESC LIMIT ? OFFSET ?",
            (status, int(limit), int(offset)),
        ).fetchall()
        total = int(total_row["count"]) if total_row else 0
        return total, list(rows)
