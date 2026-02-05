from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timedelta
from typing import List, Optional

from backend.src.common.utils import now_iso, parse_json_list
from backend.src.constants import (
    AGENT_KNOWLEDGE_RERANK_RECENT_DAYS,
    AGENT_KNOWLEDGE_RERANK_REUSE_CALLS_CAP,
    AGENT_KNOWLEDGE_RERANK_WEIGHT_BASE,
    AGENT_KNOWLEDGE_RERANK_WEIGHT_REUSE,
    AGENT_KNOWLEDGE_RERANK_WEIGHT_SUCCESS,
    TOOL_APPROVAL_STATUS_DRAFT,
    TOOL_APPROVAL_STATUS_REJECTED,
    TOOL_METADATA_APPROVAL_KEY,
)
from backend.src.repositories.tool_call_records_repo import (
    get_skill_reuse_quality_map,
    get_tool_reuse_quality_map,
)
from backend.src.services.search.fts_search import build_fts_or_query, fts_table_exists
from backend.src.repositories.repo_conn import provide_connection


def _resolve_rerank_since() -> Optional[str]:
    """
    P1：知识库质量信号窗口（最近 N 天）。
    """
    try:
        days = int(AGENT_KNOWLEDGE_RERANK_RECENT_DAYS)
    except Exception:
        days = 30
    if days <= 0:
        return None
    try:
        now_dt = datetime.fromisoformat(now_iso().replace("Z", "+00:00"))
        since_dt = now_dt - timedelta(days=int(days))
        return since_dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def list_tool_hints(*, limit: int = 8, conn: Optional[sqlite3.Connection] = None) -> List[dict]:
    """
    给 Agent 提供“可用工具清单”的最小字段集合（用于拼 prompt）。
    """
    with provide_connection(conn) as inner:
        # 过滤掉未批准（draft/rejected）的工具，避免 Agent 在规划阶段复用“未验证的新工具”。
        # 说明：未写 approval 字段的历史工具默认视为 approved。
        rows = inner.execute(
            "SELECT id, name, description, metadata FROM tools_items ORDER BY id ASC LIMIT ?",
            (max(int(limit) * 4, 16),),
        ).fetchall()
    items: List[dict] = []
    for row in rows:
        status = ""
        if row["metadata"]:
            try:
                meta = json.loads(row["metadata"])
            except Exception:
                meta = None
            if isinstance(meta, dict):
                approval = meta.get(TOOL_METADATA_APPROVAL_KEY)
                if isinstance(approval, dict):
                    status = str(approval.get("status") or "").strip().lower()
        if status in {TOOL_APPROVAL_STATUS_DRAFT, TOOL_APPROVAL_STATUS_REJECTED}:
            continue
        items.append(
            {
                "id": int(row["id"]),
                "name": row["name"],
                "description": row["description"] or "",
            }
        )

    # re-rank：融入“被复用次数/最近成功率”（P1：知识库质量）
    try:
        limit_value = int(limit)
    except Exception:
        limit_value = 8
    if limit_value <= 0:
        limit_value = 8
    if not items:
        return []

    since = _resolve_rerank_since()

    tool_ids = [int(it["id"]) for it in items if it.get("id") is not None]
    stats_map = get_tool_reuse_quality_map(tool_ids=tool_ids, since=since)

    def _key(it: dict):
        tid = int(it.get("id") or 0)
        stats = stats_map.get(tid) or {}
        calls = int(stats.get("calls") or 0)
        reuse_calls = int(stats.get("reuse_calls") or 0)
        pass_calls = int(stats.get("pass_calls") or 0)
        fail_calls = int(stats.get("fail_calls") or 0)
        denom = pass_calls + fail_calls
        success_rate = (pass_calls / denom) if denom else 0.0
        return (success_rate, reuse_calls, calls, tid)

    ranked = sorted(items, key=_key, reverse=True)
    return ranked[:limit_value]


def list_tool_hints_by_names(
    *,
    names: List[str],
    limit: int = 8,
    conn: Optional[sqlite3.Connection] = None,
) -> List[dict]:
    """
    按名称读取工具提示（用于“方案提到的工具优先注入”）。

    说明：
    - 保持输入顺序（按 names 的出现顺序优先）
    - 过滤掉未批准（draft/rejected）的工具
    """
    raw_names: List[str] = []
    seen = set()
    for n in names or []:
        name = str(n or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        raw_names.append(name)
        if len(raw_names) >= max(int(limit) * 4, 32):
            break

    if not raw_names:
        return []

    placeholders = ",".join(["?"] * len(raw_names))
    order_cases = " ".join([f"WHEN ? THEN {i}" for i in range(len(raw_names))])
    sql = (
        f"SELECT id, name, description, metadata FROM tools_items "
        f"WHERE name IN ({placeholders}) "
        f"ORDER BY CASE name {order_cases} ELSE 999 END, id ASC"
    )

    with provide_connection(conn) as inner:
        rows = inner.execute(sql, (*raw_names, *raw_names)).fetchall()

    items: List[dict] = []
    for row in rows:
        status = ""
        if row["metadata"]:
            try:
                meta = json.loads(row["metadata"])
            except Exception:
                meta = None
            if isinstance(meta, dict):
                approval = meta.get(TOOL_METADATA_APPROVAL_KEY)
                if isinstance(approval, dict):
                    status = str(approval.get("status") or "").strip().lower()
        if status in {TOOL_APPROVAL_STATUS_DRAFT, TOOL_APPROVAL_STATUS_REJECTED}:
            continue
        items.append(
            {
                "id": int(row["id"]),
                "name": row["name"],
                "description": row["description"] or "",
            }
        )
        if len(items) >= int(limit):
            break
    return items


def list_skill_candidates(
    *,
    limit: int,
    query_text: Optional[str] = None,
    debug: Optional[dict] = None,
    include_draft: bool = False,
    skill_type: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> List[dict]:
    """
    读取技能候选集（用于 LLM 二次筛选）。
    - 相关性优先：FTS5
    - 兜底：最近 skills
    - 默认只返回 approved 状态的技能（include_draft=True 时包含 draft）
    """
    try:
        with provide_connection(conn) as inner:
            items: List[dict] = []
            seen_ids = set()

            # skill_type 过滤（兼容旧数据：NULL 视为 methodology）
            skill_type_value = str(skill_type or "").strip().lower()
            if skill_type_value == "methodology":
                skill_type_condition = " AND (s.skill_type = 'methodology' OR s.skill_type IS NULL)"
                skill_type_params: List[str] = []
                recent_skill_type_condition = " AND (skill_type = 'methodology' OR skill_type IS NULL)"
                recent_skill_type_params: List[str] = []
            elif skill_type_value == "solution":
                skill_type_condition = " AND s.skill_type = 'solution'"
                skill_type_params = []
                recent_skill_type_condition = " AND skill_type = 'solution'"
                recent_skill_type_params = []
            elif skill_type_value:
                skill_type_condition = " AND s.skill_type = ?"
                skill_type_params = [skill_type_value]
                recent_skill_type_condition = " AND skill_type = ?"
                recent_skill_type_params = [skill_type_value]
            else:
                skill_type_condition = ""
                skill_type_params = []
                recent_skill_type_condition = ""
                recent_skill_type_params = []

            # 状态过滤条件
            if include_draft:
                status_condition = "s.status IN ('approved', 'draft')"
            else:
                status_condition = "(s.status = 'approved' OR s.status IS NULL)"

            started = time.perf_counter()
            fts_query = build_fts_or_query(query_text, limit=12) if query_text else ""
            fts_available = bool(fts_query) and fts_table_exists(inner, "skills_items_fts")
            fts_used = False
            fts_hits = 0
            fts_time_ms: Optional[int] = None
            recent_time_ms: Optional[int] = None
            from_recent = 0

            # 1) 相关性召回：优先用 FTS5（简单但强），把"最可能相关"的技能放进候选集
            if fts_available:
                fts_used = True
                t0 = time.perf_counter()
                rows = inner.execute(
                    f"""
                        SELECT s.id, s.name, s.description, s.scope, s.category, s.tags
                        FROM skills_items_fts f
                        JOIN skills_items s ON s.id = f.rowid
                        WHERE skills_items_fts MATCH ? AND {status_condition}{skill_type_condition}
                        ORDER BY bm25(skills_items_fts) ASC, s.id DESC
                        LIMIT ?
                        """,
                    (fts_query, *skill_type_params, int(limit)),
                ).fetchall()
                fts_time_ms = int((time.perf_counter() - t0) * 1000)
                fts_hits = len(rows)
                for row in rows:
                    sid = int(row["id"])
                    if sid in seen_ids:
                        continue
                    seen_ids.add(sid)
                    items.append(
                        {
                            "id": sid,
                            "name": row["name"],
                            "description": row["description"] or "",
                            "scope": row["scope"] or "",
                            "category": row["category"] or "",
                            "tags": parse_json_list(row["tags"]),
                        }
                    )

            # 2) 新近补齐：为了避免 FTS 未命中导致"技能候选为空"，补一批最新技能
            if len(items) < limit:
                t0 = time.perf_counter()
                # 兼容旧数据：status 为 NULL 的视为 approved
                if include_draft:
                    recent_status_condition = "status IN ('approved', 'draft') OR status IS NULL"
                else:
                    recent_status_condition = "status = 'approved' OR status IS NULL"
                rows = inner.execute(
                    f"SELECT id, name, description, scope, category, tags FROM skills_items s WHERE {recent_status_condition}{recent_skill_type_condition} ORDER BY id DESC LIMIT ?",
                    (*recent_skill_type_params, int(limit)),
                ).fetchall()
                recent_time_ms = int((time.perf_counter() - t0) * 1000)
                for row in rows:
                    sid = int(row["id"])
                    if sid in seen_ids:
                        continue
                    seen_ids.add(sid)
                    from_recent += 1
                    items.append(
                        {
                            "id": sid,
                            "name": row["name"],
                            "description": row["description"] or "",
                            "scope": row["scope"] or "",
                            "category": row["category"] or "",
                            "tags": parse_json_list(row["tags"]),
                        }
                    )

            elapsed_ms = int((time.perf_counter() - started) * 1000)
            if isinstance(debug, dict):
                try:
                    total = len(items[:limit])
                    from_fts = total - int(from_recent)
                    hit_rate = round(float(from_fts) / float(limit), 3) if limit else 0.0
                    debug.update(
                        {
                            "fts_used": bool(fts_used),
                            "fts_available": bool(fts_available),
                            "fts_hits": int(fts_hits),
                            "fts_hit_rate": hit_rate,
                            "fts_time_ms": int(fts_time_ms) if fts_time_ms is not None else None,
                            "recent_time_ms": int(recent_time_ms)
                            if recent_time_ms is not None
                            else None,
                            "elapsed_ms": int(elapsed_ms),
                            "total": int(total),
                            "include_draft": include_draft,
                            "skill_type": skill_type_value or None,
                        }
                    )
                    if fts_query:
                        debug["fts_query"] = fts_query[:120]
                except Exception as exc:
                    debug["metrics_error"] = str(exc)

            selected = items[:limit]
            # P1：知识库质量信号 re-rank（仅对候选列表做轻量重排，避免完全覆盖 FTS 相关性）
            try:
                limit_value = int(limit)
            except Exception:
                limit_value = 0
            if limit_value > 0 and selected:
                since = _resolve_rerank_since()

                skill_ids = [int(it.get("id") or 0) for it in selected if it.get("id") is not None]
                stats_map = get_skill_reuse_quality_map(skill_ids=skill_ids, since=since)

                n = len(selected)

                def _rerank_key(pair):
                    idx, it = pair
                    sid = int(it.get("id") or 0)
                    stats = stats_map.get(sid) or {}
                    calls = int(stats.get("calls") or 0)
                    reuse_calls = int(stats.get("reuse_calls") or 0)
                    pass_calls = int(stats.get("pass_calls") or 0)
                    fail_calls = int(stats.get("fail_calls") or 0)
                    denom = pass_calls + fail_calls
                    success_rate = (pass_calls / denom) if denom else 0.0
                    base = ((n - idx) / n) if n else 0.0
                    try:
                        cap = int(AGENT_KNOWLEDGE_RERANK_REUSE_CALLS_CAP)
                    except Exception:
                        cap = 10
                    if cap <= 0:
                        cap = 10
                    reuse_bonus = min(1.0, reuse_calls / float(cap)) if reuse_calls > 0 else 0.0
                    try:
                        w_base = float(AGENT_KNOWLEDGE_RERANK_WEIGHT_BASE)
                    except Exception:
                        w_base = 0.7
                    try:
                        w_success = float(AGENT_KNOWLEDGE_RERANK_WEIGHT_SUCCESS)
                    except Exception:
                        w_success = 0.2
                    try:
                        w_reuse = float(AGENT_KNOWLEDGE_RERANK_WEIGHT_REUSE)
                    except Exception:
                        w_reuse = 0.1
                    score = base * w_base + success_rate * w_success + reuse_bonus * w_reuse
                    return (score, base, success_rate, reuse_calls, calls, sid)

                selected = [
                    it for _idx, it in sorted(enumerate(selected), key=_rerank_key, reverse=True)
                ]
            return selected
    except Exception as exc:
        if isinstance(debug, dict):
            debug["error"] = str(exc)
        return []


def list_solution_candidates_by_skill_tags(
    *,
    skill_tags: List[str],
    limit: int,
    domain_ids: Optional[List[str]] = None,
    debug: Optional[dict] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> List[dict]:
    """
    按“技能 tag”匹配方案候选集（Solution=skills_items.skill_type='solution'）。

    约定：
    - skills_items.tags 存储 JSON 数组
    - 方案生成时会写入形如 "skill:{skill_id}" 的 tag，便于后续按技能检索方案

    注意：
    - 这里不用 FTS：直接按 tags LIKE 匹配（兼容 tags 里可能包含中文/转义 JSON 的情况）
    """
    raw_tags: List[str] = []
    seen = set()
    for t in skill_tags or []:
        tag = str(t or "").strip()
        if not tag or tag in seen:
            continue
        seen.add(tag)
        raw_tags.append(tag)
        if len(raw_tags) >= 32:
            break

    if not raw_tags:
        return []

    try:
        limit_value = int(limit)
    except Exception:
        limit_value = 30
    if limit_value <= 0:
        limit_value = 30

    try:
        with provide_connection(conn) as inner:
            domain_where = ""
            domain_params: List[str] = []
            if domain_ids:
                # 前缀匹配：支持 data 匹配 data.clean / data.collect 等
                conds = []
                for did in domain_ids:
                    key = str(did or "").strip()
                    if not key:
                        continue
                    conds.append("(domain_id = ? OR domain_id LIKE ?)")
                    domain_params.extend([key, f"{key}.%"])
                if conds:
                    domain_where = "(" + " OR ".join(conds) + ") AND "

            tag_conds = []
            tag_params: List[str] = []
            for tag in raw_tags:
                v1 = json.dumps(tag, ensure_ascii=False)
                v2 = json.dumps(tag, ensure_ascii=True)
                tag_conds.append("(tags LIKE ? OR tags LIKE ?)")
                tag_params.append(f"%{v1}%")
                tag_params.append(f"%{v2}%")
            tag_where = "(" + " OR ".join(tag_conds) + ")"

            # 兼容旧数据：status 为 NULL 的视为 approved
            sql = (
                "SELECT id, name, description, scope, category, tags, domain_id "
                "FROM skills_items s "
                f"WHERE {domain_where}(status = 'approved' OR status IS NULL) "
                "AND s.skill_type = 'solution' "
                f"AND {tag_where} "
                "ORDER BY id DESC LIMIT ?"
            )
            rows = inner.execute(sql, (*domain_params, *tag_params, int(limit_value))).fetchall()

        items: List[dict] = []
        for row in rows or []:
            items.append(
                {
                    "id": int(row["id"]),
                    "name": row["name"],
                    "description": row["description"] or "",
                    "scope": row["scope"] or "",
                    "category": row["category"] or "",
                    "tags": parse_json_list(row["tags"]),
                    "domain_id": row["domain_id"] or "",
                }
            )

        if isinstance(debug, dict):
            debug.update(
                {
                    "tag_match_used": True,
                    "tag_match_tags": raw_tags[:12],
                    "tag_match_total": len(items),
                    "tag_match_domain_filter": list(domain_ids or []),
                }
            )

        # P1：知识库质量信号 re-rank（solutions 也是 skills，复用信号来自 tool_call_records.skill_id）
        since = _resolve_rerank_since()
        if items and limit_value > 0:
            stats_map = get_skill_reuse_quality_map(
                skill_ids=[int(it.get("id") or 0) for it in items if it.get("id") is not None],
                since=since,
            )

            def _key(it: dict):
                sid = int(it.get("id") or 0)
                stats = stats_map.get(sid) or {}
                calls = int(stats.get("calls") or 0)
                reuse_calls = int(stats.get("reuse_calls") or 0)
                pass_calls = int(stats.get("pass_calls") or 0)
                fail_calls = int(stats.get("fail_calls") or 0)
                denom = pass_calls + fail_calls
                success_rate = (pass_calls / denom) if denom else 0.0
                return (success_rate, reuse_calls, calls, sid)

            items = sorted(items, key=_key, reverse=True)

        return items[:limit_value]
    except Exception as exc:
        if isinstance(debug, dict):
            debug["tag_match_error"] = str(exc)
        return []


def load_skills_by_ids(skill_ids: List[int], *, conn: Optional[sqlite3.Connection] = None) -> List[dict]:
    if not skill_ids:
        return []
    placeholders = ",".join(["?"] * len(skill_ids))
    try:
        with provide_connection(conn) as inner:
            rows = inner.execute(
                f"SELECT * FROM skills_items WHERE id IN ({placeholders})",
                skill_ids,
            ).fetchall()
        by_id = {int(row["id"]): row for row in rows}
        ordered = []
        for sid in skill_ids:
            row = by_id.get(int(sid))
            if not row:
                continue
            ordered.append(
                {
                    "id": int(row["id"]),
                    "name": row["name"],
                    "description": row["description"],
                    "scope": row["scope"],
                    "category": row["category"],
                    "tags": parse_json_list(row["tags"]),
                    "triggers": parse_json_list(row["triggers"]),
                    "aliases": parse_json_list(row["aliases"]),
                    "source_path": row["source_path"],
                    "prerequisites": parse_json_list(row["prerequisites"]),
                    "inputs": parse_json_list(row["inputs"]),
                    "outputs": parse_json_list(row["outputs"]),
                    "steps": parse_json_list(row["steps"]),
                    "failure_modes": parse_json_list(row["failure_modes"]),
                    "validation": parse_json_list(row["validation"]),
                    "version": row["version"],
                }
            )
        return ordered
    except Exception:
        return []


def list_memory_candidates(
    *,
    limit: int,
    query_text: Optional[str] = None,
    debug: Optional[dict] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> List[dict]:
    """
    从 memory_items 读取候选记忆：
    - 相关性优先：FTS5
    - 兜底：最近 memories
    """
    try:
        with provide_connection(conn) as inner:
            items: List[dict] = []
            seen_ids = set()

            started = time.perf_counter()
            fts_query = build_fts_or_query(query_text, limit=12) if query_text else ""
            fts_available = bool(fts_query) and fts_table_exists(inner, "memory_items_fts")
            fts_used = False
            fts_hits = 0
            fts_time_ms: Optional[int] = None
            recent_time_ms: Optional[int] = None
            from_recent = 0

            # 1) 相关性召回：优先用 FTS5（简单但强），把“最可能相关”的记忆放进候选集
            if fts_available:
                fts_used = True
                t0 = time.perf_counter()
                rows = inner.execute(
                    """
                        SELECT m.id, m.content, m.memory_type, m.tags
                        FROM memory_items_fts f
                        JOIN memory_items m ON m.id = f.rowid
                        WHERE memory_items_fts MATCH ?
                        ORDER BY bm25(memory_items_fts) ASC, m.id DESC
                        LIMIT ?
                        """,
                    (fts_query, int(limit)),
                ).fetchall()
                fts_time_ms = int((time.perf_counter() - t0) * 1000)
                fts_hits = len(rows)
                for row in rows:
                    mid = int(row["id"])
                    if mid in seen_ids:
                        continue
                    seen_ids.add(mid)
                    items.append(
                        {
                            "id": mid,
                            "content": row["content"] or "",
                            "memory_type": row["memory_type"] or "",
                            "tags": parse_json_list(row["tags"]),
                        }
                    )

            # 2) 新近补齐：避免 FTS 未命中导致候选为空，同时保留“最近上下文”的价值
            if len(items) < limit:
                t0 = time.perf_counter()
                rows = inner.execute(
                    "SELECT id, content, memory_type, tags FROM memory_items ORDER BY id DESC LIMIT ?",
                    (int(limit),),
                ).fetchall()
                recent_time_ms = int((time.perf_counter() - t0) * 1000)
                for row in rows:
                    mid = int(row["id"])
                    if mid in seen_ids:
                        continue
                    seen_ids.add(mid)
                    from_recent += 1
                    items.append(
                        {
                            "id": mid,
                            "content": row["content"] or "",
                            "memory_type": row["memory_type"] or "",
                            "tags": parse_json_list(row["tags"]),
                        }
                    )

            elapsed_ms = int((time.perf_counter() - started) * 1000)
            if isinstance(debug, dict):
                try:
                    total = len(items[:limit])
                    from_fts = total - int(from_recent)
                    hit_rate = round(float(from_fts) / float(limit), 3) if limit else 0.0
                    debug.update(
                        {
                            "fts_used": bool(fts_used),
                            "fts_available": bool(fts_available),
                            "fts_hits": int(fts_hits),
                            "fts_hit_rate": hit_rate,
                            "fts_time_ms": int(fts_time_ms) if fts_time_ms is not None else None,
                            "recent_time_ms": int(recent_time_ms)
                            if recent_time_ms is not None
                            else None,
                            "elapsed_ms": int(elapsed_ms),
                            "total": int(total),
                        }
                    )
                    if fts_query:
                        debug["fts_query"] = fts_query[:120]
                except Exception as exc:
                    debug["metrics_error"] = str(exc)

            selected = items[:limit]
            # P1：知识库质量信号 re-rank（仅对候选列表做轻量重排，避免完全覆盖 FTS 相关性）
            try:
                limit_value = int(limit)
            except Exception:
                limit_value = 0
            if limit_value > 0 and selected:
                try:
                    days = int(AGENT_KNOWLEDGE_RERANK_RECENT_DAYS)
                except Exception:
                    days = 30
                since = None
                if days > 0:
                    try:
                        now_dt = datetime.fromisoformat(now_iso().replace("Z", "+00:00"))
                        since_dt = now_dt - timedelta(days=int(days))
                        since = since_dt.isoformat().replace("+00:00", "Z")
                    except Exception:
                        since = None

                skill_ids = [int(it.get("id") or 0) for it in selected if it.get("id") is not None]
                stats_map = get_skill_reuse_quality_map(skill_ids=skill_ids, since=since)

                n = len(selected)

                def _rerank_key(pair):
                    idx, it = pair
                    sid = int(it.get("id") or 0)
                    stats = stats_map.get(sid) or {}
                    calls = int(stats.get("calls") or 0)
                    reuse_calls = int(stats.get("reuse_calls") or 0)
                    pass_calls = int(stats.get("pass_calls") or 0)
                    fail_calls = int(stats.get("fail_calls") or 0)
                    denom = pass_calls + fail_calls
                    success_rate = (pass_calls / denom) if denom else 0.0
                    base = ((n - idx) / n) if n else 0.0
                    reuse_bonus = min(1.0, reuse_calls / 10.0) if reuse_calls > 0 else 0.0
                    score = base * 0.7 + success_rate * 0.2 + reuse_bonus * 0.1
                    return (score, base, success_rate, reuse_calls, calls, sid)

                selected = [
                    it for _idx, it in sorted(enumerate(selected), key=_rerank_key, reverse=True)
                ]
            return selected
    except Exception as exc:
        if isinstance(debug, dict):
            debug["error"] = str(exc)
        return []


def list_graph_candidates(
    *,
    terms: List[str],
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
) -> List[dict]:
    """
    读取图谱节点候选集：
    - 先按关键字 LIKE 搜索（命中更准）
    - 再补充最近节点（避免无命中时完全为空）
    """
    items: List[dict] = []
    seen_ids = set()
    try:
        with provide_connection(conn) as inner:
            if terms:
                conds = []
                params: List[str] = []
                for term in terms:
                    conds.append("(label LIKE ? OR node_type LIKE ?)")
                    pattern = f"%{term}%"
                    params.extend([pattern, pattern])
                where_clause = " OR ".join(conds)
                rows = inner.execute(
                    f"SELECT id, label, node_type, evidence FROM graph_nodes WHERE {where_clause} ORDER BY id DESC LIMIT ?",
                    (*params, int(limit)),
                ).fetchall()
                for row in rows:
                    gid = int(row["id"])
                    if gid in seen_ids:
                        continue
                    seen_ids.add(gid)
                    items.append(
                        {
                            "id": gid,
                            "label": row["label"] or "",
                            "node_type": row["node_type"] or "",
                            "evidence": row["evidence"] or "",
                        }
                    )
            if len(items) < limit:
                rows = inner.execute(
                    "SELECT id, label, node_type, evidence FROM graph_nodes ORDER BY id DESC LIMIT ?",
                    (int(limit),),
                ).fetchall()
                for row in rows:
                    gid = int(row["id"])
                    if gid in seen_ids:
                        continue
                    seen_ids.add(gid)
                    items.append(
                        {
                            "id": gid,
                            "label": row["label"] or "",
                            "node_type": row["node_type"] or "",
                            "evidence": row["evidence"] or "",
                        }
                    )
                    if len(items) >= limit:
                        break
    except Exception:
        return []
    return items[:limit]


def load_graph_nodes_by_ids(
    node_ids: List[int],
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> List[dict]:
    if not node_ids:
        return []
    placeholders = ",".join(["?"] * len(node_ids))
    try:
        with provide_connection(conn) as inner:
            rows = inner.execute(
                f"SELECT id, label, node_type, evidence FROM graph_nodes WHERE id IN ({placeholders})",
                node_ids,
            ).fetchall()
        by_id = {int(row["id"]): row for row in rows}
        ordered: List[dict] = []
        for nid in node_ids:
            row = by_id.get(int(nid))
            if not row:
                continue
            ordered.append(
                {
                    "id": int(row["id"]),
                    "label": row["label"] or "",
                    "node_type": row["node_type"] or "",
                    "evidence": row["evidence"] or "",
                }
            )
        return ordered
    except Exception:
        return []


def load_graph_edges_between(
    *,
    node_ids: List[int],
    limit: int = 24,
    conn: Optional[sqlite3.Connection] = None,
) -> List[dict]:
    """
    只加载"候选节点集合内部"的边（避免边无限膨胀）。
    """
    if not node_ids:
        return []
    placeholders = ",".join(["?"] * len(node_ids))
    params = list(node_ids) + list(node_ids)
    try:
        with provide_connection(conn) as inner:
            rows = inner.execute(
                f"SELECT id, source, target, relation, confidence, evidence FROM graph_edges "
                f"WHERE source IN ({placeholders}) AND target IN ({placeholders}) "
                "ORDER BY id DESC LIMIT ?",
                (*params, int(limit)),
            ).fetchall()
        edges: List[dict] = []
        for row in rows:
            edges.append(
                {
                    "id": int(row["id"]),
                    "source": int(row["source"]),
                    "target": int(row["target"]),
                    "relation": row["relation"] or "",
                    "confidence": row["confidence"],
                    "evidence": row["evidence"] or "",
                }
            )
        return edges
    except Exception:
        return []


def list_domain_candidates(
    *,
    limit: int = 20,
    conn: Optional[sqlite3.Connection] = None,
) -> List[dict]:
    """
    读取领域候选集（用于 LLM 筛选目标领域）。
    只返回 active 状态的领域。
    """
    try:
        with provide_connection(conn) as inner:
            rows = inner.execute(
                "SELECT domain_id, name, parent_id, description, keywords, skill_count "
                "FROM domains WHERE status = 'active' ORDER BY skill_count DESC, domain_id ASC LIMIT ?",
                (int(limit),),
            ).fetchall()
        items: List[dict] = []
        for row in rows:
            items.append(
                {
                    "domain_id": row["domain_id"],
                    "name": row["name"],
                    "parent_id": row["parent_id"],
                    "description": row["description"] or "",
                    "keywords": parse_json_list(row["keywords"]),
                    "skill_count": int(row["skill_count"]) if row["skill_count"] else 0,
                }
            )
        return items
    except Exception:
        return []


def list_skill_candidates_by_domains(
    *,
    domain_ids: List[str],
    limit: int,
    query_text: Optional[str] = None,
    debug: Optional[dict] = None,
    include_draft: bool = False,
    skill_type: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> List[dict]:
    """
    读取指定领域内的技能候选集（用于 LLM 二次筛选）。
    - 相关性优先：FTS5
    - 按领域前缀筛选（支持 data 匹配 data.collect, data.clean 等）
    - 默认只返回 approved 状态的技能（include_draft=True 时包含 draft）
    """
    if not domain_ids:
        return list_skill_candidates(
            limit=limit,
            query_text=query_text,
            debug=debug,
            include_draft=include_draft,
            skill_type=skill_type,
            conn=conn,
        )

    try:
        with provide_connection(conn) as inner:
            items: List[dict] = []
            seen_ids = set()

            # skill_type 过滤（兼容旧数据：NULL 视为 methodology）
            skill_type_value = str(skill_type or "").strip().lower()
            if skill_type_value == "methodology":
                skill_type_condition = " AND (s.skill_type = 'methodology' OR s.skill_type IS NULL)"
                skill_type_params: List[str] = []
                recent_skill_type_condition = " AND (skill_type = 'methodology' OR skill_type IS NULL)"
                recent_skill_type_params: List[str] = []
            elif skill_type_value == "solution":
                skill_type_condition = " AND s.skill_type = 'solution'"
                skill_type_params = []
                recent_skill_type_condition = " AND skill_type = 'solution'"
                recent_skill_type_params = []
            elif skill_type_value:
                skill_type_condition = " AND s.skill_type = ?"
                skill_type_params = [skill_type_value]
                recent_skill_type_condition = " AND skill_type = ?"
                recent_skill_type_params = [skill_type_value]
            else:
                skill_type_condition = ""
                skill_type_params = []
                recent_skill_type_condition = ""
                recent_skill_type_params = []

            # 状态过滤条件
            if include_draft:
                status_condition = "s.status IN ('approved', 'draft') OR s.status IS NULL"
            else:
                status_condition = "(s.status = 'approved' OR s.status IS NULL)"

            started = time.perf_counter()
            fts_query = build_fts_or_query(query_text, limit=12) if query_text else ""
            fts_available = bool(fts_query) and fts_table_exists(inner, "skills_items_fts")
            fts_used = False
            fts_hits = 0
            fts_time_ms: Optional[int] = None
            recent_time_ms: Optional[int] = None
            from_recent = 0

            # 构建领域前缀匹配条件（支持父领域匹配子领域）
            domain_conditions = []
            domain_params: List[str] = []
            for domain_id in domain_ids:
                domain_conditions.append("(s.domain_id = ? OR s.domain_id LIKE ?)")
                domain_params.append(domain_id)
                domain_params.append(f"{domain_id}.%")
            domain_where = f"({' OR '.join(domain_conditions)})"

            # 1) 相关性召回：优先用 FTS5
            if fts_available:
                fts_used = True
                t0 = time.perf_counter()
                sql = f"""
                    SELECT s.id, s.name, s.description, s.scope, s.category, s.tags, s.domain_id
                    FROM skills_items_fts f
                    JOIN skills_items s ON s.id = f.rowid
                    WHERE skills_items_fts MATCH ? AND {domain_where} AND ({status_condition}){skill_type_condition}
                    ORDER BY bm25(skills_items_fts) ASC, s.id DESC
                    LIMIT ?
                """
                rows = inner.execute(sql, (fts_query, *domain_params, *skill_type_params, int(limit))).fetchall()
                fts_time_ms = int((time.perf_counter() - t0) * 1000)
                fts_hits = len(rows)
                for row in rows:
                    sid = int(row["id"])
                    if sid in seen_ids:
                        continue
                    seen_ids.add(sid)
                    items.append(
                        {
                            "id": sid,
                            "name": row["name"],
                            "description": row["description"] or "",
                            "scope": row["scope"] or "",
                            "category": row["category"] or "",
                            "tags": parse_json_list(row["tags"]),
                            "domain_id": row["domain_id"] or "",
                        }
                    )

            # 2) 新近补齐：避免 FTS 未命中导致候选为空
            if len(items) < limit:
                t0 = time.perf_counter()
                sql = f"""
                    SELECT id, name, description, scope, category, tags, domain_id
                    FROM skills_items s
                    WHERE {domain_where} AND ({status_condition}){recent_skill_type_condition}
                    ORDER BY id DESC LIMIT ?
                """
                rows = inner.execute(sql, (*domain_params, *recent_skill_type_params, int(limit))).fetchall()
                recent_time_ms = int((time.perf_counter() - t0) * 1000)
                for row in rows:
                    sid = int(row["id"])
                    if sid in seen_ids:
                        continue
                    seen_ids.add(sid)
                    from_recent += 1
                    items.append(
                        {
                            "id": sid,
                            "name": row["name"],
                            "description": row["description"] or "",
                            "scope": row["scope"] or "",
                            "category": row["category"] or "",
                            "tags": parse_json_list(row["tags"]),
                            "domain_id": row["domain_id"] or "",
                        }
                    )

            elapsed_ms = int((time.perf_counter() - started) * 1000)
            if isinstance(debug, dict):
                try:
                    total = len(items[:limit])
                    from_fts = total - int(from_recent)
                    hit_rate = round(float(from_fts) / float(limit), 3) if limit else 0.0
                    debug.update(
                        {
                            "fts_used": bool(fts_used),
                            "fts_available": bool(fts_available),
                            "fts_hits": int(fts_hits),
                            "fts_hit_rate": hit_rate,
                            "fts_time_ms": int(fts_time_ms) if fts_time_ms is not None else None,
                            "recent_time_ms": int(recent_time_ms)
                            if recent_time_ms is not None
                            else None,
                            "elapsed_ms": int(elapsed_ms),
                            "total": int(total),
                            "domain_filter": domain_ids,
                            "include_draft": include_draft,
                            "skill_type": skill_type_value or None,
                        }
                    )
                    if fts_query:
                        debug["fts_query"] = fts_query[:120]
                except Exception as exc:
                    debug["metrics_error"] = str(exc)

            return items[:limit]
    except Exception as exc:
        if isinstance(debug, dict):
            debug["error"] = str(exc)
        return []
