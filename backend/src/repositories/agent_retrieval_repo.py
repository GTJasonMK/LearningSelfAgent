from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

from backend.src.common.serializers import skill_content_from_row
from backend.src.common.sql import in_clause_placeholders
from backend.src.common.utils import coerce_int, now_iso, parse_json_list, tool_approval_status
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
    days = coerce_int(AGENT_KNOWLEDGE_RERANK_RECENT_DAYS, default=30)
    if days <= 0:
        return None
    try:
        now_dt = datetime.fromisoformat(now_iso().replace("Z", "+00:00"))
        since_dt = now_dt - timedelta(days=days)
        return since_dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def _skill_candidate_from_row(row, *, include_domain: bool = False) -> dict:
    item = {
        "id": int(row["id"]),
        "name": row["name"],
        "description": row["description"] or "",
        "scope": row["scope"] or "",
        "category": row["category"] or "",
        "tags": parse_json_list(row["tags"]),
    }
    if include_domain:
        item["domain_id"] = row["domain_id"] or ""
    return item


def _memory_candidate_from_row(row) -> dict:
    return {
        "id": int(row["id"]),
        "content": row["content"] or "",
        "memory_type": row["memory_type"] or "",
        "tags": parse_json_list(row["tags"]),
    }


def _approved_tool_hint_from_row(row) -> Optional[dict]:
    status = tool_approval_status(row["metadata"], approval_key=TOOL_METADATA_APPROVAL_KEY)
    if status in {TOOL_APPROVAL_STATUS_DRAFT, TOOL_APPROVAL_STATUS_REJECTED}:
        return None
    return {
        "id": int(row["id"]),
        "name": row["name"],
        "description": row["description"] or "",
    }


def _graph_candidate_from_row(row) -> dict:
    return {
        "id": int(row["id"]),
        "label": row["label"] or "",
        "node_type": row["node_type"] or "",
        "evidence": row["evidence"] or "",
    }


def _resolve_limit(value: Any, *, default: int) -> int:
    limit_value = coerce_int(value, default=default)
    if limit_value <= 0:
        return coerce_int(default, default=0)
    return limit_value


def _coerce_float(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _positive_id(value: Any) -> Optional[int]:
    parsed = coerce_int(value, default=0)
    return parsed if parsed > 0 else None


def _column(name: str, *, alias: str) -> str:
    return f"{alias}.{name}" if alias else name


def _skill_status_condition(
    *,
    include_draft: bool,
    alias: str,
    include_legacy_null_when_draft: bool,
) -> str:
    status_col = _column("status", alias=alias)
    if include_draft:
        if include_legacy_null_when_draft:
            return f"{status_col} IN ('approved', 'draft') OR {status_col} IS NULL"
        return f"{status_col} IN ('approved', 'draft')"
    return f"{status_col} = 'approved' OR {status_col} IS NULL"


def _skill_type_filter(
    skill_type: Optional[str],
    *,
    alias: str,
) -> tuple[str, List[str], str]:
    """
    统一生成 skill_type 过滤 SQL 片段。
    """
    skill_type_value = str(skill_type or "").strip().lower()
    skill_type_col = _column("skill_type", alias=alias)
    if skill_type_value == "methodology":
        return f" AND ({skill_type_col} = 'methodology' OR {skill_type_col} IS NULL)", [], skill_type_value
    if skill_type_value == "solution":
        return f" AND {skill_type_col} = 'solution'", [], skill_type_value
    if skill_type_value:
        return f" AND {skill_type_col} = ?", [skill_type_value], skill_type_value
    return "", [], skill_type_value


def _append_unique_candidates(
    *,
    rows: List[Any],
    seen_ids: set,
    items: List[dict],
    build_item: Callable[[Any], dict],
) -> int:
    added = 0
    for row in rows or []:
        item_id = _positive_id(row["id"])
        if item_id is None or item_id in seen_ids:
            continue
        seen_ids.add(item_id)
        items.append(build_item(row))
        added += 1
    return added


def _update_retrieval_debug(
    *,
    debug: Optional[dict],
    limit: int,
    items: List[dict],
    from_recent: int,
    fts_used: bool,
    fts_available: bool,
    fts_hits: int,
    fts_time_ms: Optional[int],
    recent_time_ms: Optional[int],
    elapsed_ms: int,
    fts_query: str,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    if not isinstance(debug, dict):
        return
    try:
        total = len(items[:limit])
        from_fts = total - coerce_int(from_recent, default=0)
        hit_rate = round(float(from_fts) / float(limit), 3) if limit else 0.0
        payload: Dict[str, Any] = {
            "fts_used": bool(fts_used),
            "fts_available": bool(fts_available),
            "fts_hits": coerce_int(fts_hits, default=0),
            "fts_hit_rate": hit_rate,
            "fts_time_ms": coerce_int(fts_time_ms, default=0) if fts_time_ms is not None else None,
            "recent_time_ms": coerce_int(recent_time_ms, default=0) if recent_time_ms is not None else None,
            "elapsed_ms": coerce_int(elapsed_ms, default=0),
            "total": coerce_int(total, default=0),
        }
        if extra:
            payload.update(extra)
        debug.update(payload)
        if fts_query:
            debug["fts_query"] = fts_query[:120]
    except Exception as exc:
        debug["metrics_error"] = str(exc)


def _rerank_by_skill_quality(
    *,
    selected: List[dict],
    since: Optional[str],
    reuse_cap: int,
    weight_base: float,
    weight_success: float,
    weight_reuse: float,
) -> List[dict]:
    if not selected:
        return []
    cap = max(coerce_int(reuse_cap, default=1), 1)
    stats_map = get_skill_reuse_quality_map(
        skill_ids=[coerce_int(it.get("id") or 0, default=0) for it in selected if it.get("id") is not None],
        since=since,
    )
    total = len(selected)

    def _rerank_key(pair):
        idx, it = pair
        sid = coerce_int(it.get("id") or 0, default=0)
        stats = stats_map.get(sid) or {}
        calls = coerce_int(stats.get("calls") or 0, default=0)
        reuse_calls = coerce_int(stats.get("reuse_calls") or 0, default=0)
        pass_calls = coerce_int(stats.get("pass_calls") or 0, default=0)
        fail_calls = coerce_int(stats.get("fail_calls") or 0, default=0)
        denom = pass_calls + fail_calls
        success_rate = (pass_calls / denom) if denom else 0.0
        base = ((total - idx) / total) if total else 0.0
        reuse_bonus = min(1.0, reuse_calls / float(cap)) if reuse_calls > 0 else 0.0
        score = base * weight_base + success_rate * weight_success + reuse_bonus * weight_reuse
        return (score, base, success_rate, reuse_calls, calls, sid)

    return [it for _idx, it in sorted(enumerate(selected), key=_rerank_key, reverse=True)]


def _collect_unique_texts(values: List[str], *, limit: int) -> List[str]:
    items: List[str] = []
    seen = set()
    limit_value = _resolve_limit(limit, default=8)
    for value in values or []:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        items.append(text)
        if len(items) >= limit_value:
            break
    return items


def _quality_sort_tuple(stats: dict, *, entity_id: int) -> tuple:
    calls = coerce_int(stats.get("calls") or 0, default=0)
    reuse_calls = coerce_int(stats.get("reuse_calls") or 0, default=0)
    pass_calls = coerce_int(stats.get("pass_calls") or 0, default=0)
    fail_calls = coerce_int(stats.get("fail_calls") or 0, default=0)
    denom = pass_calls + fail_calls
    success_rate = (pass_calls / denom) if denom else 0.0
    return (success_rate, reuse_calls, calls, coerce_int(entity_id, default=0))


def _resolve_rerank_weights() -> tuple[int, float, float, float]:
    reuse_cap = _resolve_limit(AGENT_KNOWLEDGE_RERANK_REUSE_CALLS_CAP, default=10)
    weight_base = _coerce_float(AGENT_KNOWLEDGE_RERANK_WEIGHT_BASE, default=0.7)
    weight_success = _coerce_float(AGENT_KNOWLEDGE_RERANK_WEIGHT_SUCCESS, default=0.2)
    weight_reuse = _coerce_float(AGENT_KNOWLEDGE_RERANK_WEIGHT_REUSE, default=0.1)
    return reuse_cap, weight_base, weight_success, weight_reuse


def list_tool_hints(*, limit: int = 8, conn: Optional[sqlite3.Connection] = None) -> List[dict]:
    """
    给 Agent 提供“可用工具清单”的最小字段集合（用于拼 prompt）。
    """
    with provide_connection(conn) as inner:
        # 过滤掉未批准（draft/rejected）的工具，避免 Agent 在规划阶段复用“未验证的新工具”。
        # 说明：未写 approval 字段的历史工具默认视为 approved。
        rows = inner.execute(
            "SELECT id, name, description, metadata FROM tools_items ORDER BY id ASC LIMIT ?",
            (max(coerce_int(limit, default=8) * 4, 16),),
        ).fetchall()
    items: List[dict] = []
    for row in rows:
        item = _approved_tool_hint_from_row(row)
        if not item:
            continue
        items.append(item)

    # re-rank：融入“被复用次数/最近成功率”（P1：知识库质量）
    limit_value = _resolve_limit(limit, default=8)
    if not items:
        return []

    since = _resolve_rerank_since()

    tool_ids = [coerce_int(it["id"], default=0) for it in items if it.get("id") is not None]
    stats_map = get_tool_reuse_quality_map(tool_ids=tool_ids, since=since)

    def _key(it: dict):
        tid = coerce_int(it.get("id") or 0, default=0)
        stats = stats_map.get(tid) or {}
        return _quality_sort_tuple(stats, entity_id=tid)

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
    limit_value = _resolve_limit(limit, default=8)
    raw_names = _collect_unique_texts(names or [], limit=max(limit_value * 4, 32))

    if not raw_names:
        return []

    placeholders = in_clause_placeholders(raw_names)
    if not placeholders:
        return []
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
        item = _approved_tool_hint_from_row(row)
        if not item:
            continue
        items.append(item)
        if len(items) >= limit_value:
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
            limit_value = _resolve_limit(limit, default=8)

            skill_type_condition, skill_type_params, skill_type_value = _skill_type_filter(
                skill_type,
                alias="s",
            )
            recent_skill_type_condition, recent_skill_type_params, _ = _skill_type_filter(
                skill_type,
                alias="",
            )
            status_condition = _skill_status_condition(
                include_draft=include_draft,
                alias="s",
                include_legacy_null_when_draft=False,
            )
            recent_status_condition = _skill_status_condition(
                include_draft=include_draft,
                alias="",
                include_legacy_null_when_draft=True,
            )

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
                        WHERE skills_items_fts MATCH ? AND ({status_condition}){skill_type_condition}
                        ORDER BY bm25(skills_items_fts) ASC, s.id DESC
                        LIMIT ?
                        """,
                    (fts_query, *skill_type_params, limit_value),
                ).fetchall()
                fts_time_ms = int((time.perf_counter() - t0) * 1000)
                fts_hits = len(rows)
                _append_unique_candidates(
                    rows=rows,
                    seen_ids=seen_ids,
                    items=items,
                    build_item=lambda row: _skill_candidate_from_row(row),
                )

            # 2) 新近补齐：为了避免 FTS 未命中导致"技能候选为空"，补一批最新技能
            if len(items) < limit_value:
                t0 = time.perf_counter()
                rows = inner.execute(
                    f"SELECT id, name, description, scope, category, tags FROM skills_items s WHERE ({recent_status_condition}){recent_skill_type_condition} ORDER BY id DESC LIMIT ?",
                    (*recent_skill_type_params, limit_value),
                ).fetchall()
                recent_time_ms = int((time.perf_counter() - t0) * 1000)
                from_recent += _append_unique_candidates(
                    rows=rows,
                    seen_ids=seen_ids,
                    items=items,
                    build_item=lambda row: _skill_candidate_from_row(row),
                )

            elapsed_ms = int((time.perf_counter() - started) * 1000)
            _update_retrieval_debug(
                debug=debug,
                limit=limit_value,
                items=items,
                from_recent=from_recent,
                fts_used=fts_used,
                fts_available=fts_available,
                fts_hits=fts_hits,
                fts_time_ms=fts_time_ms,
                recent_time_ms=recent_time_ms,
                elapsed_ms=elapsed_ms,
                fts_query=fts_query,
                extra={
                    "include_draft": include_draft,
                    "skill_type": skill_type_value or None,
                },
            )

            selected = items[:limit_value]
            if selected:
                reuse_cap, weight_base, weight_success, weight_reuse = _resolve_rerank_weights()
                selected = _rerank_by_skill_quality(
                    selected=selected,
                    since=_resolve_rerank_since(),
                    reuse_cap=reuse_cap,
                    weight_base=weight_base,
                    weight_success=weight_success,
                    weight_reuse=weight_reuse,
                )
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
    raw_tags = _collect_unique_texts(skill_tags or [], limit=32)

    if not raw_tags:
        return []

    limit_value = _resolve_limit(limit, default=30)

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
            approved_status_condition = _skill_status_condition(
                include_draft=False,
                alias="",
                include_legacy_null_when_draft=False,
            )
            sql = (
                "SELECT id, name, description, scope, category, tags, domain_id "
                "FROM skills_items s "
                f"WHERE {domain_where}({approved_status_condition}) "
                "AND s.skill_type = 'solution' "
                f"AND {tag_where} "
                "ORDER BY id DESC LIMIT ?"
            )
            rows = inner.execute(sql, (*domain_params, *tag_params, limit_value)).fetchall()

        items: List[dict] = []
        for row in rows or []:
            items.append(_skill_candidate_from_row(row, include_domain=True))

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
                skill_ids=[coerce_int(it.get("id") or 0, default=0) for it in items if it.get("id") is not None],
                since=since,
            )

            def _key(it: dict):
                sid = coerce_int(it.get("id") or 0, default=0)
                stats = stats_map.get(sid) or {}
                return _quality_sort_tuple(stats, entity_id=sid)

            items = sorted(items, key=_key, reverse=True)

        return items[:limit_value]
    except Exception as exc:
        if isinstance(debug, dict):
            debug["tag_match_error"] = str(exc)
        return []


def load_skills_by_ids(skill_ids: List[int], *, conn: Optional[sqlite3.Connection] = None) -> List[dict]:
    if not skill_ids:
        return []
    placeholders = in_clause_placeholders(skill_ids)
    if not placeholders:
        return []
    try:
        with provide_connection(conn) as inner:
            rows = inner.execute(
                f"SELECT * FROM skills_items WHERE id IN ({placeholders})",
                skill_ids,
            ).fetchall()
        by_id = {}
        for row in rows:
            row_id = _positive_id(row["id"])
            if row_id is None:
                continue
            by_id[row_id] = row
        ordered = []
        for sid in skill_ids:
            sid_value = _positive_id(sid)
            if sid_value is None:
                continue
            row = by_id.get(sid_value)
            if not row:
                continue
            ordered.append({"id": sid_value, **skill_content_from_row(row)})
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
            limit_value = _resolve_limit(limit, default=8)

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
                    (fts_query, limit_value),
                ).fetchall()
                fts_time_ms = int((time.perf_counter() - t0) * 1000)
                fts_hits = len(rows)
                _append_unique_candidates(
                    rows=rows,
                    seen_ids=seen_ids,
                    items=items,
                    build_item=lambda row: _memory_candidate_from_row(row),
                )

            # 2) 新近补齐：避免 FTS 未命中导致候选为空，同时保留“最近上下文”的价值
            if len(items) < limit_value:
                t0 = time.perf_counter()
                rows = inner.execute(
                    "SELECT id, content, memory_type, tags FROM memory_items ORDER BY id DESC LIMIT ?",
                    (limit_value,),
                ).fetchall()
                recent_time_ms = int((time.perf_counter() - t0) * 1000)
                from_recent += _append_unique_candidates(
                    rows=rows,
                    seen_ids=seen_ids,
                    items=items,
                    build_item=lambda row: _memory_candidate_from_row(row),
                )

            elapsed_ms = int((time.perf_counter() - started) * 1000)
            _update_retrieval_debug(
                debug=debug,
                limit=limit_value,
                items=items,
                from_recent=from_recent,
                fts_used=fts_used,
                fts_available=fts_available,
                fts_hits=fts_hits,
                fts_time_ms=fts_time_ms,
                recent_time_ms=recent_time_ms,
                elapsed_ms=elapsed_ms,
                fts_query=fts_query,
            )

            selected = items[:limit_value]
            if selected:
                selected = _rerank_by_skill_quality(
                    selected=selected,
                    since=_resolve_rerank_since(),
                    reuse_cap=10,
                    weight_base=0.7,
                    weight_success=0.2,
                    weight_reuse=0.1,
                )
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
    limit_value = _resolve_limit(limit, default=20)
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
                    (*params, limit_value),
                ).fetchall()
                _append_unique_candidates(
                    rows=rows,
                    seen_ids=seen_ids,
                    items=items,
                    build_item=lambda row: _graph_candidate_from_row(row),
                )
            if len(items) < limit_value:
                rows = inner.execute(
                    "SELECT id, label, node_type, evidence FROM graph_nodes ORDER BY id DESC LIMIT ?",
                    (limit_value,),
                ).fetchall()
                _append_unique_candidates(
                    rows=rows,
                    seen_ids=seen_ids,
                    items=items,
                    build_item=lambda row: _graph_candidate_from_row(row),
                )
    except Exception:
        return []
    return items[:limit_value]


def load_graph_nodes_by_ids(
    node_ids: List[int],
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> List[dict]:
    if not node_ids:
        return []
    placeholders = in_clause_placeholders(node_ids)
    if not placeholders:
        return []
    try:
        with provide_connection(conn) as inner:
            rows = inner.execute(
                f"SELECT id, label, node_type, evidence FROM graph_nodes WHERE id IN ({placeholders})",
                node_ids,
            ).fetchall()
        by_id = {}
        for row in rows:
            row_id = _positive_id(row["id"])
            if row_id is None:
                continue
            by_id[row_id] = row
        ordered: List[dict] = []
        for nid in node_ids:
            nid_value = _positive_id(nid)
            if nid_value is None:
                continue
            row = by_id.get(nid_value)
            if not row:
                continue
            ordered.append(_graph_candidate_from_row(row))
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
    placeholders = in_clause_placeholders(node_ids)
    if not placeholders:
        return []
    params = list(node_ids) + list(node_ids)
    limit_value = _resolve_limit(limit, default=24)
    try:
        with provide_connection(conn) as inner:
            rows = inner.execute(
                f"SELECT id, source, target, relation, confidence, evidence FROM graph_edges "
                f"WHERE source IN ({placeholders}) AND target IN ({placeholders}) "
                "ORDER BY id DESC LIMIT ?",
                (*params, limit_value),
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
    limit_value = _resolve_limit(limit, default=20)
    try:
        with provide_connection(conn) as inner:
            rows = inner.execute(
                "SELECT domain_id, name, parent_id, description, keywords, skill_count "
                "FROM domains WHERE status = 'active' ORDER BY skill_count DESC, domain_id ASC LIMIT ?",
                (limit_value,),
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
                    "skill_count": coerce_int(row["skill_count"], default=0),
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
            limit_value = _resolve_limit(limit, default=8)
            skill_type_condition, skill_type_params, skill_type_value = _skill_type_filter(
                skill_type,
                alias="s",
            )
            recent_skill_type_condition, recent_skill_type_params, _ = _skill_type_filter(
                skill_type,
                alias="",
            )
            status_condition = _skill_status_condition(
                include_draft=include_draft,
                alias="s",
                include_legacy_null_when_draft=True,
            )
            recent_status_condition = _skill_status_condition(
                include_draft=include_draft,
                alias="",
                include_legacy_null_when_draft=True,
            )

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
                rows = inner.execute(
                    sql,
                    (fts_query, *domain_params, *skill_type_params, limit_value),
                ).fetchall()
                fts_time_ms = int((time.perf_counter() - t0) * 1000)
                fts_hits = len(rows)
                _append_unique_candidates(
                    rows=rows,
                    seen_ids=seen_ids,
                    items=items,
                    build_item=lambda row: _skill_candidate_from_row(row, include_domain=True),
                )

            # 2) 新近补齐：避免 FTS 未命中导致候选为空
            if len(items) < limit_value:
                t0 = time.perf_counter()
                sql = f"""
                    SELECT id, name, description, scope, category, tags, domain_id
                    FROM skills_items s
                    WHERE {domain_where} AND ({recent_status_condition}){recent_skill_type_condition}
                    ORDER BY id DESC LIMIT ?
                """
                rows = inner.execute(
                    sql,
                    (*domain_params, *recent_skill_type_params, limit_value),
                ).fetchall()
                recent_time_ms = int((time.perf_counter() - t0) * 1000)
                from_recent += _append_unique_candidates(
                    rows=rows,
                    seen_ids=seen_ids,
                    items=items,
                    build_item=lambda row: _skill_candidate_from_row(row, include_domain=True),
                )

            elapsed_ms = int((time.perf_counter() - started) * 1000)
            _update_retrieval_debug(
                debug=debug,
                limit=limit_value,
                items=items,
                from_recent=from_recent,
                fts_used=fts_used,
                fts_available=fts_available,
                fts_hits=fts_hits,
                fts_time_ms=fts_time_ms,
                recent_time_ms=recent_time_ms,
                elapsed_ms=elapsed_ms,
                fts_query=fts_query,
                extra={
                    "domain_filter": domain_ids,
                    "include_draft": include_draft,
                    "skill_type": skill_type_value or None,
                },
            )

            return items[:limit_value]
    except Exception as exc:
        if isinstance(debug, dict):
            debug["error"] = str(exc)
        return []
