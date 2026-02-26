from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Optional

from backend.src.common.utils import coerce_int, now_iso, parse_json_dict
from backend.src.constants import TOOL_APPROVAL_STATUS_REJECTED
from backend.src.repositories.skills_repo import update_skill_status
from backend.src.repositories.tool_call_records_repo import (
    get_skill_reuse_quality_map,
    get_tool_reuse_quality_map,
)
from backend.src.repositories.tools_repo import get_tool
from backend.src.services.knowledge.governance.helpers import (
    _build_rejected_tool_meta,
    _collect_distinct_positive_ids,
    _extract_tool_approval,
    _tool_current_approval_status,
    _update_tool_meta_and_publish,
)
from backend.src.services.skills.skills_publish import publish_skill_file
from backend.src.storage import get_connection


def auto_deprecate_low_quality_knowledge(
    *,
    since_days: int = 30,
    min_calls: int = 3,
    success_rate_threshold: float = 0.3,
    dry_run: bool = False,
    include_skills: bool = True,
    include_tools: bool = True,
    reason: Optional[str] = None,
) -> dict:
    """
    过期机制（质量驱动）：按“最近 success_rate + 最小样本量”自动废弃低质量知识。

    判定信号：
    - tool_call_records.reuse_status in (pass/fail) 的统计（unknown/NULL 不计入成功率分母）
    - 仅当 (pass+fail) >= min_calls 且 success_rate < 阈值 才会触发降级

    动作：
    - skills_items（status=approved）-> deprecated
    - tools_items（approval.status=approved）-> rejected
    """
    days = coerce_int(since_days, default=30)
    if days <= 0:
        days = 30

    min_calls_value = coerce_int(min_calls, default=3)
    if min_calls_value < 1:
        min_calls_value = 1

    try:
        threshold = float(success_rate_threshold)
    except Exception:
        threshold = 0.3
    if threshold < 0:
        threshold = 0.0
    if threshold > 1:
        threshold = 1.0

    now_value = now_iso()
    reason_text = str(reason or "").strip() or "maintenance_auto_deprecate"

    # since：ISO string（与 tool_call_records.created_at 字段一致）
    since = None
    try:
        now_dt = datetime.fromisoformat(now_value.replace("Z", "+00:00"))
        since_dt = now_dt - timedelta(days=days)
        since = since_dt.isoformat().replace("+00:00", "Z")
    except Exception:
        since = None

    skill_actions: List[dict] = []
    tool_actions: List[dict] = []
    errors: List[str] = []

    changed_skill_ids: List[int] = []
    changed_tool_ids: List[int] = []

    with get_connection() as conn:
        if include_skills:
            # 仅对“存在复用验证信号”的 skill_id 做治理，避免误伤纯文本技能。
            rows = conn.execute(
                "SELECT DISTINCT skill_id FROM tool_call_records WHERE skill_id IS NOT NULL"
                + (" AND created_at >= ?" if since else ""),
                (since,) if since else (),
            ).fetchall()
            skill_ids = _collect_distinct_positive_ids(rows, key="skill_id", limit=2000)

            stats_map = get_skill_reuse_quality_map(skill_ids=skill_ids, since=since, conn=conn)
            for sid in skill_ids:
                sid_value = coerce_int(sid, default=0)
                if sid_value <= 0:
                    continue
                stats = stats_map.get(sid_value) or {}
                pass_calls = coerce_int(stats.get("pass_calls"), default=0)
                fail_calls = coerce_int(stats.get("fail_calls"), default=0)
                denom = pass_calls + fail_calls
                if denom < min_calls_value:
                    continue
                success_rate = float(pass_calls) / float(denom) if denom else 0.0
                if success_rate >= threshold:
                    continue

                row = conn.execute(
                    "SELECT id, name, status FROM skills_items WHERE id = ? LIMIT 1",
                    (sid_value,),
                ).fetchone()
                if not row:
                    continue
                current_status = str(row["status"] or "approved").strip().lower() or "approved"
                if current_status != "approved":
                    continue

                skill_actions.append(
                    {
                        "skill_id": sid_value,
                        "name": str(row["name"] or ""),
                        "from_status": current_status,
                        "to_status": "deprecated",
                        "pass_calls": pass_calls,
                        "fail_calls": fail_calls,
                        "success_rate": round(success_rate, 4),
                    }
                )
                if dry_run:
                    changed_skill_ids.append(sid_value)
                    continue
                try:
                    updated = update_skill_status(skill_id=sid_value, status="deprecated", conn=conn)
                    if updated is not None:
                        changed_skill_ids.append(sid_value)
                except Exception as exc:
                    errors.append(f"skill:{sid}: {exc}")

        if include_tools:
            rows = conn.execute(
                "SELECT DISTINCT tool_id FROM tool_call_records"
                + (" WHERE created_at >= ?" if since else ""),
                (since,) if since else (),
            ).fetchall()
            tool_ids = _collect_distinct_positive_ids(rows, key="tool_id", limit=2000)

            stats_map = get_tool_reuse_quality_map(tool_ids=tool_ids, since=since, conn=conn)
            for tid in tool_ids:
                tid_value = coerce_int(tid, default=0)
                if tid_value <= 0:
                    continue
                stats = stats_map.get(tid_value) or {}
                pass_calls = coerce_int(stats.get("pass_calls"), default=0)
                fail_calls = coerce_int(stats.get("fail_calls"), default=0)
                denom = pass_calls + fail_calls
                if denom < min_calls_value:
                    continue
                success_rate = float(pass_calls) / float(denom) if denom else 0.0
                if success_rate >= threshold:
                    continue

                tool_row = get_tool(tool_id=tid_value, conn=conn)
                if not tool_row:
                    continue
                meta = parse_json_dict(tool_row["metadata"]) or {}
                approval = _extract_tool_approval(meta)
                current_status = _tool_current_approval_status(meta)
                if current_status != "approved":
                    continue

                tool_actions.append(
                    {
                        "tool_id": tid_value,
                        "name": str(tool_row["name"] or ""),
                        "from_status": current_status,
                        "to_status": TOOL_APPROVAL_STATUS_REJECTED,
                        "pass_calls": pass_calls,
                        "fail_calls": fail_calls,
                        "success_rate": round(success_rate, 4),
                    }
                )
                if dry_run:
                    changed_tool_ids.append(tid_value)
                    continue
                try:
                    next_meta = _build_rejected_tool_meta(
                        meta=meta,
                        approval=approval,
                        now_value=now_value,
                        reason_text=reason_text,
                    )
                    _update_tool_meta_and_publish(
                        tool_id=tid_value,
                        metadata=next_meta,
                        change_notes=f"maintenance_auto_deprecate(since_days:{days})",
                        now_value=now_value,
                        conn=conn,
                    )
                    changed_tool_ids.append(tid_value)
                except Exception as exc:
                    errors.append(f"tool:{tid}: {exc}")

    # skills：状态更新后同步落盘文件（失败不阻塞）
    if include_skills and not dry_run and changed_skill_ids:
        for sid in changed_skill_ids:
            try:
                _source_path, publish_err = publish_skill_file(sid)
                if publish_err:
                    errors.append(f"skill:{sid}: publish_failed:{publish_err}")
            except Exception as exc:
                errors.append(f"skill:{sid}: publish_failed:{exc}")

    return {
        "ok": True,
        "dry_run": bool(dry_run),
        "since_days": days,
        "since": since,
        "min_calls": min_calls_value,
        "success_rate_threshold": threshold,
        "at": now_value,
        "skills": {
            "candidates": len(skill_actions),
            "changed": len(changed_skill_ids),
            "actions": skill_actions[:200],
        },
        "tools": {
            "candidates": len(tool_actions),
            "changed": len(changed_tool_ids),
            "actions": tool_actions[:200],
        },
        "errors": errors[:50],
    }

