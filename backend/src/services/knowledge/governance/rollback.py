from __future__ import annotations

from typing import List, Optional

from backend.src.common.utils import now_iso, parse_json_dict
from backend.src.constants import TOOL_APPROVAL_STATUS_REJECTED
from backend.src.repositories.skills_repo import VALID_SKILL_STATUSES, update_skill_status
from backend.src.repositories.tools_repo import get_tool
from backend.src.services.knowledge.governance.helpers import (
    _extract_tool_approval,
    _tool_current_approval_status,
    _update_tool_meta_and_publish,
)
from backend.src.services.skills.skills_publish import publish_skill_file
from backend.src.storage import get_connection


def rollback_knowledge_from_run(
    *,
    run_id: int,
    dry_run: bool = False,
    include_skills: bool = True,
    include_tools: bool = True,
    draft_skill_target_status: str = "abandoned",
    approved_skill_target_status: str = "deprecated",
    tool_target_status: str = TOOL_APPROVAL_STATUS_REJECTED,
    reason: Optional[str] = None,
) -> dict:
    """
    一键回滚/废弃：将某次 run 产生/更新的知识批量降级，避免错误知识长期影响后续任务。

    当前覆盖：
    - skills_items：匹配 source_run_id==run_id 或 tags 含 run:{run_id}
      - draft -> abandoned（默认）
      - approved -> deprecated（默认）
    - tools_items：匹配 metadata.approval.created_run_id==run_id 的工具，标记为 rejected（默认）

    注意：
    - 这是“知识治理”操作：默认不会删除 DB 行，只做状态降级并同步落盘文件；
    - dry_run=True 时只返回影响面预览，不写库不落盘。
    """
    try:
        rid = int(run_id)
    except Exception:
        return {"ok": False, "error": "invalid_run_id"}
    if rid <= 0:
        return {"ok": False, "error": "invalid_run_id"}

    now_value = now_iso()
    reason_text = str(reason or "").strip() or "maintenance_rollback"

    # 兜底：状态合法性
    draft_target = str(draft_skill_target_status or "").strip().lower() or "abandoned"
    if draft_target not in VALID_SKILL_STATUSES:
        draft_target = "abandoned"
    approved_target = str(approved_skill_target_status or "").strip().lower() or "deprecated"
    if approved_target not in VALID_SKILL_STATUSES:
        approved_target = "deprecated"

    tool_target = str(tool_target_status or "").strip().lower() or TOOL_APPROVAL_STATUS_REJECTED
    if tool_target not in {TOOL_APPROVAL_STATUS_REJECTED}:
        tool_target = TOOL_APPROVAL_STATUS_REJECTED

    changed_skill_ids: List[int] = []
    skill_changes: List[dict] = []
    skill_errors: List[str] = []

    changed_tool_ids: List[int] = []
    tool_changes: List[dict] = []
    tool_errors: List[str] = []

    with get_connection() as conn:
        if include_skills:
            # tags 是 JSON 数组字符串：用包含双引号的 LIKE 尽量避免误匹配（例如 run:12 命中 run:123）。
            tag_pattern = f"%\"run:{rid}\"%"
            rows = conn.execute(
                "SELECT id, name, status, skill_type, source_path FROM skills_items WHERE (source_run_id = ? OR tags LIKE ?) ORDER BY id ASC",
                (int(rid), tag_pattern),
            ).fetchall()

            for row in rows or []:
                try:
                    sid = int(row["id"])
                except Exception:
                    continue
                if sid <= 0:
                    continue
                current_status = str(row["status"] or "approved").strip().lower()
                if not current_status:
                    current_status = "approved"

                target_status = None
                if current_status == "draft":
                    target_status = draft_target
                elif current_status in {"approved", "deprecated", "abandoned"}:
                    # deprecated/abandoned 保持不变；approved 才需要降级
                    target_status = approved_target if current_status == "approved" else current_status
                else:
                    # 非法/未知值按 approved 处理
                    target_status = approved_target

                changed = target_status != current_status
                skill_changes.append(
                    {
                        "skill_id": sid,
                        "name": str(row["name"] or ""),
                        "skill_type": str(row["skill_type"] or "methodology"),
                        "source_path": str(row["source_path"] or "").strip() or None,
                        "from_status": current_status,
                        "to_status": target_status,
                        "changed": bool(changed),
                    }
                )
                if not changed:
                    continue
                if dry_run:
                    changed_skill_ids.append(sid)
                    continue
                try:
                    updated = update_skill_status(skill_id=sid, status=str(target_status), conn=conn)
                    if updated is not None:
                        changed_skill_ids.append(sid)
                except Exception as exc:
                    skill_errors.append(f"skill:{sid}: {exc}")

        if include_tools:
            # 先用 LIKE 粗筛（不依赖 SQLite json1），再解析 JSON 精确匹配 created_run_id。
            rows = conn.execute(
                "SELECT id, name, metadata, source_path FROM tools_items WHERE metadata LIKE ? ORDER BY id ASC",
                ("%created_run_id%",),
            ).fetchall()

            for row in rows or []:
                try:
                    tid = int(row["id"])
                except Exception:
                    continue
                if tid <= 0:
                    continue
                meta = parse_json_dict(row["metadata"]) or {}
                approval = _extract_tool_approval(meta)
                if not approval:
                    continue
                created_run_id = approval.get("created_run_id")
                try:
                    created_run_id = int(created_run_id) if created_run_id is not None else None
                except Exception:
                    created_run_id = None
                if created_run_id != int(rid):
                    continue

                current_status = _tool_current_approval_status(meta)
                changed = current_status != tool_target
                tool_changes.append(
                    {
                        "tool_id": tid,
                        "name": str(row["name"] or ""),
                        "source_path": str(row["source_path"] or "").strip() or None,
                        "from_status": current_status,
                        "to_status": tool_target,
                        "changed": bool(changed),
                    }
                )
                if not changed:
                    continue
                if dry_run:
                    changed_tool_ids.append(tid)
                    continue

                try:
                    # 避免覆盖其他字段：只更新 approval 子对象
                    next_meta = dict(meta)
                    approval_next = dict(approval)
                    approval_next["status"] = tool_target
                    approval_next["rejected_at"] = now_value
                    approval_next["rejected_reason"] = reason_text
                    next_meta[TOOL_METADATA_APPROVAL_KEY] = approval_next

                    _update_tool_meta_and_publish(
                        tool_id=int(tid),
                        metadata=next_meta,
                        change_notes=f"maintenance_rollback(run:{rid})",
                        now_value=now_value,
                        conn=conn,
                    )
                    changed_tool_ids.append(tid)
                except Exception as exc:
                    tool_errors.append(f"tool:{tid}: {exc}")

    # 技能文件落盘：需要独立连接（publish_skill_file 内部会读写 DB）
    if include_skills and not dry_run and changed_skill_ids:
        for sid in changed_skill_ids:
            try:
                _source_path, publish_err = publish_skill_file(int(sid))
                if publish_err:
                    skill_errors.append(f"skill:{sid}: publish_failed:{publish_err}")
            except Exception as exc:
                skill_errors.append(f"skill:{sid}: publish_failed:{exc}")

    # 兜底：返回 payload 中补充 tool 的最新 metadata（便于 UI 直接刷新）
    if include_tools and not dry_run and changed_tool_ids:
        try:
            with get_connection() as conn:
                for tid in changed_tool_ids[:50]:
                    row = get_tool(tool_id=int(tid), conn=conn)
                    if not row:
                        continue
        except Exception:
            pass

    return {
        "ok": True,
        "dry_run": bool(dry_run),
        "run_id": int(rid),
        "at": now_value,
        "skills": {
            "matched": len(skill_changes),
            "changed": len(changed_skill_ids),
            "changes": skill_changes[:200],
            "errors": skill_errors[:50],
        },
        "tools": {
            "matched": len(tool_changes),
            "changed": len(changed_tool_ids),
            "changes": tool_changes[:200],
            "errors": tool_errors[:50],
        },
    }


