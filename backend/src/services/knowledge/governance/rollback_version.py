from __future__ import annotations

import json
from typing import Optional

from backend.src.common.utils import now_iso, parse_json_dict
from backend.src.repositories.skills_repo import get_skill
from backend.src.repositories.tools_repo import get_tool
from backend.src.services.skills.skills_publish import publish_skill_file
from backend.src.services.tools.tools_store import publish_tool_file
from backend.src.storage import get_connection


def rollback_skill_to_previous_version(
    *,
    skill_id: int,
    dry_run: bool = False,
    reason: Optional[str] = None,
) -> dict:
    """
    一键回滚技能到上一版本（基于 skill_version_records 快照）。

    说明：
    - 仅支持“回滚一步”（latest record）；
    - 回滚会写入一条新的 skill_version_records（记录当前 -> 回滚目标），便于再次回滚/追溯；
    - 默认不修改 source_path（保持文件路径稳定），但会重新发布文件内容。
    """
    try:
        sid = int(skill_id)
    except Exception:
        return {"ok": False, "error": "invalid_skill_id"}
    if sid <= 0:
        return {"ok": False, "error": "invalid_skill_id"}

    note = str(reason or "").strip() or "maintenance_rollback_version"

    with get_connection() as conn:
        current = get_skill(skill_id=int(sid), conn=conn)
        if not current:
            return {"ok": False, "error": "skill_not_found", "skill_id": int(sid)}

        rec = conn.execute(
            "SELECT id, previous_version, next_version, previous_snapshot, created_at FROM skill_version_records "
            "WHERE skill_id = ? ORDER BY id DESC LIMIT 1",
            (int(sid),),
        ).fetchone()
        if not rec:
            return {"ok": False, "error": "no_previous_version", "skill_id": int(sid)}

        snap = parse_json_dict(rec["previous_snapshot"]) or {}
        if not snap:
            return {"ok": False, "error": "invalid_snapshot", "skill_id": int(sid), "record_id": int(rec["id"])}

        target_version = snap.get("version")
        from_version = current["version"]
        if dry_run:
            return {
                "ok": True,
                "dry_run": True,
                "skill_id": int(sid),
                "record_id": int(rec["id"]),
                "from_version": from_version,
                "to_version": target_version,
                "from_name": str(current["name"] or ""),
                "to_name": str(snap.get("name") or ""),
                "at": now_iso(),
            }

        # 记录“回滚动作”（当前 -> 目标），便于追溯与二次回滚
        try:
            current_snapshot = {k: current[k] for k in current.keys()}
            conn.execute(
                "INSERT INTO skill_version_records (skill_id, previous_version, next_version, previous_snapshot, change_notes, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    int(sid),
                    current["version"],
                    target_version,
                    json.dumps(current_snapshot, ensure_ascii=False),
                    f"rollback(record:{int(rec['id'])}):{note}",
                    now_iso(),
                ),
            )
        except Exception:
            pass

        # 回滚：用快照字段覆盖（source_path/created_at 保持不变，避免路径漂移）
        conn.execute(
            """
            UPDATE skills_items
            SET name = ?,
                description = ?,
                scope = ?,
                category = ?,
                tags = ?,
                triggers = ?,
                aliases = ?,
                prerequisites = ?,
                inputs = ?,
                outputs = ?,
                steps = ?,
                failure_modes = ?,
                validation = ?,
                version = ?,
                task_id = ?,
                domain_id = ?,
                skill_type = ?,
                status = ?,
                source_task_id = ?,
                source_run_id = ?
            WHERE id = ?
            """,
            (
                snap.get("name") if snap.get("name") is not None else current["name"],
                snap.get("description"),
                snap.get("scope"),
                snap.get("category"),
                snap.get("tags"),
                snap.get("triggers"),
                snap.get("aliases"),
                snap.get("prerequisites"),
                snap.get("inputs"),
                snap.get("outputs"),
                snap.get("steps"),
                snap.get("failure_modes"),
                snap.get("validation"),
                target_version if target_version is not None else current["version"],
                snap.get("task_id"),
                snap.get("domain_id"),
                snap.get("skill_type"),
                snap.get("status"),
                snap.get("source_task_id"),
                snap.get("source_run_id"),
                int(sid),
            ),
        )

    # 落盘（失败不阻塞主流程）
    publish_err = None
    source_path = None
    try:
        source_path, publish_err = publish_skill_file(int(sid))
    except Exception as exc:
        publish_err = str(exc)

    return {
        "ok": True,
        "dry_run": False,
        "skill_id": int(sid),
        "record_id": int(rec["id"]),
        "from_version": from_version,
        "to_version": target_version,
        "source_path": source_path,
        "publish_error": publish_err,
        "at": now_iso(),
    }


def rollback_tool_to_previous_version(
    *,
    tool_id: int,
    dry_run: bool = False,
    reason: Optional[str] = None,
) -> dict:
    """
    一键回滚工具到上一版本（基于 tool_version_records.previous_snapshot）。

    说明：
    - 仅支持“回滚一步”（latest record）；
    - 回滚会写入一条新的 tool_version_records（记录当前 -> 回滚目标），便于追溯；
    - 默认不修改 source_path（保持文件路径稳定），但会重新发布文件内容。
    """
    try:
        tid = int(tool_id)
    except Exception:
        return {"ok": False, "error": "invalid_tool_id"}
    if tid <= 0:
        return {"ok": False, "error": "invalid_tool_id"}

    note = str(reason or "").strip() or "maintenance_rollback_version"

    with get_connection() as conn:
        current = get_tool(tool_id=int(tid), conn=conn)
        if not current:
            return {"ok": False, "error": "tool_not_found", "tool_id": int(tid)}

        rec = conn.execute(
            "SELECT id, previous_version, next_version, previous_snapshot, created_at FROM tool_version_records "
            "WHERE tool_id = ? ORDER BY id DESC LIMIT 1",
            (int(tid),),
        ).fetchone()
        if not rec:
            return {"ok": False, "error": "no_previous_version", "tool_id": int(tid)}

        snap = parse_json_dict(rec["previous_snapshot"]) or {}
        if not snap:
            return {"ok": False, "error": "invalid_snapshot", "tool_id": int(tid), "record_id": int(rec["id"])}

        target_version = snap.get("version") or rec["previous_version"]
        from_version = current["version"]
        if dry_run:
            return {
                "ok": True,
                "dry_run": True,
                "tool_id": int(tid),
                "record_id": int(rec["id"]),
                "from_version": from_version,
                "to_version": target_version,
                "from_name": str(current["name"] or ""),
                "to_name": str(snap.get("name") or ""),
                "at": now_iso(),
            }

        # 记录“回滚动作”（当前 -> 目标），便于追溯与二次回滚
        try:
            current_snapshot = {k: current[k] for k in current.keys()}
            conn.execute(
                "INSERT INTO tool_version_records (tool_id, previous_version, next_version, previous_snapshot, change_notes, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    int(tid),
                    current["version"],
                    target_version,
                    json.dumps(current_snapshot, ensure_ascii=False),
                    f"rollback(record:{int(rec['id'])}):{note}",
                    now_iso(),
                ),
            )
        except Exception:
            pass

        # metadata：允许回滚到 NULL
        meta_value = snap.get("metadata")
        if meta_value is None:
            metadata_text = None
        elif isinstance(meta_value, dict):
            metadata_text = json.dumps(meta_value, ensure_ascii=False)
        elif isinstance(meta_value, str):
            # snap 中的 metadata 通常是 raw DB string
            metadata_text = meta_value
        else:
            metadata_text = None

        conn.execute(
            "UPDATE tools_items SET name = ?, description = ?, version = ?, metadata = ?, updated_at = ? WHERE id = ?",
            (
                snap.get("name") if snap.get("name") is not None else current["name"],
                snap.get("description") if snap.get("description") is not None else current["description"],
                str(target_version or current["version"]),
                metadata_text,
                now_iso(),
                int(tid),
            ),
        )

        # 同步落盘（失败不阻塞）
        publish_err = None
        try:
            publish_tool_file(int(tid), conn=conn)
        except Exception as exc:
            publish_err = str(exc)

    return {
        "ok": True,
        "dry_run": False,
        "tool_id": int(tid),
        "record_id": int(rec["id"]),
        "from_version": from_version,
        "to_version": target_version,
        "publish_error": publish_err,
        "at": now_iso(),
    }
