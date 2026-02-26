from __future__ import annotations

from typing import List

from backend.src.common.utils import coerce_int, tool_approval_status
from backend.src.constants import TOOL_APPROVAL_STATUS_REJECTED, TOOL_METADATA_APPROVAL_KEY
from backend.src.repositories.tools_repo import update_tool
from backend.src.services.tools.tools_store import publish_tool_file


def _extract_tool_approval(meta: dict) -> dict:
    approval = meta.get(TOOL_METADATA_APPROVAL_KEY)
    return approval if isinstance(approval, dict) else {}


def _skills_status_where(include_draft: bool) -> str:
    if include_draft:
        return "status IN ('approved', 'draft') OR status IS NULL"
    return "status = 'approved' OR status IS NULL"


def _collect_distinct_positive_ids(rows, *, key: str, limit: int = 2000) -> List[int]:
    out: List[int] = []
    limit_value = coerce_int(limit, default=2000)
    if limit_value <= 0:
        limit_value = 2000
    for row in rows or []:
        try:
            rid = coerce_int(row[key], default=0)
        except Exception:
            continue
        if rid <= 0 or rid in out:
            continue
        out.append(rid)
        if len(out) >= limit_value:
            break
    return out


def _tool_current_approval_status(meta: dict) -> str:
    return tool_approval_status(
        meta,
        approval_key=TOOL_METADATA_APPROVAL_KEY,
        default="approved",
    )


def _build_rejected_tool_meta(
    *,
    meta: dict,
    approval: dict,
    now_value: str,
    reason_text: str,
) -> dict:
    next_meta = dict(meta)
    approval_next = dict(approval)
    approval_next["status"] = TOOL_APPROVAL_STATUS_REJECTED
    approval_next["rejected_at"] = now_value
    approval_next["rejected_reason"] = reason_text
    next_meta[TOOL_METADATA_APPROVAL_KEY] = approval_next
    return next_meta


def _update_tool_meta_and_publish(
    *,
    tool_id: int,
    metadata: dict,
    change_notes: str,
    now_value: str,
    conn,
) -> None:
    update_tool(
        tool_id=int(tool_id),
        name=None,
        description=None,
        version=None,
        metadata=metadata,
        change_notes=change_notes,
        updated_at=now_value,
        conn=conn,
    )
    try:
        publish_tool_file(int(tool_id), conn=conn)
    except Exception:
        pass
