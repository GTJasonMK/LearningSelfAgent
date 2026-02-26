from __future__ import annotations

import os
from typing import List, Optional

from backend.src.common.utils import json_preview, parse_json_value, truncate_text


def build_artifacts_check(*, plan_artifacts: List[object], state_obj: Optional[dict]) -> tuple[str, List[dict], List[str]]:
    """
    依据 plan_artifacts + state.workdir 计算产物落盘检查结果。

    返回：(workdir, items, missing)
    - items: [{"path": str, "exists": bool}]
    - missing: [path, ...]
    """
    workdir = str(state_obj.get("workdir") or "").strip() if isinstance(state_obj, dict) else ""
    if not workdir:
        workdir = os.getcwd()

    items: List[dict] = []
    missing: List[str] = []
    for item in plan_artifacts or []:
        rel = str(item or "").strip()
        if not rel:
            continue

        target = rel
        if not os.path.isabs(target):
            target = os.path.abspath(os.path.join(workdir, target))
        exists = bool(os.path.exists(target))

        items.append({"path": rel, "exists": exists})
        if not exists:
            missing.append(rel)

    return workdir, items, missing


def build_run_meta(
    *,
    run_id: int,
    run_row: Optional[dict],
    mode: str,
    state_obj: Optional[dict],
    workdir: str,
    artifacts_check_items: List[dict],
    missing_artifacts: List[str],
) -> dict:
    """
    构建评估上下文里的 run_meta（含 think 扩展信息与 artifacts_check）。
    """
    run_meta = {
        "run_id": int(run_id),
        "status": run_row["status"] if run_row else "",
        "started_at": run_row["started_at"] if run_row else None,
        "finished_at": run_row["finished_at"] if run_row else None,
        "summary": run_row["summary"] if run_row else "",
        "mode": str(mode or ""),
        "artifacts_check": {
            "workdir": workdir,
            "items": artifacts_check_items,
            "missing": missing_artifacts,
        },
    }

    if str(mode or "") == "think" and isinstance(state_obj, dict):
        vote_records = state_obj.get("vote_records")
        if vote_records is None:
            vote_records = state_obj.get("plan_votes")

        alternative_plans = state_obj.get("alternative_plans")
        if alternative_plans is None:
            alternative_plans = state_obj.get("plan_alternatives")

        run_meta["think"] = {
            "think_config": state_obj.get("think_config"),
            "winning_planner_id": state_obj.get("winning_planner_id"),
            "vote_records": vote_records,
            "alternative_plans": alternative_plans,
            "reflection_count": state_obj.get("reflection_count"),
            "reflection_records": state_obj.get("reflection_records"),
            "executor_assignments": state_obj.get("executor_assignments"),
        }

    return run_meta


def compact_steps_for_review(
    step_rows: List[dict],
    *,
    payload_key: str,
    result_key: str,
    error_key: str,
    max_items: Optional[int],
    payload_max_chars: int = 360,
    result_max_chars: int = 520,
    error_max_chars: int = 260,
) -> List[dict]:
    out: List[dict] = []

    for row in step_rows or []:
        action_type = None
        payload_preview = None
        try:
            detail_obj = parse_json_value(row["detail"]) if row["detail"] else None
            if isinstance(detail_obj, dict):
                action_type = detail_obj.get("type")
                payload_preview = detail_obj.get("payload")
        except Exception:
            action_type = None
            payload_preview = None

        out.append(
            {
                "step_id": row["id"],
                "step_order": row["step_order"],
                "title": row["title"],
                "status": row["status"],
                "action_type": action_type,
                payload_key: json_preview(payload_preview, payload_max_chars),
                result_key: json_preview(row["result"], result_max_chars),
                error_key: truncate_text(str(row["error"] or ""), error_max_chars),
            }
        )

        if max_items is not None and len(out) >= int(max_items):
            break

    return out


def compact_outputs_for_review(
    output_rows: List[dict],
    *,
    content_key: str,
    max_items: Optional[int],
    content_max_chars: int,
) -> List[dict]:
    out: List[dict] = []
    for row in output_rows or []:
        out.append(
            {
                "output_id": row["id"],
                "type": row["output_type"],
                content_key: truncate_text(str(row["content"] or ""), content_max_chars),
                "created_at": row["created_at"],
            }
        )
        if max_items is not None and len(out) >= int(max_items):
            break
    return out


def compact_tools_for_review(tool_rows: List[dict]) -> List[dict]:
    out: List[dict] = []
    for row in tool_rows or []:
        out.append(
            {
                "tool_call_record_id": row["id"],
                "tool_id": row["tool_id"],
                "tool_name": row["tool_name"],
                "reuse": bool(row["reuse"]),
                "reuse_status": row["reuse_status"],
                "input": truncate_text(str(row["input"] or ""), 360),
                "output": truncate_text(str(row["output"] or ""), 520),
                "created_at": row["created_at"],
            }
        )
    return out
