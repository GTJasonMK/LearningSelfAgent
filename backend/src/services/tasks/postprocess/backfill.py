from __future__ import annotations

from typing import Callable

from backend.src.common.utils import extract_json_object
from backend.src.constants import (
    AGENT_TASK_FEEDBACK_STEP_TITLE,
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_STOPPED,
    RUN_STATUS_WAITING,
)
from backend.src.repositories.task_runs_repo import list_agent_runs_missing_reviews


def backfill_missing_agent_reviews(
    *,
    ensure_agent_review_record_fn: Callable[..., int | None],
    limit: int = 10,
) -> dict:
    """
    启动兜底：补齐最近 N 条“已结束(done/failed/stopped)但缺评估”的 Agent runs。
    """
    try:
        rows = list_agent_runs_missing_reviews(
            statuses=[RUN_STATUS_DONE, RUN_STATUS_FAILED, RUN_STATUS_STOPPED],
            limit=int(limit),
        )
    except Exception as exc:
        return {"ok": False, "error": str(exc), "count": 0, "items": []}

    created = []
    for row in rows:
        try:
            run_id = int(row["id"])
            task_id = int(row["task_id"])
        except Exception:
            continue
        review_id = ensure_agent_review_record_fn(task_id=task_id, run_id=run_id, skills=[])
        if review_id:
            created.append({"run_id": run_id, "review_id": int(review_id)})
    return {"ok": True, "count": len(created), "items": created}


def backfill_waiting_feedback_agent_reviews(
    *,
    ensure_agent_review_record_fn: Callable[..., int | None],
    limit: int = 10,
) -> dict:
    """
    启动兜底：补齐最近 N 条“waiting(确认满意度) 但缺评估”的 Agent runs。
    """
    try:
        rows = list_agent_runs_missing_reviews(statuses=[RUN_STATUS_WAITING], limit=int(limit))
    except Exception as exc:
        return {"ok": False, "error": str(exc), "count": 0, "items": []}

    created = []
    for row in rows:
        try:
            run_id = int(row["id"])
            task_id = int(row["task_id"])
        except Exception:
            continue
        # 只处理"确认满意度等待"，避免把"用户补充信息等待(user_prompt)"误当作已完成任务。
        try:
            raw_state = str(row["agent_state"] or "") if row and "agent_state" in row.keys() else ""
            state_obj = extract_json_object(raw_state) if raw_state else None
            paused = state_obj.get("paused") if isinstance(state_obj, dict) else None
            step_title = str(paused.get("step_title") or "").strip() if isinstance(paused, dict) else ""
            if step_title != AGENT_TASK_FEEDBACK_STEP_TITLE:
                continue
        except Exception:
            continue

        try:
            review_id = ensure_agent_review_record_fn(task_id=task_id, run_id=run_id, skills=[], force=False)
            if review_id:
                created.append({"run_id": run_id, "review_id": int(review_id)})
        except Exception:
            # 单行评估失败不阻塞其他行
            continue
    return {"ok": True, "count": len(created), "items": created}
