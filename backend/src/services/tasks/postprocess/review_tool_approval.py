from __future__ import annotations

from typing import Callable, Optional


def ensure_existing_review_tool_approval(
    *,
    task_id: int,
    run_id: int,
    existing_review_id: int,
    run_row: Optional[dict],
    allow_tool_approval_on_waiting_feedback_fn: Callable[[Optional[dict]], bool],
    safe_write_debug_fn: Callable[..., None],
) -> None:
    """
    已有评估记录时，补齐一次幂等工具批准闭环。
    """
    try:
        from backend.src.repositories.agent_reviews_repo import get_agent_review as repo_get_agent_review
        from backend.src.services.tools.tool_approval import approve_draft_tools_from_run

        review_row = repo_get_agent_review(review_id=int(existing_review_id))
        review_status = str(review_row["status"] or "").strip() if review_row else ""
        review_distill_status = str(review_row["distill_status"] or "").strip().lower() if review_row else ""
        if not review_distill_status:
            review_distill_status = ""

        allow_waiting_feedback = allow_tool_approval_on_waiting_feedback_fn(run_row)

        approve_draft_tools_from_run(
            task_id=int(task_id),
            run_id=int(run_id),
            run_status=str(run_row["status"] or "") if run_row else "",
            review_id=int(existing_review_id),
            review_status=str(review_status or ""),
            distill_status=str(review_distill_status or "") if review_distill_status else None,
            allow_waiting_feedback=bool(allow_waiting_feedback),
            model=None,
        )
    except Exception as exc:
        safe_write_debug_fn(
            int(task_id),
            int(run_id),
            message="tool.approval.ensure_failed",
            data={"review_id": int(existing_review_id), "error": str(exc)},
            level="warning",
        )


def approve_tools_after_review(
    *,
    task_id: int,
    run_id: int,
    review_id: int,
    review_status: str,
    distill_status: str,
    run_row: Optional[dict],
    allow_tool_approval_on_waiting_feedback_fn: Callable[[Optional[dict]], bool],
    safe_write_debug_fn: Callable[..., None],
) -> None:
    """
    新评估写回后触发工具批准闭环。
    """
    try:
        from backend.src.services.tools.tool_approval import approve_draft_tools_from_run

        allow_waiting_feedback = allow_tool_approval_on_waiting_feedback_fn(run_row)

        approve_draft_tools_from_run(
            task_id=int(task_id),
            run_id=int(run_id),
            run_status=str(run_row["status"] or "") if run_row else "",
            review_id=int(review_id),
            review_status=str(review_status or ""),
            distill_status=str(distill_status or "") if distill_status else None,
            allow_waiting_feedback=bool(allow_waiting_feedback),
            model=None,
        )
    except Exception as exc:
        safe_write_debug_fn(
            int(task_id),
            int(run_id),
            message="tool.approval.failed",
            data={"review_id": int(review_id), "error": str(exc)},
            level="warning",
        )
