from __future__ import annotations

from typing import Callable, Optional

from backend.src.constants import AGENT_REVIEW_DISTILL_STATUS_ALLOW
from backend.src.repositories.agent_reviews_repo import get_latest_agent_review_id_for_run


def resolve_distill_gate(
    *,
    task_id: int,
    run_id: int,
    ensure_agent_review_record_fn: Callable[..., Optional[int]],
    safe_write_debug_fn: Callable[..., None],
) -> dict:
    """
    读取/创建最新 review 记录，并计算是否允许知识沉淀。
    """
    allow_distill = False
    latest_review_id: Optional[int] = None
    review_status = ""

    try:
        latest_review_id = get_latest_agent_review_id_for_run(run_id=int(run_id))
        if not latest_review_id:
            latest_review_id = ensure_agent_review_record_fn(
                task_id=int(task_id),
                run_id=int(run_id),
                skills=[],
                force=False,
            )

        if latest_review_id:
            from backend.src.repositories.agent_reviews_repo import get_agent_review as repo_get_agent_review

            review_row = repo_get_agent_review(review_id=int(latest_review_id))
            review_status = str(review_row["status"] or "").strip().lower() if review_row else ""
            review_distill_status = str(review_row["distill_status"] or "").strip().lower() if review_row else ""
            distill_score = review_row["distill_score"] if review_row else None
            distill_threshold = review_row["distill_threshold"] if review_row else None

            if review_distill_status:
                score_ok = True
                try:
                    if distill_score is not None and distill_threshold is not None:
                        score_ok = float(distill_score) >= float(distill_threshold)
                except Exception:
                    score_ok = True
                allow_distill = (
                    review_status == "pass"
                    and review_distill_status == AGENT_REVIEW_DISTILL_STATUS_ALLOW
                    and bool(score_ok)
                )
            else:
                # backward compatible：旧评估记录没有 distill_status 时，保持历史语义（pass=允许沉淀）
                allow_distill = review_status == "pass"
    except Exception as exc:
        allow_distill = False
        latest_review_id = None
        review_status = ""
        safe_write_debug_fn(
            int(task_id),
            int(run_id),
            message="postprocess.review_gate.failed",
            data={"error": str(exc)},
            level="warning",
        )

    return {
        "allow_distill": bool(allow_distill),
        "latest_review_id": int(latest_review_id) if latest_review_id else None,
        "review_status": str(review_status or ""),
    }
