from __future__ import annotations

from typing import Callable, List, Optional

from backend.src.services.agent_review.review_decision import (
    evaluate_review_decision as evaluate_review_decision_shared,
)


def evaluate_review_decision(
    *,
    obj: object,
    err: Optional[str],
    raw_text: str,
    step_rows: List[dict],
    output_rows: List[dict],
    tool_rows: List[dict],
    plan_artifacts: List[str],
    artifacts_check_items: List[dict],
    find_unverified_text_output_fn: Callable[[List[dict]], Optional[dict]],
) -> dict:
    return evaluate_review_decision_shared(
        obj=obj,
        err=err,
        raw_text=raw_text,
        step_rows=step_rows,
        output_rows=output_rows,
        tool_rows=tool_rows,
        plan_artifacts=plan_artifacts,
        artifacts_check_items=artifacts_check_items,
        find_unverified_text_output_fn=find_unverified_text_output_fn,
    )
