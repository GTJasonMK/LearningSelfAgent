from __future__ import annotations

import sqlite3
from typing import Any, Optional, Sequence

from backend.src.repositories import agent_reviews_repo
from backend.src.services.common.coerce import to_int, to_optional_int, to_text


def create_agent_review_record(
    *,
    task_id: int,
    run_id: int,
    status: str,
    pass_score: Optional[float] = None,
    pass_threshold: Optional[float] = None,
    distill_status: Optional[str] = None,
    distill_score: Optional[float] = None,
    distill_threshold: Optional[float] = None,
    distill_notes: Optional[str] = None,
    distill_evidence_refs: Optional[Sequence[Any]] = None,
    summary: str,
    issues: Sequence[Any],
    next_actions: Sequence[Any],
    skills: Sequence[Any],
    created_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> int:
    return to_int(
        agent_reviews_repo.create_agent_review_record(
            task_id=to_int(task_id),
            run_id=to_int(run_id),
            status=to_text(status),
            pass_score=pass_score,
            pass_threshold=pass_threshold,
            distill_status=distill_status,
            distill_score=distill_score,
            distill_threshold=distill_threshold,
            distill_notes=distill_notes,
            distill_evidence_refs=distill_evidence_refs,
            summary=to_text(summary),
            issues=list(issues or []),
            next_actions=list(next_actions or []),
            skills=list(skills or []),
            created_at=created_at,
            conn=conn,
        )
    )


def list_agent_reviews(
    *,
    offset: int,
    limit: int,
    task_id: Optional[int],
    run_id: Optional[int],
    conn: Optional[sqlite3.Connection] = None,
):
    return agent_reviews_repo.list_agent_reviews(
        offset=to_int(offset),
        limit=to_int(limit),
        task_id=to_optional_int(task_id),
        run_id=to_optional_int(run_id),
        conn=conn,
    )


def get_agent_review(*, review_id: int, conn: Optional[sqlite3.Connection] = None):
    if conn is None:
        return agent_reviews_repo.get_agent_review(review_id=to_int(review_id))
    return agent_reviews_repo.get_agent_review(review_id=to_int(review_id), conn=conn)
