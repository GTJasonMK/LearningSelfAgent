from __future__ import annotations

from typing import Optional, Sequence

from backend.src.repositories.eval_repo import create_eval_record as create_eval_record_repo
from backend.src.repositories.eval_repo import get_eval_latest_summary as get_eval_latest_summary_repo
from backend.src.repositories.eval_repo import get_eval_record as get_eval_record_repo
from backend.src.repositories.eval_repo import (
    list_eval_criteria_by_eval_id as list_eval_criteria_by_eval_id_repo,
)
from backend.src.repositories.expectations_repo import (
    create_expectation as create_expectation_repo,
)
from backend.src.repositories.expectations_repo import get_expectation as get_expectation_repo
from backend.src.services.common.coerce import (
    to_int,
    to_optional_int,
    to_optional_text,
    to_text,
)


def create_expectation(*, goal: str, criteria: Sequence[str], created_at: Optional[str] = None):
    return create_expectation_repo(
        goal=to_text(goal),
        criteria=[to_text(item) for item in (criteria or [])],
        created_at=created_at,
    )


def get_expectation(*, expectation_id: int):
    return get_expectation_repo(expectation_id=to_int(expectation_id))


def create_eval_record(
    *,
    status: str,
    score: Optional[float],
    notes: Optional[str],
    task_id: Optional[int],
    expectation_id: Optional[int],
    created_at: Optional[str] = None,
):
    return create_eval_record_repo(
        status=to_text(status),
        score=(float(score) if score is not None else None),
        notes=to_optional_text(notes),
        task_id=to_optional_int(task_id),
        expectation_id=to_optional_int(expectation_id),
        created_at=created_at,
    )


def get_eval_record(*, eval_id: int):
    return get_eval_record_repo(eval_id=to_int(eval_id))


def list_eval_criteria_by_eval_id(*, eval_id: int):
    return list_eval_criteria_by_eval_id_repo(eval_id=to_int(eval_id))


def get_eval_latest_summary():
    return get_eval_latest_summary_repo()
