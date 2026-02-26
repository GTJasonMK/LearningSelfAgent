from __future__ import annotations

import sqlite3
from typing import Optional, Tuple

from backend.src.repositories.task_steps_repo import (
    TaskStepCreateParams as TaskStepCreateParamsRepo,
)
from backend.src.repositories.task_steps_repo import create_task_step as create_task_step_repo
from backend.src.repositories.task_steps_repo import get_task_step as get_task_step_repo
from backend.src.repositories.task_steps_repo import (
    get_last_non_planned_step_for_run as get_last_non_planned_step_for_run_repo,
)
from backend.src.repositories.task_steps_repo import (
    get_max_step_order_for_run_by_status as get_max_step_order_for_run_by_status_repo,
)
from backend.src.repositories.task_steps_repo import list_task_steps as list_task_steps_repo
from backend.src.repositories.task_steps_repo import (
    list_task_steps_for_run as list_task_steps_for_run_repo,
)
from backend.src.repositories.task_steps_repo import (
    list_task_steps_for_task as list_task_steps_for_task_repo,
)
from backend.src.repositories.task_steps_repo import mark_task_step_done as mark_task_step_done_repo
from backend.src.repositories.task_steps_repo import mark_task_step_failed as mark_task_step_failed_repo
from backend.src.repositories.task_steps_repo import mark_task_step_running as mark_task_step_running_repo
from backend.src.repositories.task_steps_repo import mark_task_step_skipped as mark_task_step_skipped_repo
from backend.src.repositories.task_steps_repo import update_task_step as update_task_step_repo
from backend.src.services.common.coerce import (
    to_int,
    to_int_or_default,
    to_optional_int,
    to_optional_text,
    to_text,
)

TaskStepCreateParams = TaskStepCreateParamsRepo


def get_max_step_order_for_run_by_status(
    *,
    task_id: int,
    run_id: int,
    status: str,
    conn: Optional[sqlite3.Connection] = None,
) -> int:
    return to_int_or_default(
        get_max_step_order_for_run_by_status_repo(
            task_id=to_int(task_id),
            run_id=to_int(run_id),
            status=to_text(status),
            conn=conn,
        ),
        default=0,
    )


def get_last_non_planned_step_for_run(
    *,
    task_id: int,
    run_id: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return get_last_non_planned_step_for_run_repo(
        task_id=to_int(task_id),
        run_id=to_int(run_id),
        conn=conn,
    )


def list_task_steps_for_run(
    *,
    task_id: int,
    run_id: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return list_task_steps_for_run_repo(
        task_id=to_int(task_id),
        run_id=to_int(run_id),
        conn=conn,
    )


def create_task_step(
    params: TaskStepCreateParamsRepo,
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[int, str, str]:
    return create_task_step_repo(params, conn=conn)


def get_task_step(*, step_id: int, conn: Optional[sqlite3.Connection] = None):
    return get_task_step_repo(step_id=to_int(step_id), conn=conn)


def list_task_steps(
    *,
    task_id: int,
    run_id: Optional[int],
    offset: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return list_task_steps_repo(
        task_id=to_int(task_id),
        run_id=to_optional_int(run_id),
        offset=to_int(offset),
        limit=to_int(limit),
        conn=conn,
    )


def list_task_steps_for_task(*, task_id: int, conn: Optional[sqlite3.Connection] = None):
    return list_task_steps_for_task_repo(task_id=to_int(task_id), conn=conn)


def update_task_step(
    *,
    step_id: int,
    title: Optional[str] = None,
    status: Optional[str] = None,
    detail: Optional[str] = None,
    result: Optional[str] = None,
    error: Optional[str] = None,
    step_order: Optional[int] = None,
    run_id: Optional[int] = None,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
):
    return update_task_step_repo(
        step_id=to_int(step_id),
        title=title,
        status=status,
        detail=detail,
        result=result,
        error=error,
        step_order=to_optional_int(step_order),
        run_id=to_optional_int(run_id),
        updated_at=updated_at,
        conn=conn,
    )


def mark_task_step_running(
    *,
    step_id: int,
    run_id: int,
    attempts: int,
    started_at: str,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> str:
    return mark_task_step_running_repo(
        step_id=to_int(step_id),
        run_id=to_int(run_id),
        attempts=to_int(attempts),
        started_at=to_text(started_at),
        updated_at=updated_at,
        conn=conn,
    )


def mark_task_step_done(
    *,
    step_id: int,
    result: Optional[str],
    finished_at: str,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> str:
    return mark_task_step_done_repo(
        step_id=to_int(step_id),
        result=to_optional_text(result),
        finished_at=to_text(finished_at),
        updated_at=updated_at,
        conn=conn,
    )


def mark_task_step_failed(
    *,
    step_id: int,
    error: str,
    finished_at: str,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> str:
    return mark_task_step_failed_repo(
        step_id=to_int(step_id),
        error=to_text(error),
        finished_at=to_text(finished_at),
        updated_at=updated_at,
        conn=conn,
    )


def mark_task_step_skipped(
    *,
    step_id: int,
    error: Optional[str],
    finished_at: str,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> str:
    return mark_task_step_skipped_repo(
        step_id=to_int(step_id),
        error=to_optional_text(error),
        finished_at=to_text(finished_at),
        updated_at=updated_at,
        conn=conn,
    )
