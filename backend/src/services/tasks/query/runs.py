from __future__ import annotations

import sqlite3
from typing import Optional, Sequence, Tuple

from backend.src.repositories.task_run_events_repo import (
    list_task_run_events as list_task_run_events_repo,
)
from backend.src.repositories.task_runs_repo import (
    fetch_agent_run_with_task_title_by_statuses as fetch_agent_run_with_task_title_by_statuses_repo,
)
from backend.src.repositories.task_runs_repo import (
    fetch_latest_agent_run_with_task_title as fetch_latest_agent_run_with_task_title_repo,
)
from backend.src.repositories.task_runs_repo import create_task_run as create_task_run_repo
from backend.src.repositories.task_runs_repo import get_task_run as get_task_run_repo
from backend.src.repositories.task_runs_repo import (
    get_task_run_with_task_title as get_task_run_with_task_title_repo,
)
from backend.src.repositories.task_runs_repo import list_task_runs as list_task_runs_repo
from backend.src.repositories.task_runs_repo import (
    list_task_runs_for_task as list_task_runs_for_task_repo,
)
from backend.src.repositories.task_runs_repo import update_task_run as update_task_run_repo
from backend.src.services.common.coerce import (
    to_int,
    to_int_or_default,
    to_non_empty_optional_text,
    to_non_empty_texts,
    to_optional_text,
    to_text,
)


def get_task_run(*, run_id: int, conn: Optional[sqlite3.Connection] = None):
    return get_task_run_repo(run_id=to_int(run_id), conn=conn)


def create_task_run(
    *,
    task_id: int,
    status: str,
    summary: Optional[str] = None,
    started_at: Optional[str] = None,
    finished_at: Optional[str] = None,
    created_at: Optional[str] = None,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[int, str, str]:
    return create_task_run_repo(
        task_id=to_int(task_id),
        status=to_text(status),
        summary=to_optional_text(summary),
        started_at=started_at,
        finished_at=finished_at,
        created_at=created_at,
        updated_at=updated_at,
        conn=conn,
    )


def list_task_runs(
    *,
    task_id: int,
    offset: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return list_task_runs_repo(
        task_id=to_int(task_id),
        offset=to_int(offset),
        limit=to_int(limit),
        conn=conn,
    )


def list_task_runs_for_task(
    *,
    task_id: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return list_task_runs_for_task_repo(
        task_id=to_int(task_id),
        conn=conn,
    )


def get_task_run_with_task_title(*, run_id: int, conn: Optional[sqlite3.Connection] = None):
    return get_task_run_with_task_title_repo(run_id=to_int(run_id), conn=conn)


def fetch_agent_run_with_task_title_by_statuses(
    *,
    statuses: Sequence[str],
    limit: int = 1,
    conn: Optional[sqlite3.Connection] = None,
):
    return fetch_agent_run_with_task_title_by_statuses_repo(
        statuses=to_non_empty_texts(statuses),
        limit=to_int_or_default(limit, default=1),
        conn=conn,
    )


def fetch_latest_agent_run_with_task_title(*, conn: Optional[sqlite3.Connection] = None):
    return fetch_latest_agent_run_with_task_title_repo(conn=conn)


def list_task_run_events(
    *,
    run_id: int,
    after_event_id: Optional[str] = None,
    limit: int = 200,
    conn: Optional[sqlite3.Connection] = None,
):
    return list_task_run_events_repo(
        run_id=to_int(run_id),
        after_event_id=to_non_empty_optional_text(after_event_id),
        limit=to_int_or_default(limit, default=200),
        conn=conn,
    )


def update_task_run(
    *,
    run_id: int,
    status: Optional[str] = None,
    summary: Optional[str] = None,
    agent_plan: Optional[object] = None,
    agent_state: Optional[object] = None,
    clear_finished_at: bool = False,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
):
    return update_task_run_repo(
        run_id=to_int(run_id),
        status=to_optional_text(status),
        summary=to_optional_text(summary),
        agent_plan=agent_plan,
        agent_state=agent_state,
        clear_finished_at=bool(clear_finished_at),
        updated_at=updated_at,
        conn=conn,
    )
