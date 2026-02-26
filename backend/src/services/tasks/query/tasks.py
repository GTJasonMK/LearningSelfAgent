from __future__ import annotations

import sqlite3
from typing import Optional, Sequence, Tuple

from backend.src.repositories.tasks_repo import count_tasks as count_tasks_repo
from backend.src.repositories.tasks_repo import create_task as create_task_repo
from backend.src.repositories.tasks_repo import (
    fetch_current_task_title_by_run_statuses as fetch_current_task_title_by_run_statuses_repo,
)
from backend.src.repositories.tasks_repo import get_task as get_task_repo
from backend.src.repositories.tasks_repo import list_tasks as list_tasks_repo
from backend.src.repositories.tasks_repo import task_exists as task_exists_repo
from backend.src.repositories.tasks_repo import update_task as update_task_repo
from backend.src.services.common.coerce import (
    to_int,
    to_int_or_default,
    to_non_empty_texts,
    to_optional_int,
    to_optional_text,
    to_text,
)


def task_exists(*, task_id: int, conn: Optional[sqlite3.Connection] = None) -> bool:
    return bool(task_exists_repo(task_id=to_int(task_id), conn=conn))


def get_task(*, task_id: int, conn: Optional[sqlite3.Connection] = None):
    return get_task_repo(task_id=to_int(task_id), conn=conn)


def create_task(
    *,
    title: str,
    status: str,
    expectation_id: Optional[int] = None,
    started_at: Optional[str] = None,
    finished_at: Optional[str] = None,
    created_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[int, str]:
    return create_task_repo(
        title=to_text(title),
        status=to_text(status),
        expectation_id=to_optional_int(expectation_id),
        started_at=started_at,
        finished_at=finished_at,
        created_at=created_at,
        conn=conn,
    )


def count_tasks(*, conn: Optional[sqlite3.Connection] = None) -> int:
    return to_int_or_default(count_tasks_repo(conn=conn), default=0)


def fetch_current_task_title_by_run_statuses(
    *,
    statuses: Sequence[str],
    limit: int = 1,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[str]:
    return fetch_current_task_title_by_run_statuses_repo(
        statuses=to_non_empty_texts(statuses),
        limit=to_int_or_default(limit, default=1),
        conn=conn,
    )


def list_tasks(
    *,
    start_created_at: Optional[str],
    end_created_at: Optional[str],
    conn: Optional[sqlite3.Connection] = None,
):
    return list_tasks_repo(
        start_created_at=start_created_at,
        end_created_at=end_created_at,
        conn=conn,
    )


def update_task(
    *,
    task_id: int,
    status: Optional[str] = None,
    title: Optional[str] = None,
    updated_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
):
    return update_task_repo(
        task_id=to_int(task_id),
        status=to_optional_text(status),
        title=to_optional_text(title),
        updated_at=updated_at,
        conn=conn,
    )
