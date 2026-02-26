from __future__ import annotations

import sqlite3
from typing import Optional, Tuple

from backend.src.repositories.task_outputs_repo import (
    create_task_output as create_task_output_repo,
)
from backend.src.repositories.task_outputs_repo import (
    get_task_output as get_task_output_repo,
)
from backend.src.repositories.task_outputs_repo import list_task_outputs as list_task_outputs_repo
from backend.src.repositories.task_outputs_repo import (
    list_task_outputs_for_run as list_task_outputs_for_run_repo,
)
from backend.src.repositories.task_outputs_repo import (
    list_task_outputs_for_task as list_task_outputs_for_task_repo,
)
from backend.src.services.common.coerce import (
    to_int,
    to_optional_int,
    to_text,
)


def list_task_outputs_for_run(
    *,
    task_id: int,
    run_id: int,
    limit: Optional[int] = None,
    order: str = "ASC",
    conn: Optional[sqlite3.Connection] = None,
):
    return list_task_outputs_for_run_repo(
        task_id=to_int(task_id),
        run_id=to_int(run_id),
        limit=to_optional_int(limit),
        order=to_text(order or "ASC"),
        conn=conn,
    )


def create_task_output(
    *,
    task_id: int,
    run_id: Optional[int],
    output_type: str,
    content: str,
    created_at: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[int, str]:
    return create_task_output_repo(
        task_id=to_int(task_id),
        run_id=to_optional_int(run_id),
        output_type=to_text(output_type),
        content=to_text(content),
        created_at=created_at,
        conn=conn,
    )


def get_task_output(*, output_id: int, conn: Optional[sqlite3.Connection] = None):
    return get_task_output_repo(output_id=to_int(output_id), conn=conn)


def list_task_outputs(
    *,
    task_id: int,
    run_id: Optional[int],
    offset: int,
    limit: int,
    conn: Optional[sqlite3.Connection] = None,
):
    return list_task_outputs_repo(
        task_id=to_int(task_id),
        run_id=to_optional_int(run_id),
        offset=to_int(offset),
        limit=to_int(limit),
        conn=conn,
    )


def list_task_outputs_for_task(*, task_id: int, conn: Optional[sqlite3.Connection] = None):
    return list_task_outputs_for_task_repo(task_id=to_int(task_id), conn=conn)
