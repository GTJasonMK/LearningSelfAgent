from __future__ import annotations

from typing import Any, Optional

from backend.src.repositories.task_run_event_audit_repo import (
    append_task_run_event_audit as append_task_run_event_audit_repo,
)
from backend.src.repositories.task_run_events_repo import (
    create_task_run_event as create_task_run_event_repo,
)
from backend.src.services.common.coerce import (
    to_int,
    to_non_empty_optional_text,
    to_optional_int,
    to_text,
)


def create_task_run_event(
    *,
    task_id: int,
    run_id: int,
    session_key: Optional[str],
    event_id: str,
    event_type: str,
    payload: Any,
):
    return create_task_run_event_repo(
        task_id=to_int(task_id),
        run_id=to_int(run_id),
        session_key=to_non_empty_optional_text(session_key),
        event_id=to_text(event_id),
        event_type=to_text(event_type),
        payload=payload,
    )


def append_task_run_event_audit(
    *,
    task_id: int,
    run_id: int,
    session_key: Optional[str],
    event_id: str,
    event_type: str,
    payload: Any,
    row_id: Optional[int] = None,
):
    return append_task_run_event_audit_repo(
        task_id=to_int(task_id),
        run_id=to_int(run_id),
        session_key=to_non_empty_optional_text(session_key),
        event_id=to_text(event_id),
        event_type=to_text(event_type),
        payload=payload,
        row_id=to_optional_int(row_id),
    )
