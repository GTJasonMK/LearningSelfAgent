from __future__ import annotations

from typing import Optional

from backend.src.agent.contracts.stream_events import (
    STREAM_EVENT_SCHEMA_NAME,
    STREAM_EVENT_SCHEMA_VERSION,
    coerce_session_key,
)
from backend.src.constants import (
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_RUNNING,
    RUN_STATUS_STOPPED,
    RUN_STATUS_WAITING,
)
from backend.src.services.llm.llm_client import sse_json

_VALID_STREAM_RUN_STATUSES = {
    str(RUN_STATUS_RUNNING),
    str(RUN_STATUS_WAITING),
    str(RUN_STATUS_DONE),
    str(RUN_STATUS_FAILED),
    str(RUN_STATUS_STOPPED),
}

_TERMINAL_STREAM_RUN_STATUSES = {
    str(RUN_STATUS_DONE),
    str(RUN_STATUS_FAILED),
    str(RUN_STATUS_STOPPED),
}


def normalize_stream_run_status(status: object) -> str:
    value = str(status or "").strip().lower()
    if value in _VALID_STREAM_RUN_STATUSES:
        return value
    return ""


def is_legal_stream_run_status_transition(previous: object, next_status: object) -> bool:
    """
    判断 run_status 是否允许从 previous 转移到 next_status。

    约束：
    - next_status 必须是合法状态；
    - 终态（done/failed/stopped）一旦发出，只允许重复同一终态，不允许回到运行态。
    """
    nxt = normalize_stream_run_status(next_status)
    if not nxt:
        return False
    prev = normalize_stream_run_status(previous)
    if not prev:
        return True
    if prev == nxt:
        return True
    if prev in _TERMINAL_STREAM_RUN_STATUSES:
        return False
    return True


def build_run_status_sse(
    *,
    status: object,
    task_id: Optional[int],
    run_id: int,
    stage: Optional[str] = None,
    session_key: Optional[str] = None,
) -> str:
    payload = {
        "type": "run_status",
        "schema_name": STREAM_EVENT_SCHEMA_NAME,
        "schema_version": int(STREAM_EVENT_SCHEMA_VERSION),
        "run_id": int(run_id),
        "task_id": int(task_id) if task_id is not None else None,
        "status": normalize_stream_run_status(status),
    }
    session_key_value = coerce_session_key(session_key)
    if session_key_value:
        payload["session_key"] = session_key_value
    stage_text = str(stage or "").strip()
    if stage_text:
        payload["stage"] = stage_text
    return sse_json(payload)
