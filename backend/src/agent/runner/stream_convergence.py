from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from backend.src.agent.runner.stream_status_event import normalize_stream_run_status
from backend.src.constants import RUN_STATUS_FAILED


@dataclass(frozen=True)
class StreamTerminalMeta:
    run_status: str
    completion_reason: str
    terminal_source: str


def _normalize_terminal_source(source: object) -> str:
    text = str(source or "").strip().lower()
    if not text:
        return "runtime"
    if text in {"runtime", "db", "fallback"}:
        return text
    return "runtime"


def completion_reason_for_status(run_status: object) -> str:
    normalized = normalize_stream_run_status(run_status)
    if normalized == "done":
        return "completed"
    if normalized == "waiting":
        return "waiting_input"
    if normalized == "failed":
        return "failed"
    if normalized == "stopped":
        return "stopped"
    return "unknown"


def resolve_terminal_meta(
    run_status: object,
    *,
    status_source: object = "runtime",
) -> StreamTerminalMeta:
    normalized = normalize_stream_run_status(run_status) or str(RUN_STATUS_FAILED)
    terminal_source = _normalize_terminal_source(status_source)
    completion_reason = completion_reason_for_status(normalized)
    if terminal_source != "runtime":
        completion_reason = f"{completion_reason}_from_{terminal_source}"
    return StreamTerminalMeta(
        run_status=str(normalized),
        completion_reason=str(completion_reason),
        terminal_source=str(terminal_source),
    )


def build_stream_error_payload(
    *,
    error_code: object,
    error_message: object,
    phase: object,
    task_id: Optional[int],
    run_id: Optional[int],
    recoverable: bool = False,
    retryable: bool = False,
    terminal_source: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    code_text = str(error_code or "").strip() or "stream_error"
    message_text = str(error_message or "").strip() or code_text
    payload: Dict[str, Any] = {
        "type": "error",
        "code": code_text,
        "error_code": code_text,
        "message": message_text,
        "error_message": message_text,
        "phase": str(phase or "").strip() or "stream",
        "recoverable": bool(recoverable),
        "retryable": bool(retryable),
        "task_id": int(task_id) if task_id is not None else None,
        "run_id": int(run_id) if run_id is not None else None,
    }
    source_text = str(terminal_source or "").strip().lower()
    if source_text:
        payload["terminal_source"] = source_text
    if isinstance(details, dict) and details:
        payload["details"] = dict(details)
    return payload
