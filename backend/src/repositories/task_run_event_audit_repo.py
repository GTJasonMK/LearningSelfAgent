from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any, Optional

from backend.src.common.utils import now_iso
from backend.src.storage import resolve_db_path

_AUDIT_IO_LOCK = threading.Lock()


def _is_audit_enabled() -> bool:
    raw = str(os.getenv("AGENT_RUN_EVENT_AUDIT_ENABLED", "1") or "").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _resolve_audit_dir(explicit_dir: Optional[str] = None) -> str:
    raw = str(explicit_dir or os.getenv("AGENT_RUN_EVENT_AUDIT_DIR") or "").strip()
    if raw:
        return str(Path(os.path.expanduser(os.path.expandvars(raw))).resolve())

    db_path = str(resolve_db_path() or "").strip()
    if not db_path or db_path == ":memory:" or db_path.startswith("file:"):
        return ""
    base = Path(db_path).resolve().parent
    return str((base / "run_events_audit").resolve())


def _normalize_session_part(session_key: Optional[str]) -> str:
    raw = str(session_key or "").strip()
    if not raw:
        return ""
    safe = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in raw)
    safe = safe.strip("._")
    if not safe:
        return ""
    return safe[:64]


def append_task_run_event_audit(
    *,
    task_id: int,
    run_id: int,
    event_id: str,
    event_type: str,
    payload: Any,
    session_key: Optional[str] = None,
    created_at: Optional[str] = None,
    row_id: Optional[int] = None,
    audit_dir: Optional[str] = None,
) -> Optional[str]:
    """
    追加写入 run 事件 JSONL 审计日志。

    说明：
    - 仅做可观测性增强，不影响主流程；
    - 写入失败由调用方吞掉异常，避免阻断执行链路。
    """
    if not _is_audit_enabled():
        return None
    event_key = str(event_id or "").strip()
    if not event_key:
        return None
    out_dir = _resolve_audit_dir(explicit_dir=audit_dir)
    if not out_dir:
        return None

    run_value = int(run_id)
    task_value = int(task_id)
    session_part = _normalize_session_part(session_key)
    filename = f"run_{run_value}_{session_part}.jsonl" if session_part else f"run_{run_value}.jsonl"
    output_path = Path(out_dir) / filename

    row = {
        "row_id": int(row_id) if row_id is not None else None,
        "task_id": task_value,
        "run_id": run_value,
        "session_key": str(session_key or "").strip() or None,
        "event_id": event_key,
        "event_type": str(event_type or "").strip() or "unknown",
        "created_at": str(created_at or "").strip() or now_iso(),
        "logged_at": now_iso(),
        "payload": payload if isinstance(payload, dict) else payload,
    }

    with _AUDIT_IO_LOCK:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False))
            fh.write("\n")
    return str(output_path)
