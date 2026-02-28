from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Optional, Tuple

from backend.src.common.utils import now_iso, parse_optional_int

STREAM_EVENT_TYPE_RUN_CREATED = "run_created"
STREAM_EVENT_TYPE_RUN_STATUS = "run_status"
STREAM_EVENT_TYPE_NEED_INPUT = "need_input"
STREAM_EVENT_TYPE_PLAN = "plan"
STREAM_EVENT_TYPE_PLAN_DELTA = "plan_delta"
STREAM_EVENT_TYPE_DONE = "done"
STREAM_EVENT_TYPE_STREAM_END = "stream_end"
STREAM_EVENT_TYPE_ERROR = "error"
STREAM_EVENT_SCHEMA_NAME = "lsa_stream_event"
STREAM_EVENT_SCHEMA_VERSION = 2
STREAM_EVENT_SCHEMA_MIN_COMPAT_VERSION = 1


def coerce_prompt_token(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    # 限定 token 可见字符集合，避免把异常文本写入状态/日志。
    safe = "".join(ch for ch in text if ch.isalnum() or ch in {"-", "_", "."})
    return safe[:96]


def coerce_session_key(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    safe = "".join(ch for ch in text if ch.isalnum() or ch in {"-", "_", ".", ":"})
    return safe[:128]


def generate_session_key(*, task_id: int, run_id: int, created_at: str) -> str:
    raw = "|".join([str(int(task_id)), str(int(run_id)), str(created_at or "").strip()])
    digest = hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()
    return f"sess_{digest[:24]}"


def generate_prompt_token(
    *,
    task_id: int,
    run_id: int,
    step_order: int,
    question: str,
    created_at: str,
) -> str:
    raw = "|".join(
        [
            str(int(task_id)),
            str(int(run_id)),
            str(int(step_order)),
            str(question or "").strip(),
            str(created_at or "").strip(),
        ]
    )
    digest = hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()
    return digest[:24]


def build_stream_event_id(
    *,
    session_key: str,
    run_id: Optional[int],
    event_seq: int,
    event_type: str,
) -> str:
    sk = coerce_session_key(session_key) or "sess_unknown"
    rid = parse_optional_int(run_id, default=0) or 0
    et = str(event_type or "").strip().lower() or "unknown"
    seq = max(1, int(event_seq))
    return f"{sk}:{rid}:{seq}:{et}"


def _parse_sse_event_and_json(chunk: str) -> Tuple[str, Optional[dict]]:
    text = str(chunk or "")
    if not text:
        return "", None
    event_name = ""
    data_lines = []
    for line in text.splitlines():
        if line.startswith("event:"):
            event_name = str(line[6:]).strip()
            continue
        if line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
    if not data_lines:
        return event_name, None
    data_text = "\n".join(data_lines).strip()
    if not data_text:
        return event_name, None
    try:
        obj = json.loads(data_text)
    except Exception:
        return event_name, None
    return event_name, obj if isinstance(obj, dict) else None


def normalize_stream_event_schema_version(value: object) -> int:
    try:
        ver = int(value)
    except Exception:
        ver = int(STREAM_EVENT_SCHEMA_VERSION)
    if ver <= 0:
        return int(STREAM_EVENT_SCHEMA_VERSION)
    return int(ver)


def is_supported_stream_event_schema_version(value: object) -> bool:
    version = normalize_stream_event_schema_version(value)
    return int(STREAM_EVENT_SCHEMA_MIN_COMPAT_VERSION) <= int(version) <= int(STREAM_EVENT_SCHEMA_VERSION)


def normalize_stream_event_payload(payload: object) -> Optional[Dict[str, Any]]:
    """
    归一化流式事件 payload。

    约束：
    - 只接受 dict；
    - 补齐 schema_name / schema_version；
    - 兼容旧版本缺失字段；
    - schema_version 超出支持范围时返回 None（由调用方决定是否丢弃）。
    """
    if not isinstance(payload, dict):
        return None
    obj: Dict[str, Any] = dict(payload)
    obj.setdefault("schema_name", STREAM_EVENT_SCHEMA_NAME)
    obj["schema_version"] = normalize_stream_event_schema_version(obj.get("schema_version"))
    if not is_supported_stream_event_schema_version(obj.get("schema_version")):
        return None
    return obj


def parse_stream_event_chunk(chunk: str) -> Optional[Dict[str, Any]]:
    """
    解析 SSE chunk 中的 JSON data（非 JSON 或非对象返回 None）。
    """
    _event_name, obj = _parse_sse_event_and_json(chunk)
    return normalize_stream_event_payload(obj)


def attach_stream_event_meta(
    chunk: str,
    *,
    task_id: Optional[int],
    run_id: Optional[int],
    session_key: Optional[str],
    event_seq: int,
) -> Tuple[str, bool]:
    event_name, obj = _parse_sse_event_and_json(chunk)
    if not isinstance(obj, dict):
        return str(chunk or ""), False
    event_type = str(obj.get("type") or "").strip()
    if not event_type:
        # 兼容历史流：部分事件只带 SSE event（例如 event:error / event:done）而无 data.type。
        # 为保证事件可观测性（落库/审计/回放），这里做最小语义补齐。
        normalized_event_name = str(event_name or "").strip().lower()
        if normalized_event_name in {STREAM_EVENT_TYPE_DONE, STREAM_EVENT_TYPE_ERROR}:
            event_type = (
                STREAM_EVENT_TYPE_STREAM_END
                if normalized_event_name == STREAM_EVENT_TYPE_DONE
                else STREAM_EVENT_TYPE_ERROR
            )
            obj["type"] = event_type
    if not event_type:
        return str(chunk or ""), False

    normalized_session_key = coerce_session_key(session_key)
    normalized_run_id = parse_optional_int(run_id, default=0) or 0
    if normalized_session_key and not str(obj.get("session_key") or "").strip():
        obj["session_key"] = normalized_session_key
    if task_id is not None and obj.get("task_id") is None:
        obj["task_id"] = int(task_id)
    if run_id is not None and obj.get("run_id") is None:
        obj["run_id"] = int(normalized_run_id)
    if not str(obj.get("schema_name") or "").strip():
        obj["schema_name"] = STREAM_EVENT_SCHEMA_NAME
    obj["schema_version"] = int(STREAM_EVENT_SCHEMA_VERSION)
    if not str(obj.get("emitted_at") or "").strip():
        obj["emitted_at"] = now_iso()
    if not str(obj.get("causation_id") or "").strip():
        obj["causation_id"] = normalized_session_key or f"run:{int(normalized_run_id)}"
    if not str(obj.get("event_id") or "").strip():
        obj["event_id"] = build_stream_event_id(
            session_key=normalized_session_key or "sess_unknown",
            run_id=parse_optional_int(run_id, default=None),
            event_seq=int(event_seq),
            event_type=event_type,
        )

    return _sse_json(obj, event=event_name or None), True


def _sse_json(data: dict, event: Optional[str] = None) -> str:
    prefix = f"event: {event}\n" if event else ""
    return prefix + f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


def build_need_input_payload(
    *,
    task_id: int,
    run_id: int,
    question: str,
    kind: Optional[str] = None,
    choices: Optional[list] = None,
    prompt_token: Optional[str] = None,
    session_key: Optional[str] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "type": STREAM_EVENT_TYPE_NEED_INPUT,
        "schema_name": STREAM_EVENT_SCHEMA_NAME,
        "schema_version": int(STREAM_EVENT_SCHEMA_VERSION),
        "task_id": int(task_id),
        "run_id": int(run_id),
        "question": str(question or "").strip(),
    }
    kind_text = str(kind or "").strip()
    if kind_text:
        payload["kind"] = kind_text
    if isinstance(choices, list) and choices:
        payload["choices"] = list(choices)
    token = coerce_prompt_token(prompt_token)
    if token:
        payload["prompt_token"] = token
    normalized_session = coerce_session_key(session_key)
    if normalized_session:
        payload["session_key"] = normalized_session
    return payload
