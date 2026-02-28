from __future__ import annotations

from functools import wraps
from typing import AsyncGenerator, List, Optional

from backend.src.agent.contracts.stream_events import (
    attach_stream_event_meta,
    coerce_session_key,
    parse_stream_event_chunk,
)
from backend.src.agent.runner.execution_pipeline import handle_execution_exception, run_finalization_sequence
from backend.src.agent.runner.result_guard import build_missing_visible_result_body, is_terminal_result_status
from backend.src.agent.runner.stream_status_event import (
    build_run_status_sse,
    is_legal_stream_run_status_transition,
    normalize_stream_run_status,
)
from backend.src.agent.runner.stream_task_events import iter_stream_task_events
from backend.src.constants import RUN_STATUS_DONE, RUN_STATUS_FAILED, RUN_STATUS_STOPPED, STREAM_TAG_RESULT
from backend.src.common.utils import parse_optional_int
from backend.src.services.llm.llm_client import sse_json
from backend.src.services.permissions.permission_checks import ensure_write_permission
from backend.src.services.tasks.task_run_events import (
    append_task_run_event_audit,
    create_task_run_event,
)


def require_write_permission_stream(handler):
    """
    Stream 入口统一写权限守卫：无权限时直接返回 JSONResponse。
    """

    @wraps(handler)
    def _wrapped(*args, **kwargs):
        permission = ensure_write_permission()
        if permission:
            return permission
        return handler(*args, **kwargs)

    return _wrapped


async def iter_finalization_events(
    *,
    task_id: int,
    run_id: int,
    run_status: str,
    agent_state: dict,
    plan_items: List[dict],
    plan_artifacts: List[str],
    message: str,
    workdir: str,
) -> AsyncGenerator[tuple[str, str], None]:
    """
    统一封装 run_finalization_sequence 的流式转发。
    """
    emitted_status = ""
    async for event_type, event_payload in iter_stream_task_events(
        task_builder=lambda emit: run_finalization_sequence(
            task_id=int(task_id),
            run_id=int(run_id),
            run_status=str(run_status),
            agent_state=agent_state,
            plan_items=plan_items,
            plan_artifacts=plan_artifacts,
            message=message,
            workdir=workdir,
            yield_func=emit,
        )
    ):
        if event_type == "msg":
            chunk = str(event_payload)
            parsed = parse_stream_event_chunk(chunk)
            if isinstance(parsed, dict) and str(parsed.get("type") or "").strip() == "run_status":
                normalized = normalize_stream_run_status(parsed.get("status"))
                if normalized and normalized != emitted_status:
                    emitted_status = normalized
                    yield ("status", normalized)
                continue
            yield ("msg", chunk)
            continue

        normalized_done = normalize_stream_run_status(event_payload)
        if normalized_done:
            if normalized_done != emitted_status:
                emitted_status = normalized_done
                yield ("status", normalized_done)
            continue

        status_text = str(event_payload or "").strip()
        if status_text and status_text != emitted_status:
            emitted_status = status_text
            yield ("status", status_text)


async def iter_execution_exception_events(
    *,
    exc: Exception,
    task_id: Optional[int],
    run_id: Optional[int],
    mode_prefix: str,
) -> AsyncGenerator[str, None]:
    """
    统一封装未捕获异常的流式回传。
    """
    async for event_type, event_payload in iter_stream_task_events(
        task_builder=lambda emit: handle_execution_exception(
            exc,
            task_id=task_id,
            run_id=run_id,
            yield_func=emit,
            mode_prefix=mode_prefix,
        )
    ):
        if event_type == "msg":
            yield str(event_payload)


def done_sse_event(*, run_status: Optional[str] = None) -> str:
    payload = {"type": "stream_end", "kind": "stream_end"}
    status_text = str(run_status or "").strip().lower()
    if status_text:
        payload["run_status"] = status_text
    return sse_json(payload, event="done")


def chunk_has_visible_result_tag(chunk_text: str) -> bool:
    """
    检测 SSE chunk 中是否包含结果标记（STREAM_TAG_RESULT）。

    用于判断前端是否已收到最终结果展示，避免重复发送或在缺失时补发兜底消息。
    """
    return STREAM_TAG_RESULT in str(chunk_text or "")


def build_missing_visible_result_sse(run_status: str, *, task_id: int, run_id: int) -> str:
    """
    构建"缺失结果兜底"SSE 事件：当流式传输完成但前端未收到 STREAM_TAG_RESULT 时，
    补发一条包含结果标记的消息，确保前端能正确渲染结束状态。
    """
    status_text = str(run_status or "").strip()
    body = build_missing_visible_result_body(run_status=status_text, task_id=int(task_id), run_id=int(run_id))
    return sse_json({"delta": body})


class StreamRunStateEmitter:
    """
    流式运行态输出辅助器：
    - 统一追踪 has_visible_result；
    - 统一去重发送 run_status SSE；
    - 统一构建“缺失可见结果”兜底事件。
    """

    def __init__(self) -> None:
        self.task_id: Optional[int] = None
        self.run_id: Optional[int] = None
        self.session_key: str = ""
        self.has_visible_result: bool = False
        self._last_emitted_run_status: str = ""
        self._event_seq: int = 0

    def bind_run(
        self,
        *,
        task_id: Optional[int],
        run_id: Optional[int],
        session_key: Optional[str] = None,
        prime_status: object = None,
    ) -> None:
        self.task_id = parse_optional_int(task_id, default=None)
        self.run_id = parse_optional_int(run_id, default=None)
        normalized_session = coerce_session_key(session_key)
        if normalized_session:
            self.session_key = normalized_session
        normalized = normalize_stream_run_status(prime_status)
        if normalized:
            self._last_emitted_run_status = normalized

    def emit(self, chunk: str) -> str:
        text = str(chunk or "")
        rewritten, attached = attach_stream_event_meta(
            text,
            task_id=self.task_id,
            run_id=self.run_id,
            session_key=self.session_key,
            event_seq=int(self._event_seq) + 1,
        )
        if attached:
            self._event_seq += 1
            text = rewritten
            self._persist_event_log_if_needed(text)
        if not self.has_visible_result and chunk_has_visible_result_tag(text):
            self.has_visible_result = True
        return text

    def _persist_event_log_if_needed(self, chunk_text: str) -> None:
        if self.task_id is None or self.run_id is None:
            return
        event_obj = parse_stream_event_chunk(chunk_text)
        if not isinstance(event_obj, dict):
            return
        event_id = str(event_obj.get("event_id") or "").strip()
        event_type = str(event_obj.get("type") or "").strip()
        if not event_id or not event_type:
            return
        try:
            row_id = create_task_run_event(
                task_id=int(self.task_id),
                run_id=int(self.run_id),
                session_key=self.session_key,
                event_id=event_id,
                event_type=event_type,
                payload=event_obj,
            )
            if row_id is not None:
                append_task_run_event_audit(
                    task_id=int(self.task_id),
                    run_id=int(self.run_id),
                    session_key=self.session_key,
                    event_id=event_id,
                    event_type=event_type,
                    payload=event_obj,
                    row_id=int(row_id),
                )
        except Exception:
            return

    def emit_run_status(self, status: object) -> Optional[str]:
        normalized = normalize_stream_run_status(status)
        if not normalized:
            return None
        if not is_legal_stream_run_status_transition(self._last_emitted_run_status, normalized):
            return None
        if normalized == self._last_emitted_run_status:
            return None
        if self.task_id is None or self.run_id is None:
            return None
        self._last_emitted_run_status = normalized
        return self.emit(
            build_run_status_sse(
                status=normalized,
                task_id=int(self.task_id),
                run_id=int(self.run_id),
                session_key=self.session_key,
            )
        )

    def build_missing_visible_result_if_needed(self, run_status: object) -> Optional[str]:
        normalized = str(run_status or "").strip()
        if not is_terminal_result_status(normalized):
            return None
        if self.has_visible_result:
            return None
        if self.task_id is None or self.run_id is None:
            return None
        return self.emit(
            build_missing_visible_result_sse(
                normalized,
                task_id=int(self.task_id),
                run_id=int(self.run_id),
            )
        )
