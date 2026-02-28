from __future__ import annotations

from dataclasses import dataclass, field
from typing import AsyncGenerator, Awaitable, Callable, Optional

from backend.src.agent.core.run_context import AgentRunContext
from backend.src.agent.runner.session_queue import StreamQueueTicket
from backend.src.agent.runner.stream_entry_common import (
    StreamRunStateEmitter,
    done_sse_event,
    iter_execution_exception_events,
)
from backend.src.agent.runner.stream_status_event import normalize_stream_run_status
from backend.src.common.task_error_codes import format_task_error
from backend.src.constants import RUN_STATUS_FAILED, RUN_STATUS_RUNNING
from backend.src.services.llm.llm_client import sse_json
from backend.src.services.tasks.task_queries import get_task_run


@dataclass
class StreamModeLifecycle:
    """
    流式入口共享生命周期状态：
    - 统一保存 task/run/session；
    - 统一包装 emit / run_status；
    - 统一释放会话队列票据。
    """

    task_id: Optional[int] = None
    run_id: Optional[int] = None
    session_key: str = ""
    stream_state: StreamRunStateEmitter = field(default_factory=StreamRunStateEmitter)
    queue_ticket: Optional[StreamQueueTicket] = None

    def emit(self, chunk: str) -> str:
        return self.stream_state.emit(str(chunk or ""))

    def emit_run_status(self, status: object) -> Optional[str]:
        if self.task_id is None or self.run_id is None:
            return None
        self.stream_state.bind_run(
            task_id=int(self.task_id),
            run_id=int(self.run_id),
            session_key=self.session_key,
        )
        return self.stream_state.emit_run_status(status)

    async def bind_started_run(
        self,
        *,
        task_id: int,
        run_id: int,
        run_ctx: AgentRunContext,
        acquire_queue_ticket_func: Callable[..., Awaitable[StreamQueueTicket]],
    ) -> None:
        self.task_id = int(task_id)
        self.run_id = int(run_id)
        self.session_key = str(run_ctx.to_agent_state().get("session_key") or "").strip()
        self.queue_ticket = await acquire_queue_ticket_func(
            session_key=self.session_key or f"run:{int(self.run_id)}"
        )
        self.stream_state.bind_run(
            task_id=int(self.task_id),
            run_id=int(self.run_id),
            session_key=self.session_key,
            prime_status=RUN_STATUS_RUNNING,
        )

    async def release_queue_ticket_once(self) -> None:
        if self.queue_ticket is None:
            return
        try:
            await self.queue_ticket.release()
        except Exception:
            pass
        self.queue_ticket = None


async def iter_stream_exception_tail(
    *,
    lifecycle: StreamModeLifecycle,
    exc: Exception,
    mode_prefix: str,
) -> AsyncGenerator[str, None]:
    async for chunk in iter_execution_exception_events(
        exc=exc,
        task_id=lifecycle.task_id,
        run_id=lifecycle.run_id,
        mode_prefix=mode_prefix,
    ):
        yield lifecycle.emit(chunk)
    status_event = lifecycle.emit_run_status(RUN_STATUS_FAILED)
    if status_event:
        yield status_event


async def iter_stream_done_tail(
    *,
    lifecycle: StreamModeLifecycle,
    run_status: object,
) -> AsyncGenerator[str, None]:
    try:
        normalized_status = normalize_stream_run_status(run_status)
        status_source = "runtime"
        if not normalized_status:
            status_source = "fallback"
            row = None
            if lifecycle.run_id is not None:
                try:
                    row = get_task_run(run_id=int(lifecycle.run_id))
                except Exception:
                    row = None
            db_status = ""
            if row is not None:
                try:
                    db_status = normalize_stream_run_status(row["status"])
                except Exception:
                    db_status = ""
            if db_status:
                normalized_status = db_status
                status_source = "db"
            else:
                normalized_status = RUN_STATUS_FAILED
            anomaly_message = format_task_error(
                code="stream_missing_terminal_status",
                message=(
                    f"stream 结束时缺少 run_status，已自动收敛为 {normalized_status}"
                    f"（source={status_source}）"
                ),
            )
            yield lifecycle.emit(
                sse_json(
                    {
                        "message": anomaly_message,
                        "code": "stream_missing_terminal_status",
                        "task_id": lifecycle.task_id,
                        "run_id": lifecycle.run_id,
                        "resolved_status": normalized_status,
                        "status_source": status_source,
                    },
                    event="error",
                )
            )
        missing_visible_result = lifecycle.stream_state.build_missing_visible_result_if_needed(
            normalized_status
        )
        if missing_visible_result:
            yield missing_visible_result
        status_event = lifecycle.emit_run_status(normalized_status)
        if status_event:
            yield status_event
        yield lifecycle.emit(done_sse_event(run_status=str(normalized_status or "")))
    except BaseException:
        return
