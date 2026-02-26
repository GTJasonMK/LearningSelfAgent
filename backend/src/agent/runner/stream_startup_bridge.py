from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List

from backend.src.agent.core.run_context import AgentRunContext
from backend.src.agent.runner.stream_mode_lifecycle import StreamModeLifecycle


@dataclass(frozen=True)
class StreamStartupResult:
    task_id: int
    run_id: int
    run_ctx: AgentRunContext
    emitted_events: List[str]


async def bootstrap_stream_mode_lifecycle(
    *,
    lifecycle: StreamModeLifecycle,
    start_mode_run_func: Callable[..., Awaitable[Any]],
    start_mode_run_kwargs: Dict[str, Any],
    acquire_queue_ticket_func: Callable[..., Awaitable[Any]],
) -> StreamStartupResult:
    """
    统一封装流式模式启动阶段：
    - 创建 run（do / think）
    - 绑定会话队列 ticket
    - 输出首批 run_created / run_status / stage 事件
    """
    started = await start_mode_run_func(**dict(start_mode_run_kwargs or {}))
    task_id = int(started.task_id)
    run_id = int(started.run_id)
    run_ctx = started.run_ctx

    await lifecycle.bind_started_run(
        task_id=int(task_id),
        run_id=int(run_id),
        run_ctx=run_ctx,
        acquire_queue_ticket_func=acquire_queue_ticket_func,
    )

    emitted_events = [lifecycle.emit(str(event)) for event in list(started.events or [])]

    return StreamStartupResult(
        task_id=int(task_id),
        run_id=int(run_id),
        run_ctx=run_ctx,
        emitted_events=emitted_events,
    )
