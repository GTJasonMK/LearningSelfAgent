from __future__ import annotations

import asyncio
from typing import Any, AsyncGenerator, Awaitable, Callable, TypeVar

from backend.src.agent.runner.execution_pipeline import pump_async_task_messages
from backend.src.agent.runner.queue_utils import make_queue_emit

T = TypeVar("T")


async def iter_stream_task_events(
    *,
    task_builder: Callable[[Callable[[str], None]], Awaitable[T]],
) -> AsyncGenerator[tuple[str, Any], None]:
    """
    统一封装“带队列消息泵的异步任务”：
    - `task_builder` 负责创建业务协程并接收 emit 回调；
    - 先持续输出 `("msg", sse_chunk)`；
    - 任务完成后输出一次 `("done", result)`。
    """
    out_q: "asyncio.Queue[str]" = asyncio.Queue()
    emit = make_queue_emit(out_q)
    task = asyncio.create_task(task_builder(emit))
    try:
        async for msg in pump_async_task_messages(task, out_q):
            if msg:
                yield ("msg", str(msg))
        result = await task
        yield ("done", result)
    except (asyncio.CancelledError, GeneratorExit):
        # 关键：当上游 SSE 连接断开时，必须同步取消内部 task，避免“外层已停止、
        # 内层仍继续改 plan/写步骤”的竞态。
        if not task.done():
            task.cancel()
        try:
            await task
        except BaseException:
            pass
        raise
