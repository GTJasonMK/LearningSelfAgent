from __future__ import annotations

from typing import AsyncGenerator, List, Optional

from backend.src.agent.runner.execution_pipeline import handle_execution_exception, run_finalization_sequence
from backend.src.agent.runner.stream_task_events import iter_stream_task_events
from backend.src.services.llm.llm_client import sse_json


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
            yield ("msg", str(event_payload))
            continue
        yield ("status", str(event_payload or ""))


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


def done_sse_event() -> str:
    return sse_json({"type": "done"}, event="done")
