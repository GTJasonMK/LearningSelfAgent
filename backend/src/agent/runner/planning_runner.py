from __future__ import annotations

import time
from typing import Callable, Optional

from backend.src.agent.planning_phase import run_planning_phase
from backend.src.agent.runner.stream_pump import pump_sync_generator
from backend.src.constants import (
    AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
    AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
)


async def run_do_planning_phase_with_stream(
    *,
    task_id: int,
    run_id: int,
    message: str,
    workdir: str,
    model: str,
    parameters: dict,
    max_steps: int,
    tools_hint: str,
    skills_hint: str,
    solutions_hint: str,
    memories_hint: str,
    graph_hint: str,
    yield_func: Callable[[str], None],
    safe_write_debug: Optional[Callable[..., None]] = None,
    debug_done_message: str = "agent.plan.done",
    pump_label: str = "planning",
    poll_interval_seconds: float = AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
    idle_timeout_seconds: float = AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
    planning_phase_func: Optional[Callable[..., object]] = None,
):
    planning_func = planning_phase_func or run_planning_phase
    inner = planning_func(
        task_id=int(task_id),
        run_id=int(run_id),
        message=str(message or ""),
        workdir=str(workdir or ""),
        model=str(model or ""),
        parameters=dict(parameters or {}),
        max_steps=int(max_steps),
        tools_hint=str(tools_hint or ""),
        skills_hint=str(skills_hint or ""),
        solutions_hint=str(solutions_hint or ""),
        memories_hint=str(memories_hint or ""),
        graph_hint=str(graph_hint or ""),
    )
    started_at = time.monotonic()
    plan_result = None
    async for kind, payload in pump_sync_generator(
        inner=inner,
        label=str(pump_label or "planning"),
        poll_interval_seconds=float(poll_interval_seconds),
        idle_timeout_seconds=float(idle_timeout_seconds),
    ):
        if kind == "msg":
            if payload:
                yield_func(str(payload))
            continue
        if kind == "done":
            plan_result = payload
            break
        if kind == "err":
            if isinstance(payload, BaseException):
                raise payload  # noqa: TRY301
            raise RuntimeError(f"{pump_label} 异常:{payload}")  # noqa: TRY301

    if plan_result is None:
        raise RuntimeError(f"{pump_label} 返回为空")  # noqa: TRY301

    if callable(safe_write_debug):
        safe_write_debug(
            task_id,
            run_id,
            message=str(debug_done_message or "agent.plan.done"),
            data={
                "duration_ms": int((time.monotonic() - started_at) * 1000),
                "steps": len(getattr(plan_result, "plan_titles", []) or []),
            },
            level="info",
        )
    return plan_result
