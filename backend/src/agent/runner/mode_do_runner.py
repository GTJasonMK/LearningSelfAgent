from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, List, Optional

from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.runner.react_loop import run_react_loop
from backend.src.agent.runner.stream_pump import pump_sync_generator
from backend.src.services.llm.llm_client import sse_json


@dataclass
class DoExecutionResult:
    run_status: str
    last_step_order: int
    plan_struct: PlanStructure


@dataclass
class DoExecutionConfig:
    task_id: int
    run_id: int
    message: str
    workdir: str
    model: str
    parameters: dict
    tools_hint: str
    skills_hint: str
    memories_hint: str
    graph_hint: str
    agent_state: dict
    context: dict
    observations: List[str]
    start_step_order: int
    variables_source: str
    yield_func: Callable[[str], None]
    plan_struct: PlanStructure
    safe_write_debug: Optional[Callable[..., None]] = None
    debug_done_message: str = "agent.react.done"
    pump_label: str = "react"
    poll_interval_seconds: float = 0.1
    idle_timeout_seconds: float = 300.0
    heartbeat_min_interval_seconds: float = 3.0
    heartbeat_trigger_debounce_seconds: float = 0.6


async def _run_do_mode_execution_impl(config: DoExecutionConfig) -> DoExecutionResult:
    if not isinstance(config.plan_struct, PlanStructure):
        raise TypeError("DoExecutionConfig.plan_struct 必须是 PlanStructure 实例")
    plan_struct = config.plan_struct

    task_id = int(config.task_id)
    run_id = int(config.run_id)
    pump_label = str(config.pump_label or "react")

    inner_react = run_react_loop(
        task_id=task_id,
        run_id=run_id,
        message=str(config.message or ""),
        workdir=str(config.workdir or ""),
        model=str(config.model or ""),
        parameters=dict(config.parameters or {}),
        plan_struct=plan_struct,
        tools_hint=str(config.tools_hint or ""),
        skills_hint=str(config.skills_hint or ""),
        memories_hint=str(config.memories_hint or ""),
        graph_hint=str(config.graph_hint or ""),
        agent_state=dict(config.agent_state or {}),
        context=dict(config.context or {}),
        observations=list(config.observations or []),
        start_step_order=int(config.start_step_order or 1),
        variables_source=str(config.variables_source or ""),
    )

    react_started_at = time.monotonic()
    react_result = None
    async for kind, payload in pump_sync_generator(
        inner=inner_react,
        label=str(pump_label or "react"),
        poll_interval_seconds=float(config.poll_interval_seconds),
        idle_timeout_seconds=float(config.idle_timeout_seconds),
        heartbeat_builder=lambda: sse_json(
            {
                "type": "run_heartbeat",
                "phase": "do_execution",
                "task_id": int(task_id),
                "run_id": int(run_id),
                "status": "running",
                "label": str(pump_label or "react"),
            }
        ),
        heartbeat_min_interval_seconds=float(config.heartbeat_min_interval_seconds or 0),
        heartbeat_trigger_debounce_seconds=float(config.heartbeat_trigger_debounce_seconds or 0),
    ):
        if kind == "msg":
            if payload:
                config.yield_func(str(payload))
            continue
        if kind == "done":
            react_result = payload
            break
        if kind == "err":
            if isinstance(payload, BaseException):
                raise payload  # noqa: TRY301
            raise RuntimeError(f"{pump_label} 异常:{payload}")  # noqa: TRY301

    if react_result is None:
        raise RuntimeError(f"{pump_label} 返回为空")  # noqa: TRY301

    run_status = str(getattr(react_result, "run_status", "") or "")
    last_step_order = int(getattr(react_result, "last_step_order", 0) or 0)
    plan_struct_result = getattr(react_result, "plan_struct", None)
    if not isinstance(plan_struct_result, PlanStructure):
        plan_struct_result = plan_struct
    if callable(config.safe_write_debug):
        config.safe_write_debug(
            task_id,
            run_id,
            message=str(config.debug_done_message or "agent.react.done"),
            data={
                "duration_ms": int((time.monotonic() - react_started_at) * 1000),
                "run_status": str(run_status),
                "last_step_order": int(last_step_order),
            },
            level="info",
        )

    return DoExecutionResult(
        run_status=str(run_status),
        last_step_order=int(last_step_order),
        plan_struct=plan_struct_result,
    )


async def run_do_mode_execution_from_config(config: DoExecutionConfig) -> DoExecutionResult:
    return await _run_do_mode_execution_impl(config)
