# -*- coding: utf-8 -*-
"""
pending_planning 等待态进入逻辑（do/think 共用）。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional

from backend.src.agent.runner.execution_pipeline import enter_pending_planning_waiting
from backend.src.agent.runner.stream_task_events import iter_stream_task_events


@dataclass
class PendingPlanningWaitConfig:
    task_id: int
    run_id: int
    mode: str
    message: str
    workdir: str
    model: str
    parameters: dict
    max_steps: int
    user_prompt_question: str
    tools_hint: str
    skills_hint: str
    solutions_hint: str
    memories_hint: str
    graph_hint: str
    domain_ids: List[str]
    skills: List[dict]
    solutions: List[dict]
    draft_solution_id: Optional[int] = None
    think_config: Optional[Dict[str, Any]] = None
    safe_write_debug_func: Optional[Callable[..., None]] = None


async def iter_pending_planning_wait_events(
    *,
    config: PendingPlanningWaitConfig,
) -> AsyncGenerator[tuple[str, Any], None]:
    """
    进入 pending_planning waiting，并将 SSE 子流转发到外层。
    """
    async for event_type, event_payload in iter_stream_task_events(
        task_builder=lambda emit: enter_pending_planning_waiting(
            task_id=int(config.task_id),
            run_id=int(config.run_id),
            mode=str(config.mode or "do"),
            message=str(config.message or ""),
            workdir=str(config.workdir or ""),
            model=str(config.model or ""),
            parameters=dict(config.parameters or {}),
            max_steps=int(config.max_steps),
            user_prompt_question=str(config.user_prompt_question or ""),
            tools_hint=str(config.tools_hint or "(无)"),
            skills_hint=str(config.skills_hint or "(无)"),
            solutions_hint=str(config.solutions_hint or "(无)"),
            memories_hint=str(config.memories_hint or "(无)"),
            graph_hint=str(config.graph_hint or "(无)"),
            domain_ids=list(config.domain_ids or []),
            skills=list(config.skills or []),
            solutions=list(config.solutions or []),
            draft_solution_id=(
                int(config.draft_solution_id)
                if isinstance(config.draft_solution_id, int) and int(config.draft_solution_id) > 0
                else None
            ),
            think_config=config.think_config if isinstance(config.think_config, dict) else None,
            yield_func=emit,
            safe_write_debug_func=config.safe_write_debug_func,
        )
    ):
        if event_type == "msg":
            yield ("msg", str(event_payload))
            continue
        yield ("done", event_payload if isinstance(event_payload, dict) else {})


async def run_pending_planning_wait_with_stream(
    *,
    config: PendingPlanningWaitConfig,
    yield_func: Callable[[str], None],
) -> Dict[str, Any]:
    """
    进入 pending waiting 并把子流写回外层，返回 waiting 落库结果。
    """
    wait_result: Optional[Dict[str, Any]] = None
    async for event_type, event_payload in iter_pending_planning_wait_events(config=config):
        if event_type == "msg":
            yield_func(str(event_payload))
            continue
        wait_result = event_payload if isinstance(event_payload, dict) else {}
    if wait_result is None:
        raise RuntimeError("pending_planning waiting 结果为空")
    return wait_result
