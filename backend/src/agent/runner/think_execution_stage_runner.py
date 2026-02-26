from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Awaitable, Callable, List, Optional

from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.core.run_context import AgentRunContext
from backend.src.agent.runner.mode_think_runner import (
    ThinkExecutionConfig,
    build_inferred_parallel_dependencies,
    run_think_mode_execution_from_config,
)
from backend.src.agent.runner.capability_router import resolve_executor_role_by_capability, resolve_step_capability
from backend.src.agent.runner.stream_task_events import iter_stream_task_events
from backend.src.agent.runner.think_helpers import create_step_llm_config_resolver
from backend.src.agent.think import infer_executor_assignments
from backend.src.agent.think.think_execution import _infer_executor_from_allow, build_executor_assignments_payload
from backend.src.constants import (
    AGENT_EXPERIMENT_DIR_REL,
    AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
    AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
    THINK_REFLECTION_MAX_ROUNDS,
)

logger = logging.getLogger(__name__)


@dataclass
class ThinkExecutionStageConfig:
    task_id: int
    run_id: int
    message: str
    workdir: str
    model: str
    parameters: dict
    think_config: object
    llm_call_func: Callable[..., dict]
    plan_struct: PlanStructure
    plan_briefs: List[str]
    tools_hint: str
    skills_hint: str
    memories_hint: str
    graph_hint: str
    agent_state: dict
    run_ctx: Optional[AgentRunContext]
    safe_write_debug: Callable[..., None]
    persist_agent_state_func: Callable[..., Awaitable[None]]
    non_fatal_errors: tuple[type[BaseException], ...]
    base_dependencies: Optional[List[dict]] = None
    start_step_order: int = 1
    initial_reflection_count: int = 0
    max_reflection_rounds: int = THINK_REFLECTION_MAX_ROUNDS
    parallel_variables_source: str = "agent_think_parallel"
    tail_variables_source: str = "agent_think_react_tail"
    parallel_pump_label: str = "think_parallel"
    tail_pump_label: str = "think_react_tail"
    exec_done_debug_message: str = "agent.think.exec.done"
    reflection_mark_skipped_debug_message: str = "agent.think.reflection.mark_failed_step_skipped_failed"
    reflection_persist_failed_debug_message: str = "agent.think.reflection.plan.persist_failed"
    run_think_parallel_loop_func: Optional[Callable[..., object]] = None
    run_reflection_func: Optional[Callable[..., object]] = None
    poll_interval_seconds: float = AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS
    idle_timeout_seconds: float = AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS


async def iter_think_execution_stage_events(
    *,
    config: ThinkExecutionStageConfig,
) -> AsyncGenerator[tuple[str, Any], None]:
    """
    Think 执行阶段：
    - executor 分配与落库
    - 构建执行上下文
    - 运行 think 执行器并透传 SSE
    """
    if not isinstance(config.plan_struct, PlanStructure):
        raise TypeError("ThinkExecutionStageConfig.plan_struct 必须是 PlanStructure")

    plan_titles, plan_items, plan_allows, plan_artifacts = config.plan_struct.to_legacy_lists()
    agent_state = dict(config.agent_state or {})

    executor_assignments = infer_executor_assignments(
        plan_titles=plan_titles,
        plan_allows=plan_allows,
        plan_artifacts=plan_artifacts,
    )
    if isinstance(agent_state, dict):
        agent_state["executor_assignments"] = build_executor_assignments_payload(
            plan_titles=plan_titles,
            plan_allows=plan_allows,
        )
        if config.run_ctx is not None:
            config.run_ctx.set_extra("executor_assignments", list(agent_state.get("executor_assignments") or []))
            agent_state = config.run_ctx.to_agent_state()

    config.safe_write_debug(
        int(config.task_id),
        int(config.run_id),
        message="agent.think.executor_assignments",
        data={
            "assignments": [
                {"step": assignment.step_index, "executor": assignment.executor}
                for assignment in executor_assignments.assignments
            ]
        },
        level="info",
    )

    context: dict = {
        "last_llm_response": None,
        "latest_parse_input_text": None,
        "agent_workspace_rel": AGENT_EXPERIMENT_DIR_REL,
        "enforce_task_output_evidence": True,
        "enforce_shell_script_dependency": True,
        "disallow_complex_python_c": True,
        "auto_rewrite_complex_python_c": True,
        "enforce_json_parse_recent_source": True,
        "enforce_csv_artifact_quality": True,
        "enforce_csv_artifact_quality_hard_fail": True,
    }
    observations: List[str] = []
    agent_state["context"] = context
    agent_state["observations"] = observations

    think_config = config.think_config
    base_model = str(config.model or "")

    step_llm_config_resolver = create_step_llm_config_resolver(
        base_model=base_model,
        think_config=think_config,
        role_resolver=lambda _step_order, title, allow: resolve_executor_role_by_capability(
            capability=resolve_step_capability(
                allowed_actions=list(allow or []),
                step_title=title or "",
            ),
            fallback_role=_infer_executor_from_allow(allow or [], title or ""),
        ),
    )

    base_dependencies = list(config.base_dependencies or []) if isinstance(config.base_dependencies, list) else None

    def _resolve_parallel_dependencies(
        titles: List[str],
        allows: List[List[str]],
        artifacts: List[str],
        _state_obj: dict,
        plan_changed: bool,
    ) -> Optional[List[dict]]:
        if not plan_changed and isinstance(base_dependencies, list) and base_dependencies:
            return list(base_dependencies)
        return build_inferred_parallel_dependencies(
            plan_titles=titles,
            plan_allows=allows,
            plan_artifacts=artifacts,
        )

    caught_errors = config.non_fatal_errors if isinstance(config.non_fatal_errors, tuple) else (Exception,)

    async def _persist_reflection_plan(
        next_titles: List[str],
        next_items: List[dict],
        next_allows: List[List[str]],
        next_artifacts: List[str],
        next_state: dict,
    ) -> Optional[str]:
        normalized_plan = PlanStructure.from_legacy(
            plan_titles=list(next_titles or []),
            plan_items=list(next_items or []),
            plan_allows=[list(value or []) for value in (next_allows or [])],
            plan_artifacts=list(next_artifacts or []),
        )
        try:
            await config.persist_agent_state_func(
                run_id=int(config.run_id),
                agent_plan=normalized_plan.to_agent_plan_payload(),
                agent_state=next_state,
            )
            return None
        except caught_errors as exc:
            logger.exception("agent.think.reflection.plan.persist_failed: %s", exc)
            return str(exc)

    think_result = None
    async for event_type, event_payload in iter_stream_task_events(
        task_builder=lambda emit: run_think_mode_execution_from_config(
            ThinkExecutionConfig(
                task_id=int(config.task_id),
                run_id=int(config.run_id),
                message=str(config.message or ""),
                workdir=str(config.workdir or ""),
                model=str(config.model or ""),
                parameters=dict(config.parameters or {}),
                plan_struct=config.plan_struct,
                plan_briefs=list(config.plan_briefs or []),
                tools_hint=str(config.tools_hint or ""),
                skills_hint=str(config.skills_hint or ""),
                memories_hint=str(config.memories_hint or ""),
                graph_hint=str(config.graph_hint or ""),
                agent_state=agent_state,
                context=context,
                observations=observations,
                think_config=think_config,
                llm_call_func=config.llm_call_func,
                step_llm_config_resolver=step_llm_config_resolver,
                yield_func=emit,
                persist_reflection_plan_func=_persist_reflection_plan,
                safe_write_debug=config.safe_write_debug,
                start_step_order=int(config.start_step_order or 1),
                initial_reflection_count=int(config.initial_reflection_count or 0),
                max_reflection_rounds=int(config.max_reflection_rounds or THINK_REFLECTION_MAX_ROUNDS),
                parallel_variables_source=str(config.parallel_variables_source or "agent_think_parallel"),
                tail_variables_source=str(config.tail_variables_source or "agent_think_react_tail"),
                parallel_pump_label=str(config.parallel_pump_label or "think_parallel"),
                tail_pump_label=str(config.tail_pump_label or "think_react_tail"),
                exec_done_debug_message=str(config.exec_done_debug_message or "agent.think.exec.done"),
                reflection_mark_skipped_debug_message=str(
                    config.reflection_mark_skipped_debug_message
                    or "agent.think.reflection.mark_failed_step_skipped_failed"
                ),
                reflection_persist_failed_debug_message=str(
                    config.reflection_persist_failed_debug_message
                    or "agent.think.reflection.plan.persist_failed"
                ),
                resolve_parallel_dependencies=_resolve_parallel_dependencies,
                infer_executor_for_payload=lambda allow, title: _infer_executor_from_allow(allow or [], title),
                parallel_loop_runner=config.run_think_parallel_loop_func,
                reflection_runner=config.run_reflection_func,
                poll_interval_seconds=float(config.poll_interval_seconds or AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS),
                idle_timeout_seconds=float(config.idle_timeout_seconds or AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS),
            )
        )
    ):
        if event_type == "msg":
            yield ("msg", str(event_payload))
            continue
        think_result = event_payload

    if think_result is None:
        raise RuntimeError("think execution 结果为空")
    if not isinstance(getattr(think_result, "plan_struct", None), PlanStructure):
        raise RuntimeError("think 执行器返回缺少有效 plan_struct")

    yield ("done", think_result)
