# -*- coding: utf-8 -*-
"""
resume 场景的模式执行适配器（do / think）。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, AsyncGenerator, Callable, List, Optional

from backend.src.agent.core.checkpoint_store import persist_checkpoint_async
from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.runner.mode_do_runner import DoExecutionConfig, run_do_mode_execution_from_config
from backend.src.agent.runner.mode_think_runner import (
    ThinkExecutionConfig,
    build_inferred_parallel_dependencies,
    normalize_saved_parallel_dependencies,
    run_think_mode_execution_from_config,
)
from backend.src.agent.runner.stream_task_events import iter_stream_task_events
from backend.src.agent.runner.think_helpers import (
    build_plan_briefs_from_items,
    create_llm_call_func,
    create_step_llm_config_resolver,
)
from backend.src.agent.runner.think_parallel_loop import run_think_parallel_loop as default_run_think_parallel_loop
from backend.src.agent.think import create_think_config_from_dict, get_default_think_config
from backend.src.agent.think.think_execution import _infer_executor_from_allow
from backend.src.constants import (
    AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
    AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
    THINK_REFLECTION_MAX_ROUNDS,
)


@dataclass
class ResumeModeExecutionConfig:
    task_id: int
    run_id: int
    mode: str
    message: str
    workdir: str
    model: str
    parameters: dict
    plan_struct: PlanStructure
    tools_hint: str
    skills_hint: str
    memories_hint: str
    graph_hint: str
    state_obj: dict
    context: dict
    observations: List[str]
    resume_step_order: int
    safe_write_debug: Callable[..., None]
    parallel_loop_runner: Optional[Callable[..., object]] = None
    reflection_runner: Optional[Callable[..., object]] = None


async def iter_resume_mode_execution_events(
    *,
    config: ResumeModeExecutionConfig,
) -> AsyncGenerator[tuple[str, Any], None]:
    """
    统一 do/think 恢复执行流程并透传 SSE：
    - ("msg", chunk)
    - ("done", result_dict)
    """
    if not isinstance(config.plan_struct, PlanStructure):
        raise TypeError("ResumeModeExecutionConfig.plan_struct 必须是 PlanStructure")

    mode = str(config.mode or "").strip().lower()
    task_id = int(config.task_id)
    run_id = int(config.run_id)
    message = str(config.message or "")
    workdir = str(config.workdir or "")
    model = str(config.model or "")
    parameters = dict(config.parameters or {})
    plan_struct = config.plan_struct
    tools_hint = str(config.tools_hint or "")
    skills_hint = str(config.skills_hint or "")
    memories_hint = str(config.memories_hint or "(无)")
    graph_hint = str(config.graph_hint or "")
    state_obj = dict(config.state_obj or {})
    context = dict(config.context or {})
    observations = list(config.observations or [])
    resume_step_order = int(config.resume_step_order or 1)
    safe_write_debug = config.safe_write_debug
    parallel_loop_runner = config.parallel_loop_runner or default_run_think_parallel_loop
    reflection_runner = config.reflection_runner

    if mode == "think":
        raw_cfg = state_obj.get("think_config")
        if isinstance(raw_cfg, dict) and raw_cfg:
            think_config = create_think_config_from_dict(raw_cfg, base_model=model)
        else:
            think_config = get_default_think_config(base_model=model)

        default_cfg = get_default_think_config(base_model=model)
        if not getattr(think_config, "planners", None):
            think_config.planners = default_cfg.planners
        if not getattr(think_config, "executors", None):
            think_config.executors = default_cfg.executors

        llm_call_func = create_llm_call_func(
            base_model=model,
            base_parameters=parameters,
        )

        step_llm_config_resolver = create_step_llm_config_resolver(
            base_model=model,
            think_config=think_config,
            role_resolver=lambda _step_order, title, allow: _infer_executor_from_allow(allow or [], title or ""),
        )

        try:
            reflection_count = int(state_obj.get("reflection_count") or 0)
        except (TypeError, ValueError):
            reflection_count = 0
        max_reflection_rounds = int(THINK_REFLECTION_MAX_ROUNDS or 2)

        if isinstance(state_obj, dict) and not isinstance(state_obj.get("executor_assignments"), list):
            plan_titles, _, plan_allows, _ = plan_struct.to_legacy_lists()
            assignments_payload: List[dict] = []
            for i, title in enumerate(plan_titles or []):
                allow = plan_allows[i] if 0 <= i < len(plan_allows or []) else []
                role = _infer_executor_from_allow(allow or [], str(title or ""))
                assignments_payload.append(
                    {
                        "step_order": int(i) + 1,
                        "executor": role,
                        "allow": list(allow or []),
                    }
                )
            state_obj["executor_assignments"] = assignments_payload

        plan_titles, plan_items, plan_allows, _plan_artifacts = plan_struct.to_legacy_lists()
        plan_briefs = build_plan_briefs_from_items(plan_titles=plan_titles, plan_items=plan_items)

        def _resolve_parallel_dependencies(
            titles: List[str],
            allows: List[List[str]],
            artifacts: List[str],
            state_value: dict,
            plan_changed: bool,
        ) -> Optional[List[dict]]:
            saved = state_value.get("think_parallel_dependencies") if isinstance(state_value, dict) else None
            if not plan_changed:
                normalized = normalize_saved_parallel_dependencies(
                    saved_dependencies=saved,
                    plan_len=len(titles or []),
                )
                if normalized:
                    return normalized
            return build_inferred_parallel_dependencies(
                plan_titles=titles,
                plan_allows=allows,
                plan_artifacts=artifacts,
            )

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
            return await persist_checkpoint_async(
                run_id=int(run_id),
                agent_plan=normalized_plan.to_agent_plan_payload(),
                agent_state=next_state,
                task_id=int(task_id),
                safe_write_debug=safe_write_debug,
                where="resume.think.reflection.persist",
            )

        think_result = None
        async for event_type, event_payload in iter_stream_task_events(
            task_builder=lambda emit: run_think_mode_execution_from_config(
                ThinkExecutionConfig(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    message=message,
                    workdir=workdir,
                    model=model,
                    parameters=parameters,
                    plan_struct=plan_struct,
                    plan_briefs=plan_briefs,
                    tools_hint=tools_hint,
                    skills_hint=skills_hint,
                    memories_hint=memories_hint,
                    graph_hint=graph_hint,
                    agent_state=state_obj,
                    context=context,
                    observations=observations,
                    think_config=think_config,
                    llm_call_func=llm_call_func,
                    step_llm_config_resolver=step_llm_config_resolver,
                    yield_func=emit,
                    persist_reflection_plan_func=_persist_reflection_plan,
                    safe_write_debug=safe_write_debug,
                    start_step_order=int(resume_step_order),
                    initial_reflection_count=int(reflection_count),
                    max_reflection_rounds=int(max_reflection_rounds),
                    parallel_variables_source="agent_think_parallel_resume",
                    tail_variables_source="agent_think_react_tail_resume",
                    parallel_pump_label="think_parallel_resume",
                    tail_pump_label="think_react_tail_resume",
                    exec_done_debug_message="agent.think.exec_resume.done",
                    reflection_mark_skipped_debug_message="agent.think.resume.reflection.mark_failed_step_skipped_failed",
                    reflection_persist_failed_debug_message="agent.think.resume.reflection.persist_failed",
                    resolve_parallel_dependencies=_resolve_parallel_dependencies,
                    infer_executor_for_payload=lambda allow, title: _infer_executor_from_allow(allow or [], title),
                    parallel_loop_runner=parallel_loop_runner,
                    reflection_runner=reflection_runner,
                    poll_interval_seconds=AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
                    idle_timeout_seconds=AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
                )
            )
        ):
            if event_type == "msg":
                yield ("msg", str(event_payload))
                continue
            think_result = event_payload

        if think_result is None:
            raise RuntimeError("think_resume 执行结果为空")
        if not isinstance(getattr(think_result, "plan_struct", None), PlanStructure):
            raise RuntimeError("think_resume 执行器返回缺少有效 plan_struct")

        yield (
            "done",
            {
                "run_status": str(think_result.run_status or ""),
                "last_step_order": int(think_result.last_step_order or 0),
                "state_obj": dict(think_result.agent_state or {}),
                "plan_struct": think_result.plan_struct,
                "plan_briefs": list(think_result.plan_briefs or []),
            },
        )
        return

    react_result = None
    async for event_type, event_payload in iter_stream_task_events(
        task_builder=lambda emit: run_do_mode_execution_from_config(
            DoExecutionConfig(
                task_id=int(task_id),
                run_id=int(run_id),
                message=message,
                workdir=workdir,
                model=model,
                parameters=parameters,
                plan_struct=plan_struct,
                tools_hint=tools_hint,
                skills_hint=skills_hint,
                memories_hint=memories_hint,
                graph_hint=graph_hint,
                agent_state=state_obj,
                context=context,
                observations=observations,
                start_step_order=int(resume_step_order),
                variables_source="agent_react_resume",
                yield_func=emit,
                safe_write_debug=safe_write_debug,
                debug_done_message="agent.react_resume.done",
                pump_label="react_resume",
                poll_interval_seconds=AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
                idle_timeout_seconds=AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
            )
        )
    ):
        if event_type == "msg":
            yield ("msg", str(event_payload))
            continue
        react_result = event_payload

    if react_result is None:
        raise RuntimeError("react_resume 执行结果为空")
    if not isinstance(getattr(react_result, "plan_struct", None), PlanStructure):
        raise RuntimeError("react_resume 执行器返回缺少有效 plan_struct")

    yield (
        "done",
        {
            "run_status": str(react_result.run_status or ""),
            "last_step_order": int(react_result.last_step_order or 0),
            "state_obj": state_obj,
            "plan_struct": react_result.plan_struct,
            "plan_briefs": [],
        },
    )
