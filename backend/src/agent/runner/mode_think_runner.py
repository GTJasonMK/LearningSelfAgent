from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Tuple

from backend.src.agent.core.plan_coordinator import PlanCoordinator
from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.runner.feedback import is_task_feedback_step_title
from backend.src.agent.runner import react_loop as react_loop_facade
from backend.src.agent.runner.react_loop import run_react_loop
from backend.src.agent.runner.stream_pump import pump_sync_generator
from backend.src.agent.runner.think_parallel_loop import run_think_parallel_loop
from backend.src.agent.think import infer_executor_assignments, merge_fix_steps_into_plan, run_reflection
from backend.src.agent.think.think_execution import _infer_executor_from_allow
from backend.src.common.utils import now_iso
from backend.src.constants import (
    AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
    AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    STEP_STATUS_FAILED,
    STREAM_TAG_FAIL,
    STREAM_TAG_REFLECTION,
)
from backend.src.repositories.task_steps_repo import list_task_steps_for_run, mark_task_step_skipped
from backend.src.services.llm.llm_client import sse_json


@dataclass
class ThinkExecutionResult:
    run_status: str
    last_step_order: int
    reflection_count: int
    plan_briefs: List[str]
    agent_state: dict
    plan_struct: PlanStructure


@dataclass
class ThinkExecutionConfig:
    task_id: int
    run_id: int
    message: str
    workdir: str
    model: str
    parameters: dict
    plan_struct: PlanStructure
    plan_briefs: List[str]
    tools_hint: str
    skills_hint: str
    memories_hint: str
    graph_hint: str
    agent_state: dict
    context: dict
    observations: List[str]
    think_config: object
    llm_call_func: Callable[..., dict]
    step_llm_config_resolver: Callable[[int, str, List[str]], Tuple[Optional[str], Optional[dict]]]
    yield_func: Callable[[str], None]
    persist_reflection_plan_func: Optional[
        Callable[[List[str], List[dict], List[List[str]], List[str], dict], Awaitable[Optional[str]]]
    ] = None
    safe_write_debug: Optional[Callable[..., None]] = None
    start_step_order: int = 1
    initial_reflection_count: int = 0
    max_reflection_rounds: int = 2
    parallel_variables_source: str = "agent_think_parallel"
    tail_variables_source: str = "agent_think_react_tail"
    parallel_pump_label: str = "think_parallel"
    tail_pump_label: str = "think_react_tail"
    exec_done_debug_message: str = "agent.think.exec.done"
    reflection_mark_skipped_debug_message: str = "agent.think.reflection.mark_failed_step_skipped_failed"
    reflection_persist_failed_debug_message: str = "agent.think.reflection.plan.persist_failed"
    resolve_parallel_dependencies: Optional[
        Callable[[List[str], List[List[str]], List[str], dict, bool], Optional[List[dict]]]
    ] = None
    infer_executor_for_payload: Optional[Callable[[List[str], str], str]] = None
    parallel_loop_runner: Optional[Callable[..., Any]] = None
    reflection_runner: Optional[Callable[..., Any]] = None
    poll_interval_seconds: float = AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS
    idle_timeout_seconds: float = AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS


async def run_think_mode_execution_from_config(
    config: ThinkExecutionConfig,
) -> ThinkExecutionResult:
    return await _run_think_mode_execution_impl(config)


def normalize_saved_parallel_dependencies(
    *,
    saved_dependencies: object,
    plan_len: int,
) -> Optional[List[dict]]:
    """
    将 state 中缓存的依赖结构标准化为并行执行器可消费的格式。
    """
    if not isinstance(saved_dependencies, list) or int(plan_len) <= 0:
        return None
    normalized: List[dict] = []
    for item in saved_dependencies:
        if not isinstance(item, dict):
            continue
        raw_idx = item.get("step_index")
        raw_deps = item.get("depends_on")
        try:
            step_idx = int(raw_idx) if raw_idx is not None else None
        except (TypeError, ValueError):
            step_idx = None
        if step_idx is None or not (0 <= int(step_idx) < int(plan_len)):
            continue
        deps_list: List[int] = []
        if isinstance(raw_deps, list):
            for dep in raw_deps:
                try:
                    dep_idx = int(dep)
                except (TypeError, ValueError):
                    continue
                if 0 <= int(dep_idx) < int(plan_len) and int(dep_idx) != int(step_idx):
                    deps_list.append(int(dep_idx))
        normalized.append({"step_index": int(step_idx), "depends_on": sorted(set(deps_list))})
    return normalized or None


def build_inferred_parallel_dependencies(
    *,
    plan_titles: Sequence[str],
    plan_allows: Sequence[Sequence[str]],
    plan_artifacts: Sequence[str],
) -> Optional[List[dict]]:
    """
    基于当前计划结构推断并行依赖（0-based step_index）。
    """
    try:
        inferred = infer_executor_assignments(
            plan_titles=list(plan_titles or []),
            plan_allows=[list(value or []) for value in (plan_allows or [])],
            plan_artifacts=[str(value or "") for value in (plan_artifacts or [])],
        )
    except (TypeError, ValueError, AttributeError):
        return None
    dependencies: List[dict] = []
    for assignment in inferred.assignments or []:
        deps = getattr(assignment, "depends_on", None)
        if not isinstance(deps, list) or not deps:
            continue
        dependencies.append(
            {
                "step_index": int(assignment.step_index),
                "depends_on": [int(dep) for dep in deps],
            }
        )
    return dependencies or None


async def _run_think_mode_execution_impl(config: ThinkExecutionConfig) -> ThinkExecutionResult:
    """
    think 模式执行器（并行执行 + 反馈尾步 + 反思修复插入）。
    """
    task_id = int(config.task_id)
    run_id = int(config.run_id)
    message = str(config.message or "")
    workdir = str(config.workdir or "")
    model = str(config.model or "")
    parameters = dict(config.parameters or {})
    tools_hint = str(config.tools_hint or "")
    skills_hint = str(config.skills_hint or "")
    memories_hint = str(config.memories_hint or "")
    graph_hint = str(config.graph_hint or "")
    agent_state = dict(config.agent_state or {})
    context = dict(config.context or {})
    observations = list(config.observations or [])
    think_config = config.think_config
    llm_call_func = config.llm_call_func
    step_llm_config_resolver = config.step_llm_config_resolver
    yield_func = config.yield_func
    persist_reflection_plan_func = config.persist_reflection_plan_func
    safe_write_debug = config.safe_write_debug
    start_step_order = int(config.start_step_order or 1)
    initial_reflection_count = int(config.initial_reflection_count or 0)
    max_reflection_rounds = int(config.max_reflection_rounds or 2)
    parallel_variables_source = str(config.parallel_variables_source or "agent_think_parallel")
    tail_variables_source = str(config.tail_variables_source or "agent_think_react_tail")
    parallel_pump_label = str(config.parallel_pump_label or "think_parallel")
    tail_pump_label = str(config.tail_pump_label or "think_react_tail")
    exec_done_debug_message = str(config.exec_done_debug_message or "agent.think.exec.done")
    reflection_mark_skipped_debug_message = str(
        config.reflection_mark_skipped_debug_message or "agent.think.reflection.mark_failed_step_skipped_failed"
    )
    reflection_persist_failed_debug_message = str(
        config.reflection_persist_failed_debug_message or "agent.think.reflection.plan.persist_failed"
    )
    resolve_parallel_dependencies = config.resolve_parallel_dependencies
    infer_executor_for_payload = config.infer_executor_for_payload
    parallel_loop_runner = config.parallel_loop_runner
    reflection_runner = config.reflection_runner or run_reflection
    poll_interval_seconds = float(config.poll_interval_seconds or AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS)
    idle_timeout_seconds = float(config.idle_timeout_seconds or AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS)

    if not isinstance(config.plan_struct, PlanStructure):
        raise TypeError("ThinkExecutionConfig.plan_struct 必须是 PlanStructure 实例")
    plan_struct = config.plan_struct
    plan_titles, plan_items, plan_allows, plan_artifacts = plan_struct.to_legacy_lists()
    plan_briefs = list(config.plan_briefs or []) or [
        str((item or {}).get("brief") or str(title or "")[:10])
        for item, title in zip(plan_items, plan_titles)
    ]

    def _debug(message_value: str, data: Optional[dict] = None, level: str = "info") -> None:
        if not callable(safe_write_debug):
            return
        safe_write_debug(
            task_id,
            run_id,
            message=str(message_value or ""),
            data=data if isinstance(data, dict) else {},
            level=str(level or "info"),
        )

    async def _pump_inner(inner, *, label: str):
        result = None
        async for kind, payload in pump_sync_generator(
            inner=inner,
            label=str(label or "think"),
            poll_interval_seconds=float(poll_interval_seconds),
            idle_timeout_seconds=float(idle_timeout_seconds),
        ):
            if kind == "msg":
                if payload:
                    yield_func(str(payload))
                continue
            if kind == "done":
                result = payload
                break
            if kind == "err":
                if isinstance(payload, BaseException):
                    raise payload  # noqa: TRY301
                raise RuntimeError(f"{label} 异常:{payload}")  # noqa: TRY301
        if result is None:
            raise RuntimeError(f"{label} 返回为空")  # noqa: TRY301
        return result

    resolve_deps = resolve_parallel_dependencies or (
        lambda titles, allows, artifacts, _state, _modified: build_inferred_parallel_dependencies(
            plan_titles=titles,
            plan_allows=allows,
            plan_artifacts=artifacts,
        )
    )
    infer_executor = infer_executor_for_payload or (lambda allow, title: _infer_executor_from_allow(allow or [], title))
    run_parallel_loop = parallel_loop_runner or run_think_parallel_loop

    run_status = RUN_STATUS_DONE
    last_step_order = 0
    reflection_count = max(0, int(initial_reflection_count or 0))
    start_step = max(1, int(start_step_order or 1))
    plan_modified = False

    while True:
        has_feedback_tail = bool(plan_titles) and is_task_feedback_step_title(str(plan_titles[-1] or ""))
        parallel_end = len(plan_titles) - 1 if has_feedback_tail else len(plan_titles)
        tail_step_order = parallel_end + 1 if has_feedback_tail else None

        dependencies = resolve_deps(plan_titles, plan_allows, plan_artifacts, agent_state, bool(plan_modified))

        inner_parallel = run_parallel_loop(
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
            agent_state=agent_state,
            context=context,
            observations=observations,
            start_step_order=int(start_step),
            end_step_order_inclusive=int(parallel_end),
            variables_source=str(parallel_variables_source or "agent_think_parallel"),
            step_llm_config_resolver=step_llm_config_resolver,
            dependencies=dependencies,
            executor_roles=list((think_config.executors or {}).keys()),
            llm_call=react_loop_facade.create_llm_call,
            execute_step_action=react_loop_facade._execute_step_action,
            safe_write_debug=safe_write_debug,
        )

        exec_started_at = time.monotonic()
        parallel_result = await _pump_inner(inner_parallel, label=str(parallel_pump_label or "think_parallel"))
        run_status = str(parallel_result.run_status or "")
        last_step_order = int(getattr(parallel_result, "last_step_order", 0) or 0)

        # 并行循环会就地修改 plan_struct 的步骤状态，同步本地列表变量
        plan_titles, plan_items, plan_allows, plan_artifacts = plan_struct.to_legacy_lists()

        if run_status == RUN_STATUS_DONE and tail_step_order is not None:
            inner_tail = run_react_loop(
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
                agent_state=agent_state,
                context=context,
                observations=observations,
                start_step_order=int(tail_step_order),
                variables_source=str(tail_variables_source or "agent_think_react_tail"),
                step_llm_config_resolver=step_llm_config_resolver,
            )
            tail_result = await _pump_inner(inner_tail, label=str(tail_pump_label or "think_react_tail"))
            run_status = str(getattr(tail_result, "run_status", "") or "")
            last_step_order = int(getattr(tail_result, "last_step_order", 0) or 0)

        _debug(
            str(exec_done_debug_message or "agent.think.exec.done"),
            data={
                "duration_ms": int((time.monotonic() - exec_started_at) * 1000),
                "run_status": str(run_status),
                "last_step_order": int(last_step_order),
                "reflection_count": int(reflection_count),
                "has_feedback_tail": bool(has_feedback_tail),
            },
            level="info",
        )

        if run_status != RUN_STATUS_FAILED:
            break
        if reflection_count >= int(max_reflection_rounds):
            yield_func(sse_json({"delta": f"{STREAM_TAG_FAIL} 已达反思次数上限（{max_reflection_rounds}次），停止执行\n"}))
            break

        reflection_count += 1
        agent_state["reflection_count"] = int(reflection_count)
        yield_func(sse_json({"delta": f"{STREAM_TAG_REFLECTION} 执行失败，启动第 {reflection_count} 次多模型反思…\n"}))

        observations_text = "\n".join(observations[-10:]) if observations else "(无观测)"
        done_step_indices = [index for index, step in enumerate(plan_struct.steps) if step.status == "done"]
        reflection_progress: List[str] = []

        def collect_reflection_progress(msg: str) -> None:
            reflection_progress.append(str(msg or ""))

        reflection_result = await asyncio.to_thread(
            reflection_runner,
            config=think_config,
            error=f"步骤 {last_step_order} 执行失败",
            observations=observations_text,
            plan_titles=plan_titles,
            done_step_indices=done_step_indices,
            message=message,
            llm_call_func=llm_call_func,
            yield_progress=collect_reflection_progress,
            max_fix_steps=3,
        )
        for msg in reflection_progress:
            if msg:
                yield_func(sse_json({"delta": f"{msg}\n"}))

        try:
            records = agent_state.get("reflection_records")
            if not isinstance(records, list):
                records = []
            records.append(
                {
                    "round": int(reflection_count),
                    "failed_step_order": int(last_step_order),
                    "error": f"步骤 {last_step_order} 执行失败",
                    "winning_analysis": (
                        reflection_result.winning_analysis.to_dict()
                        if getattr(reflection_result, "winning_analysis", None)
                        else None
                    ),
                    "fix_steps": [
                        {
                            "title": str(step.get("title") or "").strip(),
                            "brief": str(step.get("brief") or "").strip(),
                            "allow": step.get("allow") if isinstance(step.get("allow"), list) else [],
                        }
                        for step in (reflection_result.fix_steps or [])
                        if isinstance(step, dict)
                    ],
                }
            )
            agent_state["reflection_records"] = records
        except (TypeError, ValueError, AttributeError) as exc:
            _debug(
                "agent.think.reflection.record_failed",
                data={"error": str(exc)},
                level="warning",
            )

        if not getattr(reflection_result, "fix_steps", None):
            yield_func(sse_json({"delta": f"{STREAM_TAG_FAIL} 反思未能生成修复步骤，停止执行\n"}))
            break

        failed_step_index = max(0, int(last_step_order) - 1)
        failed_step = plan_struct.get_step(failed_step_index)
        failed_title = str(failed_step.title or "").strip() if failed_step else ""
        raw_failed_allow = list(failed_step.allow or []) if failed_step else []
        failed_allow: List[str] = (
            [str(action).strip() for action in raw_failed_allow if str(action).strip()]
            if isinstance(raw_failed_allow, list)
            else []
        )
        allow_set = {str(action or "").strip().lower() for action in failed_allow if str(action or "").strip()}
        can_retry = bool(failed_title) and ("task_output" not in allow_set) and ("user_prompt" not in allow_set)

        fix_steps_for_merge = list(reflection_result.fix_steps or [])
        if can_retry:
            fix_steps_for_merge = fix_steps_for_merge[:2]
            fix_steps_for_merge.append(
                {
                    "title": failed_title,
                    "brief": "重试",
                    "allow": list(failed_allow or []),
                }
            )
        else:
            fix_steps_for_merge = fix_steps_for_merge[:3]
        if not fix_steps_for_merge:
            yield_func(sse_json({"delta": f"{STREAM_TAG_FAIL} 修复步骤为空，停止执行\n"}))
            break

        try:
            step_rows = await asyncio.to_thread(
                list_task_steps_for_run,
                task_id=int(task_id),
                run_id=int(run_id),
            )
            target_id: Optional[int] = None
            target_error = ""
            for row in reversed(step_rows or []):
                try:
                    if int(row["step_order"] or 0) != int(last_step_order):
                        continue
                    if str(row["status"] or "").strip() != str(STEP_STATUS_FAILED or "failed"):
                        continue
                    target_id = int(row["id"])
                    target_error = str(row["error"] or "").strip()
                    break
                except (TypeError, ValueError, KeyError):
                    continue
            if target_id is not None:
                await asyncio.to_thread(
                    mark_task_step_skipped,
                    step_id=int(target_id),
                    error=target_error or "skipped_by_reflection",
                    finished_at=now_iso(),
                )
        except Exception as exc:
            _debug(
                str(reflection_mark_skipped_debug_message or "agent.think.reflection.mark_failed_step_skipped_failed"),
                data={"step_order": int(last_step_order), "error": str(exc)},
                level="warning",
            )

        new_titles, new_briefs, new_allows = merge_fix_steps_into_plan(
            current_step_index=int(failed_step_index),
            plan_titles=plan_titles,
            plan_briefs=plan_briefs,
            plan_allows=plan_allows,
            fix_steps=fix_steps_for_merge,
        )

        insert_pos = failed_step_index + 1
        fix_count = len(fix_steps_for_merge or [])
        plan_titles = list(new_titles or [])
        plan_briefs = list(new_briefs or [])
        plan_allows = [list(value or []) for value in (new_allows or [])]
        plan_items = PlanCoordinator.rebuild_items_after_reflection_insert(
            plan_titles=plan_titles,
            plan_briefs=plan_briefs,
            plan_allows=plan_allows,
            old_plan_items=list(plan_items or []),
            done_step_indices=set(done_step_indices or []),
            failed_step_index=int(failed_step_index),
            insert_pos=int(insert_pos),
            fix_count=int(fix_count),
        )
        plan_struct = PlanStructure.from_legacy(
            plan_titles=list(plan_titles),
            plan_items=list(plan_items),
            plan_allows=[list(v or []) for v in plan_allows],
            plan_artifacts=list(plan_artifacts or []),
        )

        agent_state["plan_titles"] = list(plan_titles or [])
        agent_state["plan_briefs"] = list(plan_briefs or [])
        agent_state["plan_allows"] = [list(value or []) for value in (plan_allows or [])]
        try:
            agent_state["executor_assignments"] = PlanCoordinator.build_executor_assignments_payload(
                plan_titles=plan_titles,
                plan_allows=plan_allows,
                infer_executor=lambda allow, title: infer_executor(list(allow or []), str(title or "")),
            )
        except (TypeError, ValueError, AttributeError) as exc:
            _debug(
                "agent.think.executor_assignments.rebuild_failed",
                data={"error": str(exc)},
                level="warning",
            )

        if callable(persist_reflection_plan_func):
            persist_error = await persist_reflection_plan_func(
                list(plan_titles or []),
                list(plan_items or []),
                [list(value or []) for value in (plan_allows or [])],
                [str(value or "") for value in (plan_artifacts or [])],
                dict(agent_state or {}),
            )
            if persist_error:
                _debug(
                    str(reflection_persist_failed_debug_message or "agent.think.reflection.plan.persist_failed"),
                    data={"error": str(persist_error)},
                    level="warning",
                )

        yield_func(
            sse_json(
                {
                    "type": "plan",
                    "task_id": int(task_id),
                    "run_id": int(run_id),
                    "items": list(plan_items or []),
                }
            )
        )
        yield_func(sse_json({"delta": f"{STREAM_TAG_REFLECTION} 反思完成，继续从步骤 {last_step_order + 1} 执行…\n"}))

        start_step = last_step_order + 1
        plan_modified = True

    return ThinkExecutionResult(
        run_status=str(run_status or ""),
        last_step_order=int(last_step_order),
        reflection_count=int(reflection_count),
        plan_briefs=list(plan_briefs or []),
        agent_state=dict(agent_state or {}),
        plan_struct=plan_struct,
    )
