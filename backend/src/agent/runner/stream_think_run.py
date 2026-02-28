"""
Think 模式流式执行入口。

与 stream_new_run.py（do 模式）类似，但使用多模型协作规划和执行。
"""

import asyncio
import logging
import os
import sqlite3
import time
from typing import AsyncGenerator, List, Optional

from backend.src.agent.core.run_context import AgentRunContext
from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.runner.debug_utils import safe_write_debug as _safe_write_debug
from backend.src.agent.runner.feedback import canonicalize_task_feedback_steps, canonicalized_feedback_meta
from backend.src.agent.runner.run_startup import start_new_mode_run
from backend.src.agent.runner.run_stage import persist_run_stage
from backend.src.agent.runner.pending_planning_wait_runner import (
    PendingPlanningWaitConfig,
    iter_pending_planning_wait_events,
)
from backend.src.agent.runner.run_context_knowledge import apply_knowledge_identity_to_run_ctx
from backend.src.agent.runner.stream_entry_common import (
    iter_finalization_events,
    require_write_permission_stream,
)
from backend.src.agent.runner.session_queue import acquire_stream_queue_ticket
from backend.src.agent.runner.stream_mode_lifecycle import (
    StreamModeLifecycle,
    iter_stream_done_tail,
    iter_stream_exception_tail,
)
from backend.src.agent.runner.stream_request import (
    ParsedStreamCommandRequest,
    parse_stream_command_request,
)
from backend.src.agent.runner.stream_startup_bridge import bootstrap_stream_mode_lifecycle
from backend.src.agent.runner.stream_task_events import iter_stream_task_events
from backend.src.agent.runner.think_parallel_loop import run_think_parallel_loop
from backend.src.agent.runner.think_helpers import create_llm_call_func
from backend.src.agent.runner.think_execution_stage_runner import (
    ThinkExecutionStageConfig,
    iter_think_execution_stage_events,
)
from backend.src.agent.runner.think_retrieval_merge_runner import (
    ThinkRetrievalMergeConfig,
    iter_think_retrieval_merge_events,
)
from backend.src.agent.runner.execution_pipeline import (
    create_sse_response,
    handle_stream_cancellation,
    build_base_agent_state,
    persist_agent_state,
)
from backend.src.agent.support import (
    _assess_knowledge_sufficiency,
    _filter_relevant_domains,
    _collect_tools_from_solutions,
    _compose_skills,
    _draft_skill_from_message,
    _format_graph_for_prompt,
    _format_skills_for_prompt,
    _format_solutions_for_prompt,
    _draft_solution_from_skills,
    _select_relevant_graph_nodes,
    _select_relevant_skills,
    _select_relevant_solutions,
)
from backend.src.agent.think import (
    ThinkConfig,
    ThinkPlanResult,
    get_default_think_config,
    create_think_config_from_dict,
    run_think_planning_sync,
    run_reflection,
)
from backend.src.api.schemas import AgentCommandStreamRequest
from backend.src.common.utils import now_iso, parse_positive_int
from backend.src.constants import (
    AGENT_MAX_STEPS_UNLIMITED,
    AGENT_EXPERIMENT_DIR_REL,
    AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
    AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_STOPPED,
    STREAM_TAG_EXEC,
    STREAM_TAG_THINK,
    SSE_TYPE_MEMORY_ITEM,
    THINK_REFLECTION_MAX_ROUNDS,
)
from backend.src.services.llm.llm_client import sse_json
from backend.src.services.skills.skills_draft import create_skill
from backend.src.services.skills.skills_publish import publish_skill_file
from backend.src.services.tasks.task_run_lifecycle import (
    check_missing_artifacts,
    enqueue_postprocess_thread,
    enqueue_review_on_feedback_waiting,
    enqueue_stop_task_run_records,
    finalize_run_and_task_status,
    mark_run_failed,
)

logger = logging.getLogger(__name__)
NON_FATAL_STREAM_ERRORS = (sqlite3.Error, RuntimeError, TypeError, ValueError, OSError, AttributeError, ImportError)


def _log_text_brief(value: object, limit: int = 120) -> str:
    text = str(value or "").strip().replace("\n", "\\n")
    if len(text) <= int(limit):
        return text
    return f"{text[: int(limit)]}..."


@require_write_permission_stream
def stream_agent_think_command(payload: AgentCommandStreamRequest):
    """
    Think 模式指令执行（SSE 流式）：
    - 创建 task/run
    - 检索（graph/memory/skills）
    - 多模型头脑风暴规划
    - 多 Executor 分工执行（按依赖并行调度；保留顺序收尾步骤）
    - 失败时触发反思机制
    """
    parsed = parse_stream_command_request(payload)
    if not isinstance(parsed, ParsedStreamCommandRequest):
        return parsed
    message = parsed.message
    max_steps = parsed.requested_max_steps
    normalized_max_steps = int(parsed.normalized_max_steps)
    dry_run = bool(parsed.dry_run)
    model = parsed.model
    parameters = dict(parsed.parameters or {})

    # 解析 Think 配置
    think_config: ThinkConfig
    if payload.think_config:
        think_config = create_think_config_from_dict(payload.think_config, base_model=model)
        # 保底：反思机制需要 planners；执行需要 executors。若缺失则用默认补齐，避免“部分配置导致不可运行”。
        default_cfg = get_default_think_config(base_model=model)
        if not getattr(think_config, "planners", None):
            think_config.planners = default_cfg.planners
        if not getattr(think_config, "executors", None):
            think_config.executors = default_cfg.executors
        else:
            for role, exec_cfg in (default_cfg.executors or {}).items():
                if role not in think_config.executors:
                    think_config.executors[role] = exec_cfg
    else:
        think_config = get_default_think_config(base_model=model)

    async def gen() -> AsyncGenerator[str, None]:
        task_id: Optional[int] = None
        run_id: Optional[int] = None
        run_status: str = ""
        lifecycle = StreamModeLifecycle()
        plan_items: List[dict] = []
        run_ctx: Optional[AgentRunContext] = None

        try:
            workdir = os.getcwd()

            startup = await bootstrap_stream_mode_lifecycle(
                lifecycle=lifecycle,
                start_mode_run_func=start_new_mode_run,
                start_mode_run_kwargs={
                    "message": message,
                    "mode": "think",
                    "model": model,
                    "parameters": parameters,
                    "max_steps": normalized_max_steps,
                    "workdir": workdir,
                    "stage_where_prefix": "think_run",
                    "safe_write_debug": _safe_write_debug,
                    "state_overrides": {"think_config": payload.think_config},
                    "tools_hint": "(无)",
                    "skills_hint": "(无)",
                    "solutions_hint": "(无)",
                    "memories_hint": "(无)",
                    "graph_hint": "",
                    "start_debug_message": "agent.think.start",
                    "start_debug_data": {
                        "mode": "think",
                        "model": model,
                        "max_steps": max_steps,
                        "dry_run": dry_run,
                        "workdir": workdir,
                        "planner_count": think_config.get_planner_count(),
                    },
                    "start_delta": f"{STREAM_TAG_THINK} Think 模式启动，{think_config.get_planner_count()} 个规划者协作\n",
                },
                acquire_queue_ticket_func=acquire_stream_queue_ticket,
            )
            task_id = int(startup.task_id)
            run_id = int(startup.run_id)
            run_ctx = startup.run_ctx
            logger.info(
                "[agent.think] start task_id=%s run_id=%s model=%s max_steps=%s dry_run=%s planners=%s",
                task_id,
                run_id,
                model,
                normalized_max_steps,
                dry_run,
                think_config.get_planner_count(),
            )
            for event in startup.emitted_events:
                yield str(event)
            # 工具清单会在“方案匹配”之后汇总（方案提到的工具优先）
            tools_hint = "(无)"
            solutions_hint = "(无)"

            retrieval_result = None
            async for event_type, event_payload in iter_think_retrieval_merge_events(
                config=ThinkRetrievalMergeConfig(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    message=message,
                    model=model,
                    parameters=parameters,
                    think_config=think_config,
                    safe_write_debug=_safe_write_debug,
                    select_relevant_graph_nodes_func=_select_relevant_graph_nodes,
                    format_graph_for_prompt_func=_format_graph_for_prompt,
                    filter_relevant_domains_func=_filter_relevant_domains,
                    select_relevant_skills_func=_select_relevant_skills,
                    format_skills_for_prompt_func=_format_skills_for_prompt,
                    select_relevant_solutions_func=_select_relevant_solutions,
                    format_solutions_for_prompt_func=_format_solutions_for_prompt,
                    collect_tools_from_solutions_func=_collect_tools_from_solutions,
                    assess_knowledge_sufficiency_func=_assess_knowledge_sufficiency,
                    compose_skills_func=_compose_skills,
                    draft_skill_from_message_func=_draft_skill_from_message,
                    draft_solution_from_skills_func=_draft_solution_from_skills,
                    create_skill_func=create_skill,
                    publish_skill_file_func=publish_skill_file,
                )
            ):
                if event_type == "msg":
                    yield lifecycle.emit(str(event_payload))
                    continue
                retrieval_result = event_payload
            if not isinstance(retrieval_result, dict):
                raise RuntimeError("think retrieval merge 结果为空")

            merged_graph_nodes = list(retrieval_result.get("graph_nodes") or [])
            graph_hint = str(retrieval_result.get("graph_hint") or "")
            memories_hint = str(retrieval_result.get("memories_hint") or "(无)")
            domain_ids = list(retrieval_result.get("domain_ids") or ["misc"])
            merged_skills = list(retrieval_result.get("skills") or [])
            skills_hint = str(retrieval_result.get("skills_hint") or "(无)")
            merged_solutions = list(retrieval_result.get("solutions") or [])
            solutions_hint = str(retrieval_result.get("solutions_hint") or "(无)")
            tools_hint = str(retrieval_result.get("tools_hint") or "(无)")
            draft_solution_id = retrieval_result.get("draft_solution_id")
            draft_solution_id_value = parse_positive_int(draft_solution_id, default=None)
            planner_hints = (
                dict(retrieval_result.get("planner_hints") or {})
                if isinstance(retrieval_result.get("planner_hints"), dict)
                else {}
            )
            need_user_prompt = bool(retrieval_result.get("need_user_prompt"))
            user_prompt_question = str(retrieval_result.get("user_prompt_question") or "").strip()

            if need_user_prompt and user_prompt_question and task_id is not None and run_id is not None:
                wait_result = None
                async for event_type, event_payload in iter_pending_planning_wait_events(
                    config=PendingPlanningWaitConfig(
                        task_id=int(task_id),
                        run_id=int(run_id),
                        mode="think",
                        message=message,
                        workdir=workdir,
                        model=model,
                        parameters=parameters,
                        max_steps=normalized_max_steps,
                        user_prompt_question=user_prompt_question,
                        tools_hint=tools_hint,
                        skills_hint=skills_hint,
                        solutions_hint=solutions_hint,
                        memories_hint=memories_hint,
                        graph_hint=graph_hint,
                        domain_ids=list(domain_ids or []),
                        skills=list(merged_skills or []),
                        solutions=list(merged_solutions or []),
                        draft_solution_id=draft_solution_id_value,
                        think_config=payload.think_config if isinstance(payload.think_config, dict) else None,
                        safe_write_debug_func=_safe_write_debug,
                    )
                ):
                    if event_type == "msg":
                        yield lifecycle.emit(str(event_payload))
                        continue
                    wait_result = event_payload
                if wait_result is None:
                    raise RuntimeError("think pending_planning waiting 结果为空")
                logger.info(
                    "[agent.think] waiting_user_input task_id=%s run_id=%s question=%s",
                    task_id,
                    run_id,
                    _log_text_brief(user_prompt_question),
                )
                await lifecycle.release_queue_ticket_once()
                return

            # --- Think 模式规划（六阶段头脑风暴）---
            yield lifecycle.emit(sse_json({"delta": f"{STREAM_TAG_THINK} 开始多模型协作规划…\n"}))
            if run_ctx is None:
                run_ctx = AgentRunContext.from_agent_state(
                    {},
                    mode="think",
                    message=message,
                    model=model,
                    parameters=parameters,
                    max_steps=normalized_max_steps,
                    workdir=workdir,
                )
            run_ctx.set_extra("think_config", payload.think_config)
            _, _, stage_event = await persist_run_stage(
                run_ctx=run_ctx,
                task_id=task_id,
                run_id=int(run_id),
                stage="planning",
                where="think_run.stage.planning",
                safe_write_debug=_safe_write_debug,
            )
            if stage_event:
                yield lifecycle.emit(stage_event)
            # 开发阶段：规划步数不设上限（用超大值近似无限），避免被 max_steps 门槛截断。
            planning_max_steps = int(AGENT_MAX_STEPS_UNLIMITED)

            llm_call_func = create_llm_call_func(
                base_model=model,
                base_parameters=parameters,
                on_error=lambda err: logger.warning("Think LLM call error: %s", err),
            )

            # 收集规划进度消息
            progress_messages: List[str] = []

            def collect_progress(msg: str):
                progress_messages.append(msg)

            plan_started_at = time.monotonic()

            # 执行 Think 规划
            think_plan_result: ThinkPlanResult = await asyncio.to_thread(
                run_think_planning_sync,
                config=think_config,
                message=message,
                workdir=workdir,
                graph_hint=graph_hint,
                skills_hint=skills_hint,
                solutions_hint=solutions_hint,
                tools_hint=tools_hint,
                max_steps=planning_max_steps,
                llm_call_func=llm_call_func,
                yield_progress=collect_progress,
                planner_hints=planner_hints if isinstance(planner_hints, dict) else None,
            )

            # 输出规划进度
            for msg in progress_messages:
                yield lifecycle.emit(sse_json({"delta": f"{msg}\n"}))

            duration_ms = int((time.monotonic() - plan_started_at) * 1000)
            _safe_write_debug(
                task_id,
                run_id,
                message="agent.think.plan.done",
                data={
                    "duration_ms": duration_ms,
                    "steps": len(think_plan_result.plan_titles or []),
                    "winning_planner": think_plan_result.winning_planner_id,
                    "vote_records": think_plan_result.vote_records,
                },
                level="info",
            )

            if not think_plan_result.plan_titles:
                await asyncio.to_thread(
                    mark_run_failed,
                    task_id=int(task_id),
                    run_id=int(run_id),
                    reason="think_planning_empty",
                )
                status_event = lifecycle.emit_run_status(RUN_STATUS_FAILED)
                if status_event:
                    yield status_event
                yield lifecycle.emit(sse_json({"message": "Think 模式规划失败：未生成有效计划"}, event="error"))
                logger.warning(
                    "[agent.think] planning_failed_empty task_id=%s run_id=%s",
                    task_id,
                    run_id,
                )
                await lifecycle.release_queue_ticket_once()
                return

            plan_titles = think_plan_result.plan_titles
            plan_briefs = think_plan_result.plan_briefs
            plan_allows = think_plan_result.plan_allows
            plan_artifacts = think_plan_result.plan_artifacts

            # 统一收敛到 PlanStructure，避免 titles/items/allows 手工对齐。
            seed_items = []
            for i, title in enumerate(plan_titles or []):
                brief = plan_briefs[i] if i < len(plan_briefs) else ""
                seed_items.append({"id": i + 1, "title": title, "brief": brief, "status": "pending"})
            plan_struct = PlanStructure.from_legacy(
                plan_titles=list(plan_titles or []),
                plan_items=seed_items,
                plan_allows=list(plan_allows or []),
                plan_artifacts=list(plan_artifacts or []),
            )
            plan_titles, plan_items, plan_allows, plan_artifacts = plan_struct.to_legacy_lists()
            plan_briefs = [str((item or {}).get("brief") or "") for item in plan_items]

            # 统一规范反馈步骤，避免计划中间残留“确认满意度”导致提前进入 waiting。
            feedback_canonicalized = canonicalize_task_feedback_steps(
                plan_titles=plan_titles,
                plan_items=plan_items,
                plan_allows=plan_allows,
                keep_single_tail=True,
                feedback_asked=False,
                max_steps=normalized_max_steps,
            )
            feedback_meta = canonicalized_feedback_meta(feedback_canonicalized)
            if feedback_meta["changed"]:
                _safe_write_debug(
                    task_id=task_id,
                    run_id=run_id,
                    message="agent.think.plan.feedback_step_canonicalized",
                    data={
                        "found": feedback_meta["found"],
                        "removed": feedback_meta["removed"],
                        "appended": feedback_meta["appended"],
                    },
                    level="info",
                )
            plan_struct = PlanStructure.from_legacy(
                plan_titles=list(plan_titles or []),
                plan_items=list(plan_items or []),
                plan_allows=[list(value or []) for value in (plan_allows or [])],
                plan_artifacts=list(plan_artifacts or []),
            )
            plan_titles, plan_items, plan_allows, plan_artifacts = plan_struct.to_legacy_lists()
            logger.info(
                "[agent.think] plan_ready task_id=%s run_id=%s steps=%s artifacts=%s winner=%s",
                task_id,
                run_id,
                len(plan_titles or []),
                len(plan_artifacts or []),
                str(think_plan_result.winning_planner_id or ""),
            )

            yield lifecycle.emit(sse_json({"type": "plan", "task_id": task_id, "run_id": run_id, "items": plan_items}))

            # 持久化 agent 运行态
            base_state = build_base_agent_state(
                message=message,
                model=model,
                parameters=parameters,
                max_steps=max_steps,
                workdir=workdir,
                tools_hint=tools_hint,
                skills_hint=skills_hint,
                memories_hint=memories_hint,
                graph_hint=graph_hint,
            )
            run_ctx = AgentRunContext.from_agent_state(base_state, mode="think")
            run_ctx.set_hints(solutions_hint=solutions_hint)
            # 用于后处理“方案沉淀/溯源”（docs/agent 依赖）
            apply_knowledge_identity_to_run_ctx(
                run_ctx,
                domain_ids=list(domain_ids or []),
                skills=list(merged_skills or []),
                solutions=list(merged_solutions or []),
                draft_solution_id=draft_solution_id_value,
            )
            run_ctx.set_extra("think_config", payload.think_config)
            run_ctx.set_extra("winning_planner_id", think_plan_result.winning_planner_id)
            run_ctx.set_extra("alternative_plans", think_plan_result.alternative_plans)
            run_ctx.set_extra("vote_records", think_plan_result.vote_records)
            # docs/agent 规划输出字段别名（不影响既有读取逻辑）
            run_ctx.set_extra("plan_alternatives", think_plan_result.alternative_plans)
            run_ctx.set_extra("plan_votes", think_plan_result.vote_records)
            run_ctx.set_stage("planned", now_iso())
            agent_state = run_ctx.to_agent_state()

            try:
                await persist_agent_state(
                    run_id=int(run_id),
                    agent_plan=plan_struct.to_agent_plan_payload(),
                    agent_state=agent_state,
                )
            except NON_FATAL_STREAM_ERRORS as exc:
                logger.exception("agent.think.state.persist_failed: %s", exc)

            if dry_run:
                yield lifecycle.emit(sse_json({"delta": f"{STREAM_TAG_EXEC} dry_run: 已生成步骤，未执行。\n"}))
                await asyncio.to_thread(
                    finalize_run_and_task_status,
                    task_id=int(task_id),
                    run_id=int(run_id),
                    run_status=RUN_STATUS_DONE,
                )
                run_status = RUN_STATUS_DONE
                status_event = lifecycle.emit_run_status(run_status)
                if status_event:
                    yield status_event
                logger.info(
                    "[agent.think] done_dry_run task_id=%s run_id=%s status=%s",
                    task_id,
                    run_id,
                    RUN_STATUS_DONE,
                )
                await lifecycle.release_queue_ticket_once()
                return

            yield lifecycle.emit(sse_json({"delta": f"{STREAM_TAG_EXEC} 开始执行…\n"}))
            if run_ctx is None:
                run_ctx = AgentRunContext.from_agent_state(agent_state, mode="think")
            agent_state, _, stage_event = await persist_run_stage(
                run_ctx=run_ctx,
                task_id=task_id,
                run_id=int(run_id),
                stage="execute",
                where="think_run.stage.execute",
                safe_write_debug=_safe_write_debug,
            )
            if stage_event:
                yield lifecycle.emit(stage_event)

            base_dependencies = None
            try:
                if think_plan_result.elaboration and isinstance(think_plan_result.elaboration.dependencies, list):
                    base_dependencies = list(think_plan_result.elaboration.dependencies)
            except AttributeError:
                base_dependencies = None

            think_result = None
            async for event_type, event_payload in iter_think_execution_stage_events(
                config=ThinkExecutionStageConfig(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    message=message,
                    workdir=workdir,
                    model=model,
                    parameters=parameters,
                    think_config=think_config,
                    llm_call_func=llm_call_func,
                    plan_struct=plan_struct,
                    plan_briefs=plan_briefs,
                    tools_hint=tools_hint,
                    skills_hint=skills_hint,
                    memories_hint=memories_hint,
                    graph_hint=graph_hint,
                    agent_state=agent_state,
                    run_ctx=run_ctx,
                    safe_write_debug=_safe_write_debug,
                    persist_agent_state_func=persist_agent_state,
                    non_fatal_errors=NON_FATAL_STREAM_ERRORS,
                    base_dependencies=base_dependencies,
                    start_step_order=1,
                    initial_reflection_count=0,
                    max_reflection_rounds=int(THINK_REFLECTION_MAX_ROUNDS or 2),
                    parallel_variables_source="agent_think_parallel",
                    tail_variables_source="agent_think_react_tail",
                    parallel_pump_label="think_parallel",
                    tail_pump_label="think_react_tail",
                    exec_done_debug_message="agent.think.exec.done",
                    reflection_mark_skipped_debug_message="agent.think.reflection.mark_failed_step_skipped_failed",
                    reflection_persist_failed_debug_message="agent.think.reflection.plan.persist_failed",
                    run_think_parallel_loop_func=run_think_parallel_loop,
                    run_reflection_func=run_reflection,
                    poll_interval_seconds=AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
                    idle_timeout_seconds=AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
                )
            ):
                if event_type == "msg":
                    yield lifecycle.emit(str(event_payload))
                    continue
                think_result = event_payload
            if think_result is None:
                raise RuntimeError("think execution 结果为空")
            run_status = str(think_result.run_status or "")
            logger.info(
                "[agent.think] execution_done task_id=%s run_id=%s status=%s",
                task_id,
                run_id,
                run_status,
            )
            if not isinstance(getattr(think_result, "plan_struct", None), PlanStructure):
                raise RuntimeError("think 执行器返回缺少有效 plan_struct")
            plan_struct = think_result.plan_struct
            plan_titles, plan_items, plan_allows, plan_artifacts = plan_struct.to_legacy_lists()
            plan_briefs = list(think_result.plan_briefs or [])
            agent_state = dict(think_result.agent_state or {})

            async for event_type, event_payload in iter_finalization_events(
                task_id=int(task_id),
                run_id=int(run_id),
                run_status=str(run_status),
                agent_state=agent_state,
                plan_items=plan_items,
                plan_artifacts=plan_artifacts,
                message=message,
                workdir=workdir,
            ):
                if event_type == "msg":
                    yield lifecycle.emit(str(event_payload))
                elif event_type == "status":
                    run_status = str(event_payload or "")
                    status_event = lifecycle.emit_run_status(run_status)
                    if status_event:
                        yield status_event
            logger.info(
                "[agent.think] finalized task_id=%s run_id=%s status=%s",
                task_id,
                run_id,
                run_status,
            )

        except (asyncio.CancelledError, GeneratorExit):
            logger.info("[agent.think] cancelled task_id=%s run_id=%s", task_id, run_id)
            handle_stream_cancellation(task_id=task_id, run_id=run_id, reason="agent_think_stream_cancelled")
            await lifecycle.release_queue_ticket_once()
            raise
        except Exception as exc:
            logger.exception("[agent.think] exception task_id=%s run_id=%s error=%s", task_id, run_id, exc)
            async for chunk in iter_stream_exception_tail(
                lifecycle=lifecycle,
                exc=exc,
                mode_prefix="agent.think",
            ):
                yield chunk

        try:
            async for chunk in iter_stream_done_tail(
                lifecycle=lifecycle,
                run_status=run_status,
            ):
                yield chunk
        finally:
            await lifecycle.release_queue_ticket_once()

    return create_sse_response(gen)
