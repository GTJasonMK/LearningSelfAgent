import asyncio
import logging
import os
import sqlite3
from typing import AsyncGenerator, Dict, List, Optional

from backend.src.agent.core.run_context import AgentRunContext
from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.planning_phase import PlanPhaseFailure, run_planning_phase
from backend.src.agent.runner.feedback import canonicalize_task_feedback_steps, canonicalized_feedback_meta
from backend.src.agent.runner.mode_do_runner import DoExecutionConfig, run_do_mode_execution_from_config
from backend.src.agent.runner.debug_utils import safe_write_debug as _safe_write_debug
from backend.src.agent.runner.planning_runner import run_do_planning_phase_with_stream
from backend.src.agent.runner.run_startup import start_new_mode_run
from backend.src.agent.runner.run_stage import persist_run_stage
from backend.src.agent.runner.pending_planning_wait_runner import (
    PendingPlanningWaitConfig,
    iter_pending_planning_wait_events,
)
from backend.src.agent.runner.planning_enrich_runner import (
    PlanningEnrichRunConfig,
    iter_planning_enrich_events,
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
from backend.src.agent.runner.execution_pipeline import (
    create_sse_response,
    prepare_planning_knowledge_do,
    handle_stream_cancellation,
    build_base_agent_state,
    persist_agent_state,
    retrieve_all_knowledge,
)
from backend.src.agent.support import (
    _assess_knowledge_sufficiency,
    _collect_tools_from_solutions,
    _compose_skills,
    _draft_skill_from_message,
    _draft_solution_from_skills,
    _filter_relevant_domains,
    _format_graph_for_prompt,
    _format_skills_for_prompt,
    _format_solutions_for_prompt,
    _select_relevant_graph_nodes,
    _select_relevant_skills,
    _select_relevant_solutions,
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
    RUN_STATUS_WAITING,
    STREAM_TAG_DOMAIN,
    STREAM_TAG_EXEC,
    STREAM_TAG_FAIL,
    STREAM_TAG_GRAPH,
    STREAM_TAG_KNOWLEDGE,
    STREAM_TAG_SKILLS,
    STREAM_TAG_SOLUTIONS,
    SSE_TYPE_MEMORY_ITEM,
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
NON_FATAL_STREAM_ERRORS = (sqlite3.Error, RuntimeError, TypeError, ValueError, OSError, AttributeError)


@require_write_permission_stream
def stream_agent_command(payload: AgentCommandStreamRequest):
    """
    自然语言指令执行（SSE 流式）：
    - 创建 task/run
    - 检索（graph/memory/skills）
    - 规划 plan（含 allow + artifacts）
    - ReAct 执行（逐步 action -> 执行 -> 观测 -> 下一步）
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

    async def gen() -> AsyncGenerator[str, None]:
        task_id: Optional[int] = None
        run_id: Optional[int] = None
        run_status: str = ""
        lifecycle = StreamModeLifecycle()
        plan_items: List[dict] = []
        plan_struct = PlanStructure(steps=[], artifacts=[])
        run_ctx: Optional[AgentRunContext] = None

        try:
            workdir = os.getcwd()

            startup = await bootstrap_stream_mode_lifecycle(
                lifecycle=lifecycle,
                start_mode_run_func=start_new_mode_run,
                start_mode_run_kwargs={
                    "message": message,
                    "mode": "do",
                    "model": model,
                    "parameters": parameters,
                    "max_steps": normalized_max_steps,
                    "workdir": workdir,
                    "stage_where_prefix": "new_run",
                    "safe_write_debug": _safe_write_debug,
                    "start_debug_message": "agent.start",
                    "start_debug_data": {
                        "model": model,
                        "max_steps": max_steps,
                        "dry_run": dry_run,
                        "workdir": workdir,
                        "agent_workspace": AGENT_EXPERIMENT_DIR_REL,
                    },
                },
                acquire_queue_ticket_func=acquire_stream_queue_ticket,
            )
            task_id = int(startup.task_id)
            run_id = int(startup.run_id)
            run_ctx = startup.run_ctx
            for event in startup.emitted_events:
                yield str(event)
            # 工具清单会在“方案匹配”之后汇总（方案提到的工具优先）
            tools_hint = "(无)"
            solutions_hint = "(无)"

            # --- 检索：图谱→领域→技能→方案（收敛到 execution_pipeline）---
            knowledge = None
            async for event_type, event_payload in iter_stream_task_events(
                task_builder=lambda emit: retrieve_all_knowledge(
                    message=message,
                    model=model,
                    parameters=parameters,
                    yield_func=emit,
                    task_id=task_id,
                    run_id=run_id,
                    include_memories=False,
                    select_graph_nodes_func=_select_relevant_graph_nodes,
                    format_graph_for_prompt_func=_format_graph_for_prompt,
                    filter_relevant_domains_func=_filter_relevant_domains,
                    select_skills_func=_select_relevant_skills,
                    format_skills_for_prompt_func=_format_skills_for_prompt,
                    select_solutions_func=_select_relevant_solutions,
                    format_solutions_for_prompt_func=_format_solutions_for_prompt,
                    collect_tools_from_solutions_func=_collect_tools_from_solutions,
                )
            ):
                if event_type == "msg":
                    yield lifecycle.emit(str(event_payload))
                    continue
                knowledge = event_payload
            if not isinstance(knowledge, dict):
                raise RuntimeError("knowledge retrieval 结果为空")

            graph_nodes = list(knowledge.get("graph_nodes") or [])
            graph_hint = str(knowledge.get("graph_hint") or "")
            # 文档约定：Memory 不参与检索与上下文注入（仅作为后处理沉淀原料）。
            memories = []
            memories_hint = "(无)"
            domain_ids = list(knowledge.get("domain_ids") or [])
            if not domain_ids:
                domain_ids = ["misc"]
            skills = list(knowledge.get("skills") or [])
            skills_hint = str(knowledge.get("skills_hint") or "") or "(无)"
            solutions = list(knowledge.get("solutions") or [])

            # --- do 模式 planning 前“知识增强”（收敛到 execution_pipeline）---
            enriched = None
            async for event_type, event_payload in iter_planning_enrich_events(
                config=PlanningEnrichRunConfig(
                    task_builder=lambda emit: prepare_planning_knowledge_do(
                        message=message,
                        model=model,
                        parameters=parameters,
                        graph_nodes=graph_nodes,
                        graph_hint=graph_hint,
                        domain_ids=domain_ids,
                        skills=skills,
                        skills_hint=skills_hint,
                        solutions=solutions,
                        yield_func=emit,
                        task_id=task_id,
                        run_id=run_id,
                        assess_knowledge_sufficiency_func=_assess_knowledge_sufficiency,
                        compose_skills_func=_compose_skills,
                        draft_skill_from_message_func=_draft_skill_from_message,
                        draft_solution_from_skills_func=_draft_solution_from_skills,
                        create_skill_func=create_skill,
                        publish_skill_file_func=publish_skill_file,
                        format_skills_for_prompt_func=_format_skills_for_prompt,
                        format_solutions_for_prompt_func=_format_solutions_for_prompt,
                        collect_tools_from_solutions_func=_collect_tools_from_solutions,
                    ),
                    empty_result_error="planning enrich 结果为空",
                )
                ):
                    if event_type == "msg":
                        yield lifecycle.emit(str(event_payload))
                        continue
                    enriched = event_payload
            if not isinstance(enriched, dict):
                raise RuntimeError("planning enrich 结果为空")

            skills = list(enriched.get("skills") or skills or [])
            skills_hint = str(enriched.get("skills_hint") or skills_hint or "(无)")
            solutions_for_prompt = list(enriched.get("solutions_for_prompt") or solutions or [])
            draft_solution_id = enriched.get("draft_solution_id")
            draft_solution_id_value = parse_positive_int(draft_solution_id, default=None)
            solutions_hint = str(enriched.get("solutions_hint") or "(无)")
            tools_hint = str(enriched.get("tools_hint") or "(无)")
            need_user_prompt = bool(enriched.get("need_user_prompt"))
            user_prompt_question = str(enriched.get("user_prompt_question") or "").strip()

            # --- 知识不足且需询问用户：进入 waiting，并在 resume 后重新检索+规划 ---
            if need_user_prompt and user_prompt_question and task_id is not None and run_id is not None:
                wait_result = None
                async for event_type, event_payload in iter_pending_planning_wait_events(
                    config=PendingPlanningWaitConfig(
                        task_id=int(task_id),
                        run_id=int(run_id),
                        mode="do",
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
                        skills=list(skills or []),
                        solutions=list(solutions_for_prompt or []),
                        draft_solution_id=draft_solution_id_value,
                        safe_write_debug_func=_safe_write_debug,
                    )
                ):
                    if event_type == "msg":
                        yield lifecycle.emit(str(event_payload))
                        continue
                    wait_result = event_payload
                if wait_result is None:
                    raise RuntimeError("pending_planning waiting 结果为空")
                await lifecycle.release_queue_ticket_once()
                return

            # --- 规划 ---
            try:
                if run_ctx is None:
                    run_ctx = AgentRunContext.from_agent_state(
                        {},
                        mode="do",
                        message=message,
                        model=model,
                        parameters=parameters,
                        max_steps=normalized_max_steps,
                        workdir=workdir,
                    )
                _, _, stage_event = await persist_run_stage(
                    run_ctx=run_ctx,
                    task_id=task_id,
                    run_id=int(run_id),
                    stage="planning",
                    where="new_run.stage.planning",
                    safe_write_debug=_safe_write_debug,
                )
                if stage_event:
                    yield lifecycle.emit(stage_event)
                # 开发阶段：规划步数不设上限（用超大值近似无限），避免被 max_steps 门槛截断。
                planning_max_steps = int(AGENT_MAX_STEPS_UNLIMITED)
                plan_result = None
                async for event_type, event_payload in iter_stream_task_events(
                    task_builder=lambda emit: run_do_planning_phase_with_stream(
                        task_id=int(task_id),
                        run_id=int(run_id),
                        message=message,
                        workdir=workdir,
                        model=model,
                        parameters=parameters,
                        max_steps=planning_max_steps,
                        tools_hint=tools_hint,
                        skills_hint=skills_hint,
                        solutions_hint=solutions_hint,
                        memories_hint=memories_hint,
                        graph_hint=graph_hint,
                        yield_func=emit,
                        safe_write_debug=_safe_write_debug,
                        debug_done_message="agent.plan.done",
                        pump_label="planning",
                        poll_interval_seconds=AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
                        idle_timeout_seconds=AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
                        planning_phase_func=run_planning_phase,
                    )
                ):
                    if event_type == "msg":
                        yield lifecycle.emit(str(event_payload))
                        continue
                    plan_result = event_payload
                if plan_result is None:
                    raise RuntimeError("planning 结果为空")
            except PlanPhaseFailure as exc:
                if task_id is not None and run_id is not None:
                    # 规划失败也要收敛状态：避免 UI 永久显示 running
                    await asyncio.to_thread(
                        mark_run_failed,
                        task_id=int(task_id),
                        run_id=int(run_id),
                        reason=str(exc.reason),
                    )
                    # 规划失败也要落库评估记录：否则 UI 会误以为“评估没触发”
                    enqueue_postprocess_thread(
                        task_id=int(task_id),
                        run_id=int(run_id),
                        run_status=RUN_STATUS_FAILED,
                    )
                    status_event = lifecycle.emit_run_status(RUN_STATUS_FAILED)
                    if status_event:
                        yield status_event
                yield lifecycle.emit(sse_json({"message": exc.public_message}, event="error"))
                await lifecycle.release_queue_ticket_once()
                return

            plan_titles = plan_result.plan_titles
            plan_allows = plan_result.plan_allows
            plan_artifacts = plan_result.plan_artifacts
            plan_items = plan_result.plan_items
            plan_struct = PlanStructure.from_legacy(
                plan_titles=list(plan_titles or []),
                plan_items=list(plan_items or []),
                plan_allows=[list(value or []) for value in (plan_allows or [])],
                plan_artifacts=list(plan_artifacts or []),
            )
            plan_titles, plan_items, plan_allows, plan_artifacts = plan_struct.to_legacy_lists()

            # 任务闭环：统一规范反馈步骤，避免计划中间残留“确认满意度”导致提前进入 waiting。
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
                    message="agent.plan.feedback_step_canonicalized",
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

            yield lifecycle.emit(sse_json({"type": "plan", "task_id": task_id, "run_id": run_id, "items": plan_items}))

            # 持久化 agent 运行态：用于中途需要用户交互时可恢复执行
            agent_state = build_base_agent_state(
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
            # 用于后处理“方案沉淀/溯源”（docs/agent 依赖）
            run_ctx = AgentRunContext.from_agent_state(agent_state)
            run_ctx.mode = "do"
            run_ctx.set_stage("planned", now_iso())
            run_ctx.set_hints(solutions_hint=solutions_hint)
            apply_knowledge_identity_to_run_ctx(
                run_ctx,
                domain_ids=list(domain_ids or []),
                skills=list(skills or []),
                solutions=list(solutions_for_prompt or []),
                draft_solution_id=draft_solution_id_value,
            )
            agent_state = run_ctx.to_agent_state()
            try:
                await persist_agent_state(
                    run_id=int(run_id),
                    agent_plan=plan_struct.to_agent_plan_payload(),
                    agent_state=agent_state,
                )
            except NON_FATAL_STREAM_ERRORS as exc:
                # 状态持久化失败不应阻塞执行（会降低“中断恢复/交互恢复”的能力），但必须留痕便于排查。
                logger.exception("agent.state.persist_failed: %s", exc)
                _safe_write_debug(
                    task_id,
                    run_id,
                    message="agent.state.persist_failed",
                    data={"where": "after_plan", "error": str(exc)},
                    level="warning",
                )

            if dry_run:
                yield lifecycle.emit(sse_json({"delta": f"{STREAM_TAG_EXEC} dry_run: 已生成步骤，未执行。\n"}))
                try:
                    await asyncio.to_thread(
                        finalize_run_and_task_status,
                        task_id=int(task_id),
                        run_id=int(run_id),
                        run_status=RUN_STATUS_DONE,
                    )
                    _safe_write_debug(
                        task_id,
                        run_id,
                        message="agent.dry_run.done",
                        data={"steps": len(plan_titles or [])},
                        level="info",
                    )
                    status_event = lifecycle.emit_run_status(RUN_STATUS_DONE)
                    if status_event:
                        yield status_event
                except NON_FATAL_STREAM_ERRORS as exc:
                    logger.exception("agent.dry_run.finalize_failed: %s", exc)
                    _safe_write_debug(
                        task_id,
                        run_id,
                        message="agent.dry_run.finalize_failed",
                        data={"error": str(exc)},
                        level="error",
                    )
                await lifecycle.release_queue_ticket_once()
                return

            yield lifecycle.emit(sse_json({"delta": f"{STREAM_TAG_EXEC} 开始执行…\n"}))

            if run_ctx is None:
                run_ctx = AgentRunContext.from_agent_state(agent_state)
            run_ctx.ensure_defaults()
            agent_state, _, stage_event = await persist_run_stage(
                run_ctx=run_ctx,
                task_id=task_id,
                run_id=int(run_id),
                stage="execute",
                where="new_run.stage.execute",
                safe_write_debug=_safe_write_debug,
            )
            context: dict = run_ctx.context
            observations: List[str] = run_ctx.observations
            if stage_event:
                yield lifecycle.emit(stage_event)

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
                        agent_state=agent_state,
                        context=context,
                        observations=observations,
                        start_step_order=1,
                        variables_source="agent_react",
                        yield_func=emit,
                        safe_write_debug=_safe_write_debug,
                        debug_done_message="agent.react.done",
                        pump_label="react",
                        poll_interval_seconds=AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
                        idle_timeout_seconds=AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
                    )
                )
            ):
                if event_type == "msg":
                    yield lifecycle.emit(str(event_payload))
                    continue
                react_result = event_payload
            if react_result is None:
                raise RuntimeError("do execution 结果为空")
            run_status = str(react_result.run_status or "")
            if not isinstance(getattr(react_result, "plan_struct", None), PlanStructure):
                raise RuntimeError("do 执行器返回缺少有效 plan_struct")
            plan_struct = react_result.plan_struct
            plan_titles, plan_items, plan_allows, plan_artifacts = plan_struct.to_legacy_lists()

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

        except (asyncio.CancelledError, GeneratorExit):
            # SSE 连接被关闭（客户端断开/窗口退出）时不要再尝试继续 yield，否则会触发
            # “async generator ignored GeneratorExit/CancelledError” 类错误并留下 running 状态。
            handle_stream_cancellation(task_id=task_id, run_id=run_id, reason="agent_stream_cancelled")
            await lifecycle.release_queue_ticket_once()
            raise
        except Exception as exc:
            async for chunk in iter_stream_exception_tail(
                lifecycle=lifecycle,
                exc=exc,
                mode_prefix="agent",
            ):
                yield chunk

        # 正常结束/异常结束均尽量发送 done；若客户端已断开则直接结束 generator。
        try:
            async for chunk in iter_stream_done_tail(
                lifecycle=lifecycle,
                run_status=run_status,
            ):
                yield chunk
        finally:
            await lifecycle.release_queue_ticket_once()

    return create_sse_response(gen)
