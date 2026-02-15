import asyncio
import logging
import os
from typing import AsyncGenerator, List, Optional

from fastapi.responses import StreamingResponse

from backend.src.agent.core.run_context import AgentRunContext
from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.planning_phase import run_planning_phase
from backend.src.agent.runner.debug_utils import safe_write_debug as _safe_write_debug
from backend.src.agent.runner.resume_mode_execution_runner import (
    ResumeModeExecutionConfig,
    iter_resume_mode_execution_events,
)
from backend.src.agent.runner.resume_finalization_runner import (
    ResumeFinalizationConfig,
    iter_resume_finalization_events,
)
from backend.src.agent.runner.resume_preflight import (
    apply_resume_user_input,
    finalize_skip_execution_resume,
    infer_resume_step_decision,
    normalize_plan_items_for_resume,
)
from backend.src.agent.runner.stream_task_events import iter_stream_task_events
from backend.src.agent.runner.stream_entry_common import done_sse_event
from backend.src.agent.runner.think_parallel_loop import run_think_parallel_loop
from backend.src.agent.runner.execution_pipeline import (
    resume_pending_planning_after_user_input,
    retrieve_all_knowledge,
)
from backend.src.agent.support import (
    _assess_knowledge_sufficiency,
    _collect_tools_from_solutions,
    _compose_skills,
    _draft_skill_from_message,
    _draft_solution_from_skills,
    _extract_json_object,
    _filter_relevant_domains,
    _format_graph_for_prompt,
    _format_skills_for_prompt,
    _format_solutions_for_prompt,
    _list_tool_hints,
    _select_relevant_graph_nodes,
    _select_relevant_skills,
    _select_relevant_solutions,
    _truncate_observation,
)
from backend.src.agent.think import (
    run_reflection,
    run_think_planning_sync,
)
from backend.src.api.schemas import AgentCommandResumeStreamRequest
from backend.src.common.utils import error_response
from backend.src.constants import (
    AGENT_DEFAULT_MAX_STEPS,
    AGENT_PLAN_RESERVED_STEPS,
    ERROR_CODE_INVALID_REQUEST,
    ERROR_MESSAGE_LLM_CHAT_MESSAGE_MISSING,
    HTTP_STATUS_BAD_REQUEST,
    RUN_STATUS_FAILED,
    RUN_STATUS_STOPPED,
    RUN_STATUS_WAITING,
    STEP_STATUS_DONE,
    STREAM_TAG_EXEC,
)
from backend.src.agent.runner.feedback import append_task_feedback_step, is_task_feedback_step_title
from backend.src.repositories.skills_repo import create_skill
from backend.src.repositories.task_runs_repo import get_task_run
from backend.src.repositories.task_steps_repo import get_max_step_order_for_run_by_status
from backend.src.repositories.task_steps_repo import get_last_non_planned_step_for_run
from backend.src.repositories.tasks_repo import get_task
from backend.src.services.llm.llm_client import resolve_default_model, sse_json
from backend.src.services.permissions.permission_checks import ensure_write_permission
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


def stream_agent_command_resume(payload: AgentCommandResumeStreamRequest):
    """
    继续执行一个进入 waiting/stopped 的 agent run。

    说明：
    - 前端收到 need_input 后，用 run_id + 用户回答调用该接口
    - 该接口会在同一个 run_id 上继续执行剩余计划，并继续以 SSE 回传进度
    """
    permission = ensure_write_permission()
    if permission:
        return permission

    user_input = (payload.message or "").strip()
    if not user_input:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            ERROR_MESSAGE_LLM_CHAT_MESSAGE_MISSING,
            HTTP_STATUS_BAD_REQUEST,
        )

    run_id = int(payload.run_id)
    run_row = get_task_run(run_id=run_id)
    if not run_row:
        return error_response(ERROR_CODE_INVALID_REQUEST, "run 不存在", HTTP_STATUS_BAD_REQUEST)
    if run_row["status"] not in {RUN_STATUS_WAITING, RUN_STATUS_STOPPED, RUN_STATUS_FAILED}:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            f"run 当前状态不支持继续执行: {run_row['status']}",
            HTTP_STATUS_BAD_REQUEST,
        )

    async def gen() -> AsyncGenerator[str, None]:
        task_id: Optional[int] = None
        try:
            run = await asyncio.to_thread(get_task_run, run_id=run_id)
            if not run:
                yield sse_json({"message": "run 不存在"}, event="error")
                return
            task_id = int(run["task_id"])
            task_row = await asyncio.to_thread(get_task, task_id=int(task_id))

            # stopped run 继续执行：优先用已完成 steps 推断下一步（比 agent_state 更可信）
            last_done_step = await asyncio.to_thread(
                get_max_step_order_for_run_by_status,
                task_id=int(task_id),
                run_id=int(run_id),
                status=STEP_STATUS_DONE,
            )
            last_active_step = await asyncio.to_thread(
                get_last_non_planned_step_for_run,
                task_id=int(task_id),
                run_id=int(run_id),
            )
            last_active_step_order = 0
            last_active_step_status = ""
            if last_active_step is not None:
                try:
                    last_active_step_order = int(last_active_step["step_order"] or 0)
                except (TypeError, ValueError, KeyError):
                    last_active_step_order = 0
                last_active_step_status = str(last_active_step["status"] or "").strip()

            plan_obj = _extract_json_object(run["agent_plan"] or "") or {}
            plan_struct = PlanStructure.from_agent_plan_payload(plan_obj)
            plan_titles, plan_items, plan_allows, plan_artifacts = plan_struct.to_legacy_lists()

            if not plan_titles:
                yield sse_json({"message": "agent_plan 缺失，无法 resume"}, event="error")
                return

            state_obj_raw = _extract_json_object(run["agent_state"] or "") or {}
            message = str(state_obj_raw.get("message") or "").strip() or (str(task_row["title"]) if task_row else "")
            workdir = str(state_obj_raw.get("workdir") or "").strip() or os.getcwd()
            tools_hint = str(state_obj_raw.get("tools_hint") or "").strip() or _list_tool_hints()
            skills_hint = str(state_obj_raw.get("skills_hint") or "").strip() or "(无)"
            # 文档约定：Memory 不注入 Agent 上下文
            memories_hint = "(无)"
            graph_hint = str(state_obj_raw.get("graph_hint") or "").strip()
            model = str(state_obj_raw.get("model") or "").strip()
            if not model:
                model = await asyncio.to_thread(resolve_default_model)
            parameters = (
                state_obj_raw.get("parameters")
                if isinstance(state_obj_raw.get("parameters"), dict)
                else {"temperature": 0.2}
            )
            raw_max_steps = state_obj_raw.get("max_steps")
            try:
                run_max_steps: Optional[int] = int(raw_max_steps) if raw_max_steps is not None else None
            except (TypeError, ValueError):
                run_max_steps = None

            # 兼容：旧 run 没有保存 graph_hint，则重新检索一次（避免 prompt.format KeyError）
            if not graph_hint:
                graph_nodes = await asyncio.to_thread(
                    _select_relevant_graph_nodes,
                    message=message,
                    model=model,
                    parameters=parameters,
                )
                graph_hint = _format_graph_for_prompt(graph_nodes)

            run_ctx = AgentRunContext.from_agent_state(
                state_obj_raw,
                mode=str(state_obj_raw.get("mode") or "").strip().lower() or None,
                message=message,
                model=model,
                parameters=parameters,
                max_steps=run_max_steps,
                workdir=workdir,
                tools_hint=tools_hint,
                skills_hint=skills_hint,
                solutions_hint=str(state_obj_raw.get("solutions_hint") or "").strip() or "(无)",
                memories_hint=memories_hint,
                graph_hint=graph_hint,
            )
            state_obj = run_ctx.to_agent_state()
            context = run_ctx.context
            observations = [str(o).strip() for o in run_ctx.observations if str(o).strip()]
            run_ctx.observations.clear()
            run_ctx.observations.extend(list(observations))

            paused = state_obj.get("paused") if isinstance(state_obj.get("paused"), dict) else {}
            paused_step_order_raw = paused.get("step_order")
            paused_step_order: Optional[int]
            try:
                paused_step_order = int(paused_step_order_raw) if paused_step_order_raw is not None else None
            except (TypeError, ValueError):
                paused_step_order = None

            plan_total_steps = len(plan_titles)
            # 特殊：pending_planning 表示“等待用户补充后再重新规划”，即便当前 plan 只有 user_prompt 也不应判定为已完成。
            pending_planning = bool(state_obj.get("pending_planning")) if isinstance(state_obj, dict) else False
            resume_decision = infer_resume_step_decision(
                paused_step_order=paused_step_order,
                state_step_order=state_obj.get("step_order"),
                last_done_step=int(last_done_step),
                last_active_step_order=int(last_active_step_order),
                last_active_step_status=str(last_active_step_status or ""),
                plan_total_steps=int(plan_total_steps),
                pending_planning=bool(pending_planning),
            )
            resume_step_order = int(resume_decision.resume_step_order)
            skip_execution = bool(resume_decision.skip_execution)

            # 继续执行前先把计划栏状态对齐到“已完成/待执行”，避免 UI 显示漂移
            normalize_plan_items_for_resume(
                plan_items=plan_items,
                last_done_step=int(last_done_step),
            )
            plan_struct = PlanStructure.from_legacy(
                plan_titles=list(plan_titles or []),
                plan_items=list(plan_items or []),
                plan_allows=[list(value or []) for value in (plan_allows or [])],
                plan_artifacts=list(plan_artifacts or []),
            )
            plan_titles, plan_items, plan_allows, plan_artifacts = plan_struct.to_legacy_lists()

            run_status: str
            last_step_order: int = 0
            if skip_execution:
                last_step_order = int(plan_total_steps)
                _safe_write_debug(
                    task_id,
                    run_id,
                    message="agent.resume.skip_execution_plan_already_done",
                    data={
                        "last_active_step_order": int(last_active_step_order),
                        "last_active_step_status": str(last_active_step_status),
                    },
                    level="info",
                )
                run_status, skip_events = await finalize_skip_execution_resume(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    workdir=workdir,
                    plan_titles=plan_titles,
                    plan_items=plan_items,
                    plan_allows=plan_allows,
                    plan_artifacts=plan_artifacts,
                    state_obj=state_obj,
                    task_row=task_row,
                    safe_write_debug=_safe_write_debug,
                )
                for event in skip_events:
                    yield str(event)
                yield done_sse_event()
                return

            question = str(paused.get("question") or "").strip()
            if not skip_execution:
                answer_line = f"user_input: {_truncate_observation(user_input)}"
                if question:
                    answer_line = f"user_prompt: {_truncate_observation(question)} => {_truncate_observation(user_input)}"
                observations.append(answer_line)

            if not skip_execution:
                plan_titles, plan_items, plan_allows, plan_artifacts = plan_struct.to_legacy_lists()
                resume_step_order, state_obj = await apply_resume_user_input(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    user_input=str(user_input or ""),
                    question=str(question or ""),
                    paused=paused if isinstance(paused, dict) else {},
                    paused_step_order=paused_step_order,
                    resume_step_order=int(resume_step_order),
                    plan_titles=plan_titles,
                    plan_items=plan_items,
                    plan_allows=plan_allows,
                    plan_artifacts=plan_artifacts,
                    observations=observations,
                    context=context,
                    state_obj=state_obj,
                    safe_write_debug=_safe_write_debug,
                    is_task_feedback_step_title_func=is_task_feedback_step_title,
                )
                plan_struct = PlanStructure.from_legacy(
                    plan_titles=list(plan_titles or []),
                    plan_items=list(plan_items or []),
                    plan_allows=[list(value or []) for value in (plan_allows or [])],
                    plan_artifacts=list(plan_artifacts or []),
                )
                plan_titles, plan_items, plan_allows, plan_artifacts = plan_struct.to_legacy_lists()

            yield sse_json({"delta": f"{STREAM_TAG_EXEC} 已收到输入，继续执行…\n"})

            # docs/agent：知识充分性建议 ask_user 时，会先进入 waiting；用户补充后需要“重新检索 + 重新规划”再执行。
            if isinstance(state_obj, dict) and bool(state_obj.get("pending_planning")):
                pending_result = None
                async for event_type, event_payload in iter_stream_task_events(
                    task_builder=lambda emit: resume_pending_planning_after_user_input(
                        task_id=int(task_id),
                        run_id=int(run_id),
                        user_input=str(user_input or ""),
                        message=message,
                        workdir=workdir,
                        model=model,
                        parameters=parameters,
                        agent_state=state_obj,
                        paused=paused,
                        plan_titles=plan_titles,
                        plan_items=plan_items,
                        plan_allows=plan_allows,
                        select_graph_nodes_func=_select_relevant_graph_nodes,
                        format_graph_for_prompt_func=_format_graph_for_prompt,
                        filter_relevant_domains_func=_filter_relevant_domains,
                        select_skills_func=_select_relevant_skills,
                        format_skills_for_prompt_func=_format_skills_for_prompt,
                        select_solutions_func=_select_relevant_solutions,
                        format_solutions_for_prompt_func=_format_solutions_for_prompt,
                        collect_tools_from_solutions_func=_collect_tools_from_solutions,
                        assess_knowledge_sufficiency_func=_assess_knowledge_sufficiency,
                        compose_skills_func=_compose_skills,
                        draft_skill_from_message_func=_draft_skill_from_message,
                        draft_solution_from_skills_func=_draft_solution_from_skills,
                        create_skill_func=create_skill,
                        publish_skill_file_func=publish_skill_file,
                        run_planning_phase_func=run_planning_phase,
                        append_task_feedback_step_func=append_task_feedback_step,
                        run_think_planning_sync_func=run_think_planning_sync,
                        safe_write_debug_func=_safe_write_debug,
                        yield_func=emit,
                    )
                ):
                    if event_type == "msg":
                        yield str(event_payload)
                        continue
                    pending_result = event_payload
                if pending_result is None:
                    raise RuntimeError("pending_planning resume 结果为空")
                outcome = str(pending_result.get("outcome") or "").strip()
                if outcome in {"waiting", "failed"}:
                    return

                # outcome == planned：用新 plan/状态继续后续执行
                plan_titles = list(pending_result.get("plan_titles") or plan_titles)
                plan_items = list(pending_result.get("plan_items") or plan_items)
                plan_allows = list(pending_result.get("plan_allows") or plan_allows)
                plan_artifacts = list(pending_result.get("plan_artifacts") or plan_artifacts)
                plan_struct = PlanStructure.from_legacy(
                    plan_titles=list(plan_titles or []),
                    plan_items=list(plan_items or []),
                    plan_allows=[list(value or []) for value in (plan_allows or [])],
                    plan_artifacts=list(plan_artifacts or []),
                )
                plan_titles, plan_items, plan_allows, plan_artifacts = plan_struct.to_legacy_lists()
                state_obj = pending_result.get("agent_state") if isinstance(pending_result.get("agent_state"), dict) else state_obj
                message = str(pending_result.get("message") or message).strip() or message
                # 更新运行时 hints：后续执行器读取的是本地变量（而非 state_obj），必须同步。
                tools_hint = str(state_obj.get("tools_hint") or tools_hint).strip() or tools_hint
                skills_hint = str(state_obj.get("skills_hint") or skills_hint).strip() or skills_hint
                solutions_hint = str(state_obj.get("solutions_hint") or solutions_hint).strip() or solutions_hint
                graph_hint = str(state_obj.get("graph_hint") or graph_hint).strip() or graph_hint
                try:
                    resume_step_order = int(pending_result.get("resume_step_order") or resume_step_order)
                except (TypeError, ValueError):
                    resume_step_order = int(resume_step_order)

            mode = str(state_obj.get("mode") or "").strip().lower()
            run_status: str
            last_step_order: int = 0
            mode_result = None
            async for event_type, event_payload in iter_resume_mode_execution_events(
                config=ResumeModeExecutionConfig(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    mode=mode,
                    message=message,
                    workdir=workdir,
                    model=model,
                    parameters=parameters,
                    plan_struct=plan_struct,
                    tools_hint=tools_hint,
                    skills_hint=skills_hint,
                    memories_hint=memories_hint,
                    graph_hint=graph_hint,
                    state_obj=state_obj,
                    context=context,
                    observations=observations,
                    resume_step_order=int(resume_step_order),
                    safe_write_debug=_safe_write_debug,
                    parallel_loop_runner=run_think_parallel_loop,
                    reflection_runner=run_reflection,
                )
            ):
                if event_type == "msg":
                    yield str(event_payload)
                    continue
                mode_result = event_payload

            if not isinstance(mode_result, dict):
                raise RuntimeError("resume mode execution 结果为空")

            run_status = str(mode_result.get("run_status") or "")
            last_step_order = int(mode_result.get("last_step_order") or 0)
            state_obj = dict(mode_result.get("state_obj") or state_obj)
            mode_plan_struct = mode_result.get("plan_struct")
            if not isinstance(mode_plan_struct, PlanStructure):
                raise RuntimeError("resume mode execution 返回缺少有效 plan_struct")
            plan_struct = mode_plan_struct
            plan_titles, plan_items, plan_allows, plan_artifacts = plan_struct.to_legacy_lists()

            final_title = str(task_row["title"] or "").strip() if task_row else ""
            async for event_type, event_payload in iter_resume_finalization_events(
                config=ResumeFinalizationConfig(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    run_status=str(run_status),
                    workdir=workdir,
                    message_title=final_title,
                    agent_state=state_obj,
                    plan_items=plan_items,
                    plan_artifacts=plan_artifacts,
                    safe_write_debug=_safe_write_debug,
                    check_missing_artifacts=check_missing_artifacts,
                    finalize_run_and_task_status=finalize_run_and_task_status,
                    enqueue_review_on_feedback_waiting=enqueue_review_on_feedback_waiting,
                    enqueue_postprocess_thread=enqueue_postprocess_thread,
                )
            ):
                if event_type == "msg":
                    yield str(event_payload)
                    continue
                if event_type == "status":
                    run_status = str(event_payload or run_status)

        except (asyncio.CancelledError, GeneratorExit):
            # SSE 连接被关闭（客户端断开/窗口退出）时不要再尝试继续 yield，否则会触发
            # “async generator ignored GeneratorExit/CancelledError” 类错误并留下 running 状态。
            enqueue_stop_task_run_records(task_id=task_id, run_id=int(run_id), reason="agent_stream_cancelled")
            raise
        except Exception as exc:
            # 兜底：任何未捕获异常都必须把状态收敛到 failed，避免 UI 永久显示 running
            if task_id is not None:
                try:
                    await asyncio.to_thread(
                        mark_run_failed,
                        task_id=int(task_id),
                        run_id=int(run_id),
                        reason=f"exception:{exc}",
                    )
                except Exception as persist_exc:
                    logger.exception("agent.exception.persist_failed: %s", persist_exc)
                    _safe_write_debug(
                        task_id,
                        run_id,
                        message="agent.exception.persist_failed",
                        data={"error": str(persist_exc)},
                        level="error",
                    )
                _safe_write_debug(
                    task_id,
                    run_id,
                    message="agent.exception",
                    data={"error": f"{exc}"},
                    level="error",
                )
            try:
                yield sse_json({"message": f"agent resume 执行失败:{exc}"}, event="error")
            except BaseException:
                return

        # 正常结束/异常结束均尽量发送 done；若客户端已断开则直接结束 generator。
        try:
            yield done_sse_event()
        except BaseException:
            return

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(gen(), media_type="text/event-stream", headers=headers)
