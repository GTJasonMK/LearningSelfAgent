import asyncio
import logging
import os
import threading
import time
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
from backend.src.agent.runner.stream_entry_common import (
    done_sse_event,
    require_write_permission_stream,
    StreamRunStateEmitter,
)
from backend.src.agent.contracts.stream_events import coerce_session_key
from backend.src.agent.contracts.resume_contract import validate_waiting_resume_contract
from backend.src.agent.runner.session_queue import StreamQueueTicket, acquire_stream_queue_ticket
from backend.src.agent.runner.run_config_snapshot import apply_run_config_snapshot_if_missing
from backend.src.agent.runner.session_runtime import apply_session_key_to_state, resolve_or_create_session_key
from backend.src.agent.runner.think_parallel_loop import run_think_parallel_loop
from backend.src.agent.runner.execution_pipeline import (
    ensure_failed_task_output,
    resume_pending_planning_after_user_input,
    retrieve_all_knowledge,
)
from backend.src.agent.runner.stream_status_event import normalize_stream_run_status
from backend.src.agent.runner.stream_convergence import (
    build_stream_error_payload,
    resolve_terminal_meta,
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
from backend.src.common.utils import coerce_int, error_response, parse_optional_int
from backend.src.common.task_error_codes import format_task_error
from backend.src.constants import (
    AGENT_DEFAULT_MAX_STEPS,
    AGENT_PLAN_RESERVED_STEPS,
    ERROR_CODE_INVALID_REQUEST,
    ERROR_MESSAGE_LLM_CHAT_MESSAGE_MISSING,
    HTTP_STATUS_BAD_REQUEST,
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_RUNNING,
    RUN_STATUS_STOPPED,
    RUN_STATUS_WAITING,
    STEP_STATUS_DONE,
    STREAM_TAG_EXEC,
)
from backend.src.agent.runner.feedback import (
    append_task_feedback_step,
    canonicalized_feedback_meta,
    is_task_feedback_step_title,
    realign_feedback_step_for_resume,
)
from backend.src.services.skills.skills_draft import create_skill
from backend.src.services.tasks.task_queries import get_last_non_planned_step_for_run
from backend.src.services.tasks.task_queries import get_max_step_order_for_run_by_status
from backend.src.services.tasks.task_queries import get_task
from backend.src.services.tasks.task_queries import get_task_run
from backend.src.services.llm.llm_client import resolve_default_model, sse_json
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

_RESUME_TOKEN_LOCK = threading.Lock()
_RESUME_TOKEN_STATE: dict[tuple[int, str], dict] = {}
_RESUME_TOKEN_TTL_SECONDS = 600


def _log_text_brief(value: object, limit: int = 120) -> str:
    text = str(value or "").strip().replace("\n", "\\n")
    if len(text) <= int(limit):
        return text
    return f"{text[: int(limit)]}..."


def _cleanup_resume_token_state(now: float) -> None:
    expired_keys = []
    for key, info in list(_RESUME_TOKEN_STATE.items()):
        ts = float(info.get("ts") or 0.0)
        if now - ts > float(_RESUME_TOKEN_TTL_SECONDS):
            expired_keys.append(key)
    for key in expired_keys:
        _RESUME_TOKEN_STATE.pop(key, None)


def _claim_resume_prompt_token(*, run_id: int, token: str) -> tuple[bool, str]:
    token_text = str(token or "").strip()
    if not token_text:
        return True, ""
    now = time.time()
    key = (int(run_id), token_text)
    with _RESUME_TOKEN_LOCK:
        _cleanup_resume_token_state(now)
        prev = _RESUME_TOKEN_STATE.get(key)
        if isinstance(prev, dict):
            state = str(prev.get("state") or "").strip() or "done"
            return False, state
        _RESUME_TOKEN_STATE[key] = {"state": "inflight", "ts": now}
    return True, "inflight"


def _finalize_resume_prompt_token(*, run_id: int, token: str, state: str) -> None:
    token_text = str(token or "").strip()
    if not token_text:
        return
    now = time.time()
    key = (int(run_id), token_text)
    with _RESUME_TOKEN_LOCK:
        _cleanup_resume_token_state(now)
        _RESUME_TOKEN_STATE[key] = {"state": str(state or "done"), "ts": now}


@require_write_permission_stream
def stream_agent_command_resume(payload: AgentCommandResumeStreamRequest):
    """
    继续执行一个进入 waiting/stopped 的 agent run。

    说明：
    - 前端收到 need_input 后，用 run_id + 用户回答调用该接口
    - 该接口会在同一个 run_id 上继续执行剩余计划，并继续以 SSE 回传进度
    """
    user_input = (payload.message or "").strip()
    if not user_input:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            ERROR_MESSAGE_LLM_CHAT_MESSAGE_MISSING,
            HTTP_STATUS_BAD_REQUEST,
        )

    run_id = int(payload.run_id)
    request_prompt_token = str(getattr(payload, "prompt_token", "") or "").strip()
    request_session_key = coerce_session_key(getattr(payload, "session_key", ""))
    run_row = get_task_run(run_id=run_id)
    if not run_row:
        logger.warning("[agent.resume] invalid_run run_id=%s reason=not_found", run_id)
        return error_response(ERROR_CODE_INVALID_REQUEST, "run 不存在", HTTP_STATUS_BAD_REQUEST)
    if run_row["status"] not in {RUN_STATUS_WAITING, RUN_STATUS_STOPPED, RUN_STATUS_FAILED}:
        logger.warning(
            "[agent.resume] invalid_status run_id=%s status=%s",
            run_id,
            str(run_row["status"] or ""),
        )
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            f"run 当前状态不支持继续执行: {run_row['status']}",
            HTTP_STATUS_BAD_REQUEST,
        )
    logger.info(
        "[agent.resume] request run_id=%s status=%s has_prompt_token=%s has_session_key=%s",
        run_id,
        str(run_row["status"] or ""),
        bool(request_prompt_token),
        bool(request_session_key),
    )

    if str(run_row["status"] or "").strip() == RUN_STATUS_WAITING:
        paused = _extract_json_object(run_row["agent_state"] or "")
        paused_obj = paused.get("paused") if isinstance(paused, dict) and isinstance(paused.get("paused"), dict) else {}
        required_session_key = coerce_session_key((paused or {}).get("session_key"))
        required_token = str(paused_obj.get("prompt_token") or "").strip()
        contract_error = validate_waiting_resume_contract(
            required_session_key=str(required_session_key or ""),
            request_session_key=str(request_session_key or ""),
            required_prompt_token=str(required_token or ""),
            request_prompt_token=str(request_prompt_token or ""),
        )
        if contract_error:
            logger.warning(
                "[agent.resume] contract_rejected run_id=%s reason=%s",
                run_id,
                _log_text_brief(contract_error),
            )
            return error_response(
                ERROR_CODE_INVALID_REQUEST,
                str(contract_error),
                HTTP_STATUS_BAD_REQUEST,
            )
        if required_token:
            claimed, state = _claim_resume_prompt_token(run_id=int(run_id), token=request_prompt_token)
            if not claimed:
                logger.warning(
                    "[agent.resume] duplicate_submit run_id=%s prompt_token=%s state=%s",
                    run_id,
                    _log_text_brief(request_prompt_token, limit=48),
                    state,
                )
                return error_response(
                    ERROR_CODE_INVALID_REQUEST,
                    f"该输入已提交（state={state}），请等待当前执行完成",
                    HTTP_STATUS_BAD_REQUEST,
                )

    async def gen() -> AsyncGenerator[str, None]:
        task_id: Optional[int] = None
        session_key: str = ""
        run_status: str = ""
        last_step_order: int = 0
        stream_state = StreamRunStateEmitter()
        token_finalized = False
        queue_ticket: Optional[StreamQueueTicket] = None

        def _finalize_token_once(state: str) -> None:
            nonlocal token_finalized
            if token_finalized:
                return
            if not request_prompt_token:
                return
            _finalize_resume_prompt_token(
                run_id=int(run_id),
                token=request_prompt_token,
                state=str(state or "done"),
            )
            token_finalized = True

        def _emit(chunk: str) -> str:
            return stream_state.emit(str(chunk or ""))

        def _emit_run_status(status: object) -> Optional[str]:
            if task_id is None:
                return None
            stream_state.bind_run(task_id=int(task_id), run_id=int(run_id), session_key=session_key)
            return stream_state.emit_run_status(status)

        async def _cleanup_resume_resources_once(state: Optional[str] = None) -> None:
            nonlocal queue_ticket
            _finalize_token_once(str(state or run_status or "done"))
            if queue_ticket is None:
                return
            try:
                await queue_ticket.release()
            except Exception:
                pass
            queue_ticket = None

        try:
            run = await asyncio.to_thread(get_task_run, run_id=run_id)
            if not run:
                yield _emit(
                    sse_json(
                        build_stream_error_payload(
                            error_code="resume_run_not_found",
                            error_message="run 不存在",
                            phase="resume_preflight",
                            task_id=None,
                            run_id=run_id,
                            recoverable=False,
                            retryable=False,
                            terminal_source="runtime",
                        ),
                        event="error",
                    )
                )
                await _cleanup_resume_resources_once(RUN_STATUS_FAILED)
                return
            task_id = int(run["task_id"])
            logger.info(
                "[agent.resume] start task_id=%s run_id=%s from_status=%s",
                task_id,
                run_id,
                str(run["status"] or ""),
            )
            stream_state.bind_run(task_id=int(task_id), run_id=int(run_id), session_key=session_key)
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
                if isinstance(last_active_step, dict):
                    raw_last_active_order = last_active_step.get("step_order")
                else:
                    try:
                        raw_last_active_order = last_active_step["step_order"]
                    except (TypeError, KeyError, IndexError):
                        raw_last_active_order = None
                last_active_step_order = coerce_int(raw_last_active_order, default=0)
                try:
                    last_active_step_status = str(last_active_step["status"] or "").strip()
                except (TypeError, KeyError, IndexError):
                    last_active_step_status = ""

            plan_obj = _extract_json_object(run["agent_plan"] or "") or {}
            plan_struct = PlanStructure.from_agent_plan_payload(plan_obj)
            plan_titles, plan_items, plan_allows, plan_artifacts = plan_struct.to_legacy_lists()

            if not plan_titles:
                logger.warning(
                    "[agent.resume] missing_plan task_id=%s run_id=%s",
                    task_id,
                    run_id,
                )
                yield _emit(
                    sse_json(
                        build_stream_error_payload(
                            error_code="resume_missing_agent_plan",
                            error_message="agent_plan 缺失，无法 resume",
                            phase="resume_preflight",
                            task_id=task_id,
                            run_id=run_id,
                            recoverable=False,
                            retryable=False,
                            terminal_source="runtime",
                        ),
                        event="error",
                    )
                )
                await _cleanup_resume_resources_once(RUN_STATUS_FAILED)
                return

            state_obj_raw = _extract_json_object(run["agent_state"] or "") or {}
            message = str(state_obj_raw.get("message") or "").strip() or (str(task_row["title"]) if task_row else "")
            workdir = str(state_obj_raw.get("workdir") or "").strip() or os.getcwd()
            tools_hint = str(state_obj_raw.get("tools_hint") or "").strip() or _list_tool_hints()
            skills_hint = str(state_obj_raw.get("skills_hint") or "").strip() or "(无)"
            solutions_hint = str(state_obj_raw.get("solutions_hint") or "").strip() or "(无)"
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
            run_max_steps: Optional[int] = parse_optional_int(raw_max_steps, default=None)

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
            session_key = resolve_or_create_session_key(
                agent_state=state_obj,
                task_id=int(task_id),
                run_id=int(run_id),
                created_at=str(run["created_at"] or ""),
            )
            state_obj = apply_session_key_to_state(state_obj, session_key)
            state_obj = apply_run_config_snapshot_if_missing(
                agent_state=state_obj,
                mode=str(state_obj.get("mode") or "").strip() or None,
                requested_model=model,
                parameters=parameters if isinstance(parameters, dict) else {},
            )
            run_ctx.merge_state_overrides({"session_key": session_key})
            run_ctx.merge_state_overrides({"config_snapshot": state_obj.get("config_snapshot")})
            queue_ticket = await acquire_stream_queue_ticket(session_key=session_key or f"run:{int(run_id)}")
            context = run_ctx.context
            observations = [str(o).strip() for o in run_ctx.observations if str(o).strip()]
            run_ctx.observations.clear()
            run_ctx.observations.extend(list(observations))

            paused = state_obj.get("paused") if isinstance(state_obj.get("paused"), dict) else {}
            paused_step_order_raw = paused.get("step_order")
            paused_step_order: Optional[int] = parse_optional_int(paused_step_order_raw, default=None)

            feedback_canonicalized = realign_feedback_step_for_resume(
                run_status=str(run["status"] or ""),
                plan_titles=plan_titles,
                plan_items=plan_items,
                plan_allows=plan_allows,
                paused_step_order=paused_step_order,
                paused_step_title=str(paused.get("step_title") or ""),
                task_feedback_asked=bool(state_obj.get("task_feedback_asked")),
                max_steps=run_max_steps,
            )
            feedback_meta = canonicalized_feedback_meta(feedback_canonicalized)
            state_obj["task_feedback_asked"] = feedback_meta["task_feedback_asked"]
            if feedback_meta["changed"] or feedback_meta["reask_feedback"]:
                _safe_write_debug(
                    task_id,
                    run_id,
                    message="agent.resume.feedback_step_canonicalized",
                    data={
                        "found": feedback_meta["found"],
                        "removed": feedback_meta["removed"],
                        "appended": feedback_meta["appended"],
                        "reask_feedback": feedback_meta["reask_feedback"],
                        "feedback_asked_after": feedback_meta["task_feedback_asked"],
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

            plan_total_steps = len(plan_titles)
            # 特殊：pending_planning 表示“等待用户补充后再重新规划”，即便当前 plan 只有 user_prompt 也不应判定为已完成。
            pending_planning = bool(state_obj.get("pending_planning")) if isinstance(state_obj, dict) else False
            resume_decision = infer_resume_step_decision(
                paused_step_order=paused_step_order,
                state_step_order=state_obj.get("step_order"),
                last_done_step=coerce_int(last_done_step, default=0),
                last_active_step_order=coerce_int(last_active_step_order, default=0),
                last_active_step_status=str(last_active_step_status or ""),
                plan_total_steps=int(plan_total_steps),
                pending_planning=bool(pending_planning),
            )
            resume_step_order = int(resume_decision.resume_step_order)
            skip_execution = bool(resume_decision.skip_execution)

            # 继续执行前先把计划栏状态对齐到“已完成/待执行”，避免 UI 显示漂移
            normalize_plan_items_for_resume(
                plan_items=plan_items,
                last_done_step=coerce_int(last_done_step, default=0),
            )
            plan_struct = PlanStructure.from_legacy(
                plan_titles=list(plan_titles or []),
                plan_items=list(plan_items or []),
                plan_allows=[list(value or []) for value in (plan_allows or [])],
                plan_artifacts=list(plan_artifacts or []),
            )
            plan_titles, plan_items, plan_allows, plan_artifacts = plan_struct.to_legacy_lists()

            run_status = RUN_STATUS_FAILED
            last_step_order = 0
            if skip_execution:
                last_step_order = int(plan_total_steps)
                logger.info(
                    "[agent.resume] skip_execution task_id=%s run_id=%s plan_total=%s",
                    task_id,
                    run_id,
                    plan_total_steps,
                )
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
                    yield _emit(str(event))
                status_event = _emit_run_status(run_status)
                if status_event:
                    yield status_event
                normalized_status = str(run_status or "").strip()
                missing_visible_result = stream_state.build_missing_visible_result_if_needed(normalized_status)
                if missing_visible_result:
                    yield missing_visible_result
                terminal_meta = resolve_terminal_meta(
                    normalized_status,
                    status_source="runtime",
                )
                yield _emit(
                    done_sse_event(
                        run_status=str(terminal_meta.run_status or ""),
                        completion_reason=str(terminal_meta.completion_reason or ""),
                        terminal_source=str(terminal_meta.terminal_source or ""),
                    )
                )
                await _cleanup_resume_resources_once(normalized_status or run_status)
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

            yield _emit(sse_json({"delta": f"{STREAM_TAG_EXEC} 已收到输入，继续执行…\n"}))
            status_event = _emit_run_status(RUN_STATUS_RUNNING)
            if status_event:
                yield status_event

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
                        yield _emit(str(event_payload))
                        continue
                    pending_result = event_payload
                if pending_result is None:
                    raise RuntimeError("pending_planning resume 结果为空")
                outcome = str(pending_result.get("outcome") or "").strip()
                if outcome in {"waiting", "failed"}:
                    logger.info(
                        "[agent.resume] pending_planning_outcome task_id=%s run_id=%s outcome=%s",
                        task_id,
                        run_id,
                        outcome,
                    )
                    if outcome == "waiting":
                        status_event = _emit_run_status(RUN_STATUS_WAITING)
                        if status_event:
                            yield status_event
                    if outcome == "failed":
                        status_event = _emit_run_status(RUN_STATUS_FAILED)
                        if status_event:
                            yield status_event
                        yield _emit(
                            sse_json(
                                build_stream_error_payload(
                                    error_code="pending_planning_resume_failed",
                                    error_message=format_task_error(
                                        code="pending_planning_resume_failed",
                                        message="pending planning 恢复失败，执行已收敛为 failed",
                                    ),
                                    phase="resume_pending_planning",
                                    task_id=task_id,
                                    run_id=run_id,
                                    recoverable=False,
                                    retryable=False,
                                    terminal_source="runtime",
                                ),
                                event="error",
                            )
                        )
                    await _cleanup_resume_resources_once(outcome or run_status)
                    return

                # outcome == planned：用新 plan/状态继续后续执行
                logger.info(
                    "[agent.resume] pending_planning_outcome task_id=%s run_id=%s outcome=planned",
                    task_id,
                    run_id,
                )
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
                resume_step_order = coerce_int(
                    pending_result.get("resume_step_order"),
                    default=coerce_int(resume_step_order, default=1),
                )

            mode = str(state_obj.get("mode") or "").strip().lower()
            run_status = RUN_STATUS_FAILED
            last_step_order = 0
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
                    yield _emit(str(event_payload))
                    continue
                mode_result = event_payload

            if not isinstance(mode_result, dict):
                raise RuntimeError("resume mode execution 结果为空")

            run_status = str(mode_result.get("run_status") or "")
            logger.info(
                "[agent.resume] execution_done task_id=%s run_id=%s status=%s",
                task_id,
                run_id,
                run_status,
            )
            last_step_order = coerce_int(mode_result.get("last_step_order"), default=0)
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
                    yield _emit(str(event_payload))
                    continue
                if event_type == "status":
                    run_status = str(event_payload or run_status)
                    status_event = _emit_run_status(run_status)
                    if status_event:
                        yield status_event

        except (asyncio.CancelledError, GeneratorExit):
            # SSE 连接被关闭（客户端断开/窗口退出）时不要再尝试继续 yield，否则会触发
            # “async generator ignored GeneratorExit/CancelledError” 类错误并留下 running 状态。
            logger.info("[agent.resume] cancelled task_id=%s run_id=%s", task_id, run_id)
            enqueue_stop_task_run_records(task_id=task_id, run_id=int(run_id), reason="agent_stream_cancelled")
            await _cleanup_resume_resources_once("cancelled")
            raise
        except Exception as exc:
            logger.exception("[agent.resume] exception task_id=%s run_id=%s error=%s", task_id, run_id, exc)
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
                try:
                    await ensure_failed_task_output(
                        int(task_id),
                        int(run_id),
                        RUN_STATUS_FAILED,
                        lambda _msg: None,
                    )
                except Exception as failed_output_exc:
                    _safe_write_debug(
                        task_id,
                        run_id,
                        message="agent.exception.failed_output_injection_failed",
                        data={"error": str(failed_output_exc)},
                        level="warning",
                    )
                try:
                    enqueue_postprocess_thread(
                        task_id=int(task_id),
                        run_id=int(run_id),
                        run_status=RUN_STATUS_FAILED,
                    )
                except Exception:
                    pass
                _safe_write_debug(
                    task_id,
                    run_id,
                    message="agent.exception",
                    data={"error": f"{exc}"},
                    level="error",
                )
            error_code = "agent_resume_exception"
            user_message = format_task_error(
                code=error_code,
                message=f"agent resume 执行失败:{exc}",
            )
            try:
                yield _emit(
                    sse_json(
                        build_stream_error_payload(
                            error_code=error_code,
                            error_message=user_message,
                            phase="resume_exception",
                            task_id=task_id,
                            run_id=run_id,
                            recoverable=False,
                            retryable=False,
                            terminal_source="runtime",
                        ),
                        event="error",
                    )
                )
            except BaseException:
                await _cleanup_resume_resources_once(RUN_STATUS_FAILED)
                return
            status_event = _emit_run_status(RUN_STATUS_FAILED)
            if status_event:
                yield status_event
            _finalize_token_once("failed")

        # 正常结束/异常结束均尽量发送 done；若客户端已断开则直接结束 generator。
        normalized_status = ""
        status_source = "runtime"
        try:
            normalized_status = normalize_stream_run_status(run_status)
            if not normalized_status:
                status_source = "fallback"
                db_run = await asyncio.to_thread(get_task_run, run_id=int(run_id))
                db_status = ""
                if db_run is not None:
                    try:
                        db_status = normalize_stream_run_status(db_run["status"])
                    except Exception:
                        db_status = ""
                if db_status:
                    normalized_status = db_status
                    status_source = "db"
                else:
                    normalized_status = RUN_STATUS_FAILED
                anomaly_message = format_task_error(
                    code="stream_missing_terminal_status",
                    message=(
                        f"resume stream 结束时缺少 run_status，已自动收敛为 {normalized_status}"
                    ),
                )
                yield _emit(
                    sse_json(
                        build_stream_error_payload(
                            error_code="stream_missing_terminal_status",
                            error_message=anomaly_message,
                            phase="resume_finalize",
                            task_id=task_id,
                            run_id=run_id,
                            recoverable=False,
                            retryable=False,
                            terminal_source=status_source,
                            details={
                                "resolved_status": normalized_status,
                                "status_source": status_source,
                            },
                        ),
                        event="error",
                    )
                )
            terminal_meta = resolve_terminal_meta(
                normalized_status,
                status_source=status_source,
            )
            missing_visible_result = stream_state.build_missing_visible_result_if_needed(
                terminal_meta.run_status
            )
            if missing_visible_result:
                yield missing_visible_result
            status_event = _emit_run_status(terminal_meta.run_status)
            if status_event:
                yield status_event
            yield _emit(
                done_sse_event(
                    run_status=str(terminal_meta.run_status or ""),
                    completion_reason=str(terminal_meta.completion_reason or ""),
                    terminal_source=str(terminal_meta.terminal_source or ""),
                )
            )
            logger.info(
                "[agent.resume] finalized task_id=%s run_id=%s status=%s last_step=%s",
                task_id,
                run_id,
                terminal_meta.run_status,
                last_step_order,
            )
            _finalize_token_once(terminal_meta.run_status or "done")
        except BaseException:
            return
        finally:
            await _cleanup_resume_resources_once(normalized_status or run_status or "done")

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(gen(), media_type="text/event-stream", headers=headers)
