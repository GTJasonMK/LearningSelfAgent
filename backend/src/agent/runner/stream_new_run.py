import asyncio
import logging
import os
import time
from typing import AsyncGenerator, Dict, List, Optional

from backend.src.agent.planning_phase import PlanPhaseFailure, run_planning_phase
from backend.src.agent.runner.feedback import append_task_feedback_step
from backend.src.agent.runner.react_loop import run_react_loop
from backend.src.agent.runner.stream_pump import pump_sync_generator
from backend.src.agent.runner.execution_pipeline import (
    create_sse_response,
    pump_async_task_messages,
    enter_pending_planning_waiting,
    prepare_planning_knowledge_do,
    run_finalization_sequence,
    handle_execution_exception,
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
from backend.src.common.utils import error_response, now_iso
from backend.src.constants import (
    AGENT_DEFAULT_MAX_STEPS,
    AGENT_EXPERIMENT_DIR_REL,
    AGENT_PLAN_RESERVED_STEPS,
    AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
    AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
    ERROR_CODE_INVALID_REQUEST,
    ERROR_MESSAGE_LLM_CHAT_MESSAGE_MISSING,
    HTTP_STATUS_BAD_REQUEST,
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
from backend.src.services.debug.debug_output import write_task_debug_output
from backend.src.services.llm.llm_client import resolve_default_model, sse_json
from backend.src.services.permissions.permission_checks import ensure_write_permission
from backend.src.repositories.task_runs_repo import update_task_run
from backend.src.repositories.skills_repo import create_skill
from backend.src.services.skills.skills_publish import publish_skill_file
from backend.src.services.tasks.task_run_lifecycle import (
    check_missing_artifacts,
    create_task_and_run_records_for_agent,
    enqueue_postprocess_thread,
    enqueue_review_on_feedback_waiting,
    enqueue_stop_task_run_records,
    finalize_run_and_task_status,
    mark_run_failed,
)

logger = logging.getLogger(__name__)


def _safe_write_debug(
    task_id: Optional[int],
    run_id: Optional[int],
    *,
    message: str,
    data: Optional[dict] = None,
    level: str = "debug",
) -> None:
    """
    调试输出不应影响主链路：失败时降级为 logger.exception。
    """
    if task_id is None or run_id is None:
        return
    try:
        write_task_debug_output(
            task_id=int(task_id),
            run_id=int(run_id),
            message=message,
            data=data if isinstance(data, dict) else None,
            level=level,
        )
    except Exception:
        logger.exception("write_task_debug_output failed: %s", message)


def stream_agent_command(payload: AgentCommandStreamRequest):
    """
    自然语言指令执行（SSE 流式）：
    - 创建 task/run
    - 检索（graph/memory/skills）
    - 规划 plan（含 allow + artifacts）
    - ReAct 执行（逐步 action -> 执行 -> 观测 -> 下一步）
    """
    permission = ensure_write_permission()
    if permission:
        return permission

    message = (payload.message or "").strip()
    if not message:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            ERROR_MESSAGE_LLM_CHAT_MESSAGE_MISSING,
            HTTP_STATUS_BAD_REQUEST,
        )

    max_steps = payload.max_steps or AGENT_DEFAULT_MAX_STEPS
    dry_run = bool(payload.dry_run)
    model = (payload.model or "").strip() or resolve_default_model()
    parameters = payload.parameters or {"temperature": 0.2}

    async def gen() -> AsyncGenerator[str, None]:
        task_id: Optional[int] = None
        run_id: Optional[int] = None
        plan_items: List[dict] = []

        try:
            created_at = now_iso()
            workdir = os.getcwd()

            # 先创建 task/run：即使后续规划失败，也能在“最近动态/时间线”里留下可追溯记录，便于排查复杂 bug。
            task_id, run_id = await asyncio.to_thread(
                create_task_and_run_records_for_agent,
                message=message,
                created_at=created_at,
            )

            # 让前端尽早拿到 run_id：用于 evaluate/continue/debug（不走 delta，避免污染气泡文本）
            yield sse_json({"type": "run_created", "task_id": task_id, "run_id": run_id})

            _safe_write_debug(
                task_id,
                run_id,
                message="agent.start",
                data={
                    "model": model,
                    "max_steps": max_steps,
                    "dry_run": dry_run,
                    "workdir": workdir,
                    "agent_workspace": AGENT_EXPERIMENT_DIR_REL,
                },
            )
            # 工具清单会在“方案匹配”之后汇总（方案提到的工具优先）
            tools_hint = "(无)"
            solutions_hint = "(无)"

            # --- 检索：图谱→领域→技能→方案（收敛到 execution_pipeline）---
            out_q: "asyncio.Queue[str]" = asyncio.Queue()

            def _emit(msg: str) -> None:
                try:
                    out_q.put_nowait(str(msg))
                except Exception:
                    return

            knowledge_task = asyncio.create_task(
                retrieve_all_knowledge(
                    message=message,
                    model=model,
                    parameters=parameters,
                    yield_func=_emit,
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
            )
            async for msg in pump_async_task_messages(knowledge_task, out_q):
                yield msg
            knowledge = await knowledge_task

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
            enrich_q: "asyncio.Queue[str]" = asyncio.Queue()

            def _emit_enrich(msg: str) -> None:
                try:
                    enrich_q.put_nowait(str(msg))
                except Exception:
                    return

            enrich_task = asyncio.create_task(
                prepare_planning_knowledge_do(
                    message=message,
                    model=model,
                    parameters=parameters,
                    graph_nodes=graph_nodes,
                    graph_hint=graph_hint,
                    domain_ids=domain_ids,
                    skills=skills,
                    skills_hint=skills_hint,
                    solutions=solutions,
                    yield_func=_emit_enrich,
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
                )
            )
            async for msg in pump_async_task_messages(enrich_task, enrich_q):
                yield msg
            enriched = await enrich_task

            skills = list(enriched.get("skills") or skills or [])
            skills_hint = str(enriched.get("skills_hint") or skills_hint or "(无)")
            solutions_for_prompt = list(enriched.get("solutions_for_prompt") or solutions or [])
            draft_solution_id = enriched.get("draft_solution_id")
            solutions_hint = str(enriched.get("solutions_hint") or "(无)")
            tools_hint = str(enriched.get("tools_hint") or "(无)")
            need_user_prompt = bool(enriched.get("need_user_prompt"))
            user_prompt_question = str(enriched.get("user_prompt_question") or "").strip()

            # --- 知识不足且需询问用户：进入 waiting，并在 resume 后重新检索+规划 ---
            if need_user_prompt and user_prompt_question and task_id is not None and run_id is not None:
                out_q: "asyncio.Queue[str]" = asyncio.Queue()

                def _emit_wait(msg: str) -> None:
                    try:
                        if msg:
                            out_q.put_nowait(str(msg))
                    except Exception:
                        return

                wait_task = asyncio.create_task(
                    enter_pending_planning_waiting(
                        task_id=int(task_id),
                        run_id=int(run_id),
                        mode="do",
                        message=message,
                        workdir=workdir,
                        model=model,
                        parameters=parameters,
                        max_steps=int(max_steps),
                        user_prompt_question=user_prompt_question,
                        tools_hint=tools_hint,
                        skills_hint=skills_hint,
                        solutions_hint=solutions_hint,
                        memories_hint=memories_hint,
                        graph_hint=graph_hint,
                        domain_ids=list(domain_ids or []),
                        skills=list(skills or []),
                        solutions=list(solutions_for_prompt or []),
                        draft_solution_id=int(draft_solution_id)
                        if isinstance(draft_solution_id, int) and int(draft_solution_id) > 0
                        else None,
                        yield_func=_emit_wait,
                        safe_write_debug_func=_safe_write_debug,
                    )
                )
                async for msg in pump_async_task_messages(wait_task, out_q):
                    yield msg
                await wait_task
                return

            # --- 规划 ---
            try:
                # 预留步数：确认满意度 + 评估不通过后的自修复/重试（避免 plan 直接占满 max_steps 导致无法继续推进）。
                reserved = int(AGENT_PLAN_RESERVED_STEPS or 0)
                if reserved < 1:
                    reserved = 1
                planning_max_steps = int(max_steps) - reserved if int(max_steps) > 1 else 1
                if planning_max_steps < 1:
                    planning_max_steps = 1
                inner = run_planning_phase(
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
                )
                plan_started_at = time.monotonic()
                plan_result = None
                async for kind, payload in pump_sync_generator(
                    inner=inner,
                    label="planning",
                    poll_interval_seconds=AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
                    idle_timeout_seconds=AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
                ):
                    if kind == "msg":
                        if payload:
                            yield str(payload)
                        continue
                    if kind == "done":
                        plan_result = payload
                        break
                    if kind == "err":
                        if isinstance(payload, BaseException):
                            raise payload  # noqa: TRY301
                        raise RuntimeError(f"planning 异常:{payload}")  # noqa: TRY301

                if plan_result is None:
                    raise RuntimeError("planning 返回为空")  # noqa: TRY301

                duration_ms = int((time.monotonic() - plan_started_at) * 1000)
                _safe_write_debug(
                    task_id,
                    run_id,
                    message="agent.plan.done",
                    data={"duration_ms": duration_ms, "steps": len(plan_result.plan_titles or [])},
                    level="info",
                )
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
                yield sse_json({"message": exc.public_message}, event="error")
                return

            plan_titles = plan_result.plan_titles
            plan_allows = plan_result.plan_allows
            plan_artifacts = plan_result.plan_artifacts
            plan_items = plan_result.plan_items

            # 任务闭环：在计划末尾追加“确认满意度”步骤（由后端控制，避免前端硬编码逻辑漂移）。
            append_task_feedback_step(
                plan_titles=plan_titles,
                plan_items=plan_items,
                plan_allows=plan_allows,
                max_steps=int(max_steps) if isinstance(max_steps, int) else None,
            )

            yield sse_json({"type": "plan", "task_id": task_id, "items": plan_items})

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
            agent_state["mode"] = "do"
            agent_state["solutions_hint"] = solutions_hint
            agent_state["domain_ids"] = list(domain_ids or [])
            agent_state["skill_ids"] = [
                s.get("id")
                for s in (skills or [])
                if isinstance(s, dict) and isinstance(s.get("id"), int) and int(s.get("id")) > 0
            ]
            agent_state["solution_ids"] = [
                s.get("id")
                for s in (solutions_for_prompt or [])
                if isinstance(s, dict) and isinstance(s.get("id"), int) and int(s.get("id")) > 0
            ]
            if isinstance(draft_solution_id, int) and int(draft_solution_id) > 0:
                agent_state["draft_solution_id"] = int(draft_solution_id)
            try:
                await persist_agent_state(
                    run_id=int(run_id),
                    agent_plan={
                        "titles": plan_titles,
                        "items": plan_items,
                        "allows": plan_allows,
                        "artifacts": plan_artifacts,
                    },
                    agent_state=agent_state,
                )
            except Exception as exc:
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
                yield sse_json({"delta": f"{STREAM_TAG_EXEC} dry_run: 已生成步骤，未执行。\n"})
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
                except Exception as exc:
                    logger.exception("agent.dry_run.finalize_failed: %s", exc)
                    _safe_write_debug(
                        task_id,
                        run_id,
                        message="agent.dry_run.finalize_failed",
                        data={"error": str(exc)},
                        level="error",
                    )
                return

            yield sse_json({"delta": f"{STREAM_TAG_EXEC} 开始执行…\n"})

            # 执行上下文：用于 task_output 自动填充等
            context: dict = {"last_llm_response": None}
            observations: List[str] = []
            agent_state["context"] = context
            agent_state["observations"] = observations

            inner_react = run_react_loop(
                task_id=int(task_id),
                run_id=int(run_id),
                message=message,
                workdir=workdir,
                model=model,
                parameters=parameters,
                plan_titles=plan_titles,
                plan_items=plan_items,
                plan_allows=plan_allows,
                plan_artifacts=plan_artifacts,
                tools_hint=tools_hint,
                skills_hint=skills_hint,
                memories_hint=memories_hint,
                graph_hint=graph_hint,
                agent_state=agent_state,
                context=context,
                observations=observations,
                start_step_order=1,
                variables_source="agent_react",
            )
            react_started_at = time.monotonic()
            react_result = None
            async for kind, payload in pump_sync_generator(
                inner=inner_react,
                label="react",
                poll_interval_seconds=AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
                idle_timeout_seconds=AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
            ):
                if kind == "msg":
                    if payload:
                        yield str(payload)
                    continue
                if kind == "done":
                    react_result = payload
                    break
                if kind == "err":
                    if isinstance(payload, BaseException):
                        raise payload  # noqa: TRY301
                    raise RuntimeError(f"react 异常:{payload}")  # noqa: TRY301

            if react_result is None:
                raise RuntimeError("react 返回为空")  # noqa: TRY301

            run_status = react_result.run_status
            duration_ms = int((time.monotonic() - react_started_at) * 1000)
            _safe_write_debug(
                task_id,
                run_id,
                message="agent.react.done",
                data={
                    "duration_ms": duration_ms,
                    "run_status": str(run_status),
                    "last_step_order": int(getattr(react_result, "last_step_order", 0) or 0),
                },
                level="info",
            )

            # 统一后处理闭环（收敛到 execution_pipeline）
            out_q = asyncio.Queue()
            final_task = asyncio.create_task(
                run_finalization_sequence(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    run_status=str(run_status),
                    agent_state=agent_state,
                    plan_items=plan_items,
                    plan_artifacts=plan_artifacts,
                    message=message,
                    workdir=workdir,
                    yield_func=_emit,
                )
            )
            async for msg in pump_async_task_messages(final_task, out_q):
                yield msg
            run_status = await final_task

        except (asyncio.CancelledError, GeneratorExit):
            # SSE 连接被关闭（客户端断开/窗口退出）时不要再尝试继续 yield，否则会触发
            # “async generator ignored GeneratorExit/CancelledError” 类错误并留下 running 状态。
            handle_stream_cancellation(task_id=task_id, run_id=run_id, reason="agent_stream_cancelled")
            raise
        except Exception as exc:
            out_q = asyncio.Queue()
            err_task = asyncio.create_task(
                handle_execution_exception(
                    exc,
                    task_id=task_id,
                    run_id=run_id,
                    yield_func=_emit,
                    mode_prefix="agent",
                )
            )
            async for msg in pump_async_task_messages(err_task, out_q):
                yield msg
            await err_task

        # 正常结束/异常结束均尽量发送 done；若客户端已断开则直接结束 generator。
        try:
            yield sse_json({"type": "done"}, event="done")
        except BaseException:
            return

    return create_sse_response(gen)
