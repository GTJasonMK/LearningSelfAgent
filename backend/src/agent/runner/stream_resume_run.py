import asyncio
import json
import logging
import os
import time
from typing import AsyncGenerator, Dict, List, Optional, Tuple

from fastapi.responses import StreamingResponse

from backend.src.actions.registry import normalize_action_type
from backend.src.agent.planning_phase import PlanPhaseFailure, run_planning_phase
from backend.src.agent.runner import react_loop as react_loop_facade
from backend.src.agent.runner.react_loop import run_react_loop
from backend.src.agent.runner.execution_pipeline import (
    pump_async_task_messages,
    prepare_planning_knowledge_do,
    prepare_planning_knowledge_think,
    resume_pending_planning_after_user_input,
    retrieve_all_knowledge,
)
from backend.src.agent.runner.stream_pump import pump_sync_generator
from backend.src.agent.runner.think_parallel_loop import run_think_parallel_loop
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
    create_think_config_from_dict,
    get_default_think_config,
    infer_executor_assignments,
    merge_fix_steps_into_plan,
    run_think_planning_sync,
    run_reflection,
)
from backend.src.agent.think.think_execution import _infer_executor_from_allow, build_executor_assignments_payload
from backend.src.api.schemas import AgentCommandResumeStreamRequest
from backend.src.common.utils import error_response, now_iso
from backend.src.constants import (
    ACTION_TYPE_USER_PROMPT,
    AGENT_DEFAULT_MAX_STEPS,
    AGENT_PLAN_RESERVED_STEPS,
    ERROR_CODE_INVALID_REQUEST,
    ERROR_MESSAGE_LLM_CHAT_MESSAGE_MISSING,
    HTTP_STATUS_BAD_REQUEST,
    AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
    AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_RUNNING,
    RUN_STATUS_STOPPED,
    RUN_STATUS_WAITING,
    STATUS_RUNNING,
    STEP_STATUS_DONE,
    STEP_STATUS_FAILED,
    STEP_STATUS_RUNNING,
    STEP_STATUS_SKIPPED,
    STEP_STATUS_WAITING,
    STREAM_TAG_EXEC,
    STREAM_TAG_FAIL,
    STREAM_TAG_REFLECTION,
    SSE_TYPE_MEMORY_ITEM,
    TASK_OUTPUT_TYPE_USER_ANSWER,
    THINK_REFLECTION_MAX_ROUNDS,
)
from backend.src.agent.runner.feedback import append_task_feedback_step, is_task_feedback_step_title
from backend.src.repositories.task_outputs_repo import create_task_output
from backend.src.repositories.skills_repo import create_skill
from backend.src.repositories.task_runs_repo import get_task_run, update_task_run
from backend.src.repositories.task_steps_repo import get_max_step_order_for_run_by_status
from backend.src.repositories.task_steps_repo import get_last_non_planned_step_for_run
from backend.src.repositories.task_steps_repo import mark_task_step_done
from backend.src.repositories.task_steps_repo import list_task_steps_for_run, mark_task_step_skipped
from backend.src.repositories.tasks_repo import get_task, update_task
from backend.src.services.debug.debug_output import write_task_debug_output
from backend.src.services.llm.llm_client import call_openai, resolve_default_model, sse_json
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


def _safe_write_debug(
    task_id: Optional[int],
    run_id: int,
    *,
    message: str,
    data: Optional[dict] = None,
    level: str = "debug",
) -> None:
    """
    调试输出不应影响主链路：失败时降级为 logger.exception。
    """
    if task_id is None:
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


def _build_plan_briefs_from_items(*, plan_titles: List[str], plan_items: List[dict]) -> List[str]:
    """
    为 Think 反思/合并步骤构造 plan_briefs：
    - 优先使用 plan_items[i].brief
    - 兜底用 title 前 10 字
    """
    briefs: List[str] = []
    for i, title in enumerate(plan_titles or []):
        brief = ""
        if 0 <= i < len(plan_items) and isinstance(plan_items[i], dict):
            brief = str(plan_items[i].get("brief") or "").strip()
        if not brief:
            brief = str(title or "").strip()[:10]
        briefs.append(brief)
    return briefs


def _create_llm_call_func(base_model: str, base_parameters: dict):
    """
    Think 模式恢复执行用的 LLM 调用函数（给反思机制使用）。

    返回签名：(prompt, model, params) -> (text, record_id)
    """

    def llm_call(prompt: str, call_model: str, call_params: dict) -> Tuple[str, Optional[int]]:
        merged_params = {**(base_parameters or {}), **(call_params or {})}
        text, record_id, err = call_openai(prompt, call_model or base_model, merged_params)
        if err:
            return "", None
        return text or "", record_id

    return llm_call


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
                except Exception:
                    last_active_step_order = 0
                last_active_step_status = str(last_active_step["status"] or "").strip()

            plan_obj = _extract_json_object(run["agent_plan"] or "") or {}
            titles_raw = plan_obj.get("titles")
            plan_titles: List[str] = (
                [str(t).strip() for t in titles_raw if str(t).strip()] if isinstance(titles_raw, list) else []
            )

            items_raw = plan_obj.get("items")
            plan_items: List[dict] = [it for it in items_raw if isinstance(it, dict)] if isinstance(items_raw, list) else []

            allows_raw = plan_obj.get("allows")
            plan_allows: List[List[str]] = []
            if isinstance(allows_raw, list):
                for raw in allows_raw:
                    if isinstance(raw, list):
                        values = [str(v).strip() for v in raw if str(v).strip()]
                    else:
                        values = [str(raw).strip()] if str(raw).strip() else []
                    plan_allows.append(values)
            if not plan_allows or len(plan_allows) != len(plan_titles):
                # 兼容旧 run：没有 allow 则不限制
                plan_allows = [[] for _ in plan_titles]
            else:
                # 兼容旧 run：allow 里可能存在 alias（tool/cmd/...）或大小写差异，统一归一化到 action registry 的标准类型。
                normalized_allows: List[List[str]] = []
                for raw_values in plan_allows:
                    out: List[str] = []
                    for value in raw_values or []:
                        normalized = normalize_action_type(str(value))
                        if normalized and normalized not in out:
                            out.append(normalized)
                    normalized_allows.append(out)
                plan_allows = normalized_allows

            artifacts_raw = plan_obj.get("artifacts")
            plan_artifacts: List[str] = (
                [str(v).strip() for v in artifacts_raw if str(v).strip()] if isinstance(artifacts_raw, list) else []
            )

            if not plan_titles:
                yield sse_json({"message": "agent_plan 缺失，无法 resume"}, event="error")
                return

            if not plan_items or len(plan_items) != len(plan_titles):
                # 兼容旧 run：没有 items 时按 title 推断简短 brief
                plan_items = []
                for i, title in enumerate(plan_titles, start=1):
                    plan_items.append({"id": i, "brief": str(title)[:10], "status": "pending"})

            state_obj = _extract_json_object(run["agent_state"] or "") or {}
            message = str(state_obj.get("message") or "").strip() or (str(task_row["title"]) if task_row else "")
            workdir = str(state_obj.get("workdir") or "").strip() or os.getcwd()
            tools_hint = str(state_obj.get("tools_hint") or "").strip() or _list_tool_hints()
            skills_hint = str(state_obj.get("skills_hint") or "").strip() or "(无)"
            # 文档约定：Memory 不注入 Agent 上下文
            memories_hint = "(无)"
            graph_hint = str(state_obj.get("graph_hint") or "").strip()
            model = str(state_obj.get("model") or "").strip()
            if not model:
                model = await asyncio.to_thread(resolve_default_model)
            parameters = state_obj.get("parameters") if isinstance(state_obj.get("parameters"), dict) else {"temperature": 0.2}

            # 兼容：旧 run 没有保存 graph_hint，则重新检索一次（避免 prompt.format KeyError）
            if not graph_hint:
                graph_nodes = await asyncio.to_thread(
                    _select_relevant_graph_nodes,
                    message=message,
                    model=model,
                    parameters=parameters,
                )
                graph_hint = _format_graph_for_prompt(graph_nodes)

            context = state_obj.get("context") if isinstance(state_obj.get("context"), dict) else {"last_llm_response": None}
            observations_raw = state_obj.get("observations")
            observations: List[str] = (
                [str(o).strip() for o in observations_raw if str(o).strip()] if isinstance(observations_raw, list) else []
            )

            paused = state_obj.get("paused") if isinstance(state_obj.get("paused"), dict) else {}
            paused_step_order_raw = paused.get("step_order")
            paused_step_order: Optional[int]
            try:
                paused_step_order = int(paused_step_order_raw) if paused_step_order_raw is not None else None
            except Exception:
                paused_step_order = None

            resume_step_order = paused_step_order or state_obj.get("step_order") or 1
            try:
                resume_step_order = int(resume_step_order)
            except Exception:
                resume_step_order = 1
            if resume_step_order < 1:
                resume_step_order = 1

            # docs/agent：断点定位应以 task_steps 为准（最后一个 status != planned 的 step），
            # 仅在 waiting（paused）场景才优先使用 paused_step_order。
            inferred_from_steps: Optional[int] = None
            if paused_step_order is None and last_active_step_order >= 1:
                if last_active_step_status in {STEP_STATUS_DONE, STEP_STATUS_SKIPPED}:
                    inferred_from_steps = int(last_active_step_order) + 1
                elif last_active_step_status in {STEP_STATUS_RUNNING, STEP_STATUS_FAILED, STEP_STATUS_WAITING}:
                    inferred_from_steps = int(last_active_step_order)

            # 兜底：若缺失 steps（旧 run/损坏数据），仍用 last_done_step 推断下一步，避免跳过未完成步骤。
            if inferred_from_steps is None and paused_step_order is None and last_done_step >= 1:
                inferred_from_steps = int(last_done_step) + 1

            if inferred_from_steps is not None:
                resume_step_order = int(inferred_from_steps)
            else:
                # 兜底：agent_state.step_order 可能被损坏/漂移，避免越界导致错误地判定“已完成”。
                if resume_step_order > len(plan_titles):
                    resume_step_order = len(plan_titles)

            # 继续执行前先把计划栏状态对齐到“已完成/待执行”，避免 UI 显示漂移
            if plan_items:
                for idx, item in enumerate(plan_items, start=1):
                    if not isinstance(item, dict):
                        continue
                    if idx <= last_done_step:
                        item["status"] = "done"
                    elif item.get("status") in {"running", "waiting", "planned"}:
                        item["status"] = "pending"

            # 断点定位结果为 “最后一步已 done” 时，resume 仅需补齐收尾（artifact 校验/后处理），不应重复执行最后一步。
            plan_total_steps = len(plan_titles)
            # 特殊：pending_planning 表示“等待用户补充后再重新规划”，即便当前 plan 只有 user_prompt 也不应判定为已完成。
            pending_planning = bool(state_obj.get("pending_planning")) if isinstance(state_obj, dict) else False
            skip_execution = bool(
                plan_total_steps > 0
                and int(resume_step_order) > int(plan_total_steps)
                and not bool(pending_planning)
            )
            run_status: str
            last_step_order: int = 0
            if skip_execution:
                run_status = RUN_STATUS_DONE
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
                if plan_items:
                    for item in plan_items:
                        if not isinstance(item, dict):
                            continue
                        if item.get("status") not in {"done", "skipped"}:
                            item["status"] = "done"
                    yield sse_json({"type": "plan", "task_id": task_id, "items": plan_items})
                yield sse_json({"delta": f"{STREAM_TAG_EXEC} 计划已全部完成，开始收尾…\n"})

                # 持久化“计划栏最终状态”（避免 UI 刷新后仍看到旧的 pending/running）
                try:
                    updated_at = now_iso()
                    state_obj["step_order"] = int(plan_total_steps)
                    state_obj["paused"] = None
                    await asyncio.to_thread(
                        update_task_run,
                        run_id=int(run_id),
                        agent_plan={
                            "titles": plan_titles,
                            "items": plan_items,
                            "allows": plan_allows,
                            "artifacts": plan_artifacts,
                        },
                        agent_state=state_obj,
                        updated_at=updated_at,
                    )
                except Exception as exc:
                    logger.exception("agent.resume.skip_execution.persist_failed: %s", exc)
                    _safe_write_debug(
                        task_id,
                        run_id,
                        message="agent.resume.skip_execution.persist_failed",
                        data={"error": str(exc)},
                        level="warning",
                    )

                # artifacts 校验：仅在任务“成功结束”时检查
                if run_status == RUN_STATUS_DONE and plan_artifacts:
                    missing = check_missing_artifacts(artifacts=plan_artifacts, workdir=workdir)
                    if missing:
                        run_status = RUN_STATUS_FAILED
                        _safe_write_debug(
                            task_id,
                            run_id,
                            message="agent.artifacts.missing",
                            data={"missing": missing},
                            level="error",
                        )
                        yield sse_json({"delta": f"{STREAM_TAG_FAIL} 未生成文件：{', '.join(missing)}\n"})

                # 结束：落库 run/task 状态（waiting 不写 finished_at）
                await asyncio.to_thread(
                    finalize_run_and_task_status,
                    task_id=int(task_id),
                    run_id=int(run_id),
                    run_status=str(run_status),
                )

                # 自动记忆：把本次 run 的“最终结果摘要”写入 memory_items，并通过 SSE 通知前端即时更新
                if run_status == RUN_STATUS_DONE and task_id and run_id:
                    try:
                        from backend.src.services.tasks.task_postprocess import write_task_result_memory_if_missing

                        title = str(task_row["title"] or "").strip() if task_row else ""
                        item = await asyncio.to_thread(
                            write_task_result_memory_if_missing,
                            task_id=int(task_id),
                            run_id=int(run_id),
                            title=title,
                        )
                        if isinstance(item, dict) and item.get("id") is not None:
                            yield sse_json(
                                {
                                    "type": SSE_TYPE_MEMORY_ITEM,
                                    "task_id": int(task_id),
                                    "run_id": int(run_id),
                                    "item": item,
                                }
                            )
                    except Exception as exc:
                        _safe_write_debug(
                            task_id,
                            run_id,
                            message="agent.memory.auto_task_result_failed",
                            data={"error": str(exc)},
                            level="warning",
                        )

                # 后处理闭环：
                # - done：完整后处理（评估/技能/图谱）
                # - failed/stopped：至少保证评估记录可见
                if run_status in {RUN_STATUS_DONE, RUN_STATUS_FAILED, RUN_STATUS_STOPPED} and task_id and run_id:
                    enqueue_postprocess_thread(task_id=int(task_id), run_id=int(run_id), run_status=str(run_status))

                yield sse_json({"type": "done"}, event="done")
                return

            question = str(paused.get("question") or "").strip()
            if not skip_execution:
                answer_line = f"user_input: {_truncate_observation(user_input)}"
                if question:
                    answer_line = f"user_prompt: {_truncate_observation(question)} => {_truncate_observation(user_input)}"
                observations.append(answer_line)

            if not skip_execution:
                # 写入“用户回答”到 outputs，便于审计/调试
                created_at = now_iso()
                try:
                    await asyncio.to_thread(
                        create_task_output,
                        task_id=int(task_id),
                        run_id=int(run_id),
                        output_type=TASK_OUTPUT_TYPE_USER_ANSWER,
                        content=str(user_input or ""),
                        created_at=created_at,
                    )
                except Exception as exc:
                    logger.exception("agent.user_answer.output_write_failed: %s", exc)
                    _safe_write_debug(
                        task_id,
                        run_id,
                        message="agent.user_answer.output_write_failed",
                        data={"error": str(exc)},
                        level="warning",
                    )

                # docs/agent：若 paused 由 user_prompt 动作触发，则会保存 step_id；恢复时把该 step 从 waiting -> done。
                paused_step_id = paused.get("step_id")
                if paused_step_id is not None:
                    try:
                        sid = int(paused_step_id)
                    except Exception:
                        sid = 0
                    if sid > 0:
                        try:
                            result_value = json.dumps(
                                {"question": question, "answer": str(user_input or "")},
                                ensure_ascii=False,
                            )
                        except Exception:
                            result_value = json.dumps(
                                {"question": str(question), "answer": str(user_input or "")},
                                ensure_ascii=False,
                            )
                        try:
                            last_exc: Optional[BaseException] = None
                            for attempt in range(0, 3):
                                try:
                                    await asyncio.to_thread(
                                        mark_task_step_done,
                                        step_id=int(sid),
                                        result=result_value,
                                        finished_at=created_at,
                                    )
                                    last_exc = None
                                    break
                                except Exception as exc:
                                    last_exc = exc
                                    # sqlite 争用时短暂重试，避免“恢复已完成但 waiting step 没被结算”的漂移
                                    if "locked" in str(exc or "").lower() and attempt < 2:
                                        await asyncio.sleep(0.05 * (attempt + 1))
                                        continue
                                    break
                            if last_exc:
                                _safe_write_debug(
                                    task_id,
                                    run_id,
                                    message="agent.user_prompt.step_done_failed",
                                    data={"step_id": int(sid), "error": str(last_exc)},
                                    level="warning",
                                )
                        except Exception as exc:
                            _safe_write_debug(
                                task_id,
                                run_id,
                                message="agent.user_prompt.step_done_failed",
                                data={"step_id": int(sid), "error": str(exc)},
                                level="warning",
                            )

            # user_prompt 专用：若该步骤仅允许 user_prompt，则用户已回答后应直接进入下一步，避免重复提问卡死。
            if paused_step_order is not None and resume_step_order == int(paused_step_order) and 1 <= int(paused_step_order) <= len(plan_titles):
                paused_idx = int(paused_step_order) - 1
                paused_title = str(plan_titles[paused_idx] or "")
                paused_allows = plan_allows[paused_idx] if 0 <= paused_idx < len(plan_allows) else []

                allow_set = set(str(a).strip() for a in (paused_allows or []) if str(a).strip())
                is_user_prompt_only = (ACTION_TYPE_USER_PROMPT in allow_set) and (len(allow_set) == 1)
                if is_user_prompt_only and not is_task_feedback_step_title(paused_title):
                    if int(paused_step_order) < len(plan_titles):
                        resume_step_order = int(paused_step_order) + 1
                        if 0 <= paused_idx < len(plan_items) and isinstance(plan_items[paused_idx], dict):
                            plan_items[paused_idx]["status"] = "done"
                    else:
                        _safe_write_debug(
                            task_id,
                            run_id,
                            message="agent.user_prompt.only_step_is_last",
                            data={"step_order": int(paused_step_order), "title": paused_title},
                            level="warning",
                        )

            # 恢复 run/task 状态
            state_obj["paused"] = None
            state_obj["last_user_input"] = str(user_input or "")
            state_obj["last_user_prompt"] = question
            state_obj["observations"] = observations
            state_obj["context"] = context
            state_obj["step_order"] = resume_step_order
            updated_at = now_iso()
            try:
                await asyncio.to_thread(
                    update_task_run,
                    run_id=int(run_id),
                    status=RUN_STATUS_RUNNING,
                    agent_plan={
                        "titles": plan_titles,
                        "items": plan_items,
                        "allows": plan_allows,
                        "artifacts": plan_artifacts,
                    },
                    agent_state=state_obj,
                    clear_finished_at=True,
                    updated_at=updated_at,
                )
                await asyncio.to_thread(
                    update_task,
                    task_id=int(task_id),
                    status=STATUS_RUNNING,
                    updated_at=updated_at,
                )
            except Exception as exc:
                logger.exception("agent.resume_state.persist_failed: %s", exc)
                _safe_write_debug(
                    task_id,
                    run_id,
                    message="agent.resume_state.persist_failed",
                    data={"error": str(exc)},
                    level="error",
                )

            yield sse_json({"delta": f"{STREAM_TAG_EXEC} 已收到输入，继续执行…\n"})

            # docs/agent：知识充分性建议 ask_user 时，会先进入 waiting；用户补充后需要“重新检索 + 重新规划”再执行。
            if isinstance(state_obj, dict) and bool(state_obj.get("pending_planning")):
                out_q: "asyncio.Queue[str]" = asyncio.Queue()

                def _emit_pending(msg: str) -> None:
                    try:
                        out_q.put_nowait(str(msg))
                    except Exception:
                        return

                pending_task = asyncio.create_task(
                    resume_pending_planning_after_user_input(
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
                        yield_func=_emit_pending,
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
                    )
                )
                async for msg in pump_async_task_messages(pending_task, out_q):
                    yield msg
                pending_result = await pending_task
                outcome = str(pending_result.get("outcome") or "").strip()
                if outcome in {"waiting", "failed"}:
                    return

                # outcome == planned：用新 plan/状态继续后续执行
                plan_titles = list(pending_result.get("plan_titles") or plan_titles)
                plan_items = list(pending_result.get("plan_items") or plan_items)
                plan_allows = list(pending_result.get("plan_allows") or plan_allows)
                plan_artifacts = list(pending_result.get("plan_artifacts") or plan_artifacts)
                state_obj = pending_result.get("agent_state") if isinstance(pending_result.get("agent_state"), dict) else state_obj
                message = str(pending_result.get("message") or message).strip() or message
                # 更新运行时 hints：后续执行器读取的是本地变量（而非 state_obj），必须同步。
                tools_hint = str(state_obj.get("tools_hint") or tools_hint).strip() or tools_hint
                skills_hint = str(state_obj.get("skills_hint") or skills_hint).strip() or skills_hint
                solutions_hint = str(state_obj.get("solutions_hint") or solutions_hint).strip() or solutions_hint
                graph_hint = str(state_obj.get("graph_hint") or graph_hint).strip() or graph_hint
                try:
                    resume_step_order = int(pending_result.get("resume_step_order") or resume_step_order)
                except Exception:
                    resume_step_order = int(resume_step_order)

            mode = str(state_obj.get("mode") or "").strip().lower()
            run_status: str
            last_step_order: int = 0

            if mode == "think":
                # Think 模式恢复：保持“executor 选模型 + 失败反思”语义
                raw_cfg = state_obj.get("think_config")
                if isinstance(raw_cfg, dict) and raw_cfg:
                    think_config = create_think_config_from_dict(raw_cfg, base_model=model)
                else:
                    think_config = get_default_think_config(base_model=model)

                # 保底：反思机制需要 planners；执行需要 executors。若缺失则用默认补齐。
                default_cfg = get_default_think_config(base_model=model)
                if not getattr(think_config, "planners", None):
                    think_config.planners = default_cfg.planners
                if not getattr(think_config, "executors", None):
                    think_config.executors = default_cfg.executors

                llm_call_func = _create_llm_call_func(model, parameters)

                def _resolve_step_llm_config(step_order: int, title: str, allow: List[str]):
                    role = _infer_executor_from_allow(allow or [], title or "")
                    cfg = think_config.get_executor(role) or think_config.get_executor("executor_code")
                    resolved_model = model
                    overrides: Dict = {}
                    if cfg:
                        if isinstance(getattr(cfg, "model", None), str) and str(cfg.model).strip():
                            resolved_model = str(cfg.model).strip()
                        if getattr(cfg, "temperature", None) is not None:
                            overrides["temperature"] = float(cfg.temperature)
                        if getattr(cfg, "max_tokens", None) is not None:
                            overrides["max_tokens"] = int(cfg.max_tokens)
                    return resolved_model, overrides

                reflection_count = 0
                try:
                    reflection_count = int(state_obj.get("reflection_count") or 0)
                except Exception:
                    reflection_count = 0
                max_reflection_rounds = int(THINK_REFLECTION_MAX_ROUNDS or 2)
                start_step_order = int(resume_step_order)

                # 保底：缺失 executor_assignments 时重建一份（用于评估输入与可观察性）。
                if isinstance(state_obj, dict) and not isinstance(state_obj.get("executor_assignments"), list):
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

                # plan_briefs 仅用于反思插入步骤；从 plan_items 派生，避免与 plan_patch 漂移
                plan_briefs = _build_plan_briefs_from_items(plan_titles=plan_titles, plan_items=plan_items)

                while True:
                    has_feedback_tail = bool(plan_titles) and is_task_feedback_step_title(str(plan_titles[-1] or ""))
                    parallel_end = len(plan_titles) - 1 if has_feedback_tail else len(plan_titles)
                    tail_step_order = parallel_end + 1 if has_feedback_tail else None

                    # resume 场景不信任旧依赖（可能已被反思/插入步骤修改），统一本地推断依赖
                    parallel_dependencies = None
                    try:
                        saved = state_obj.get("think_parallel_dependencies") if isinstance(state_obj, dict) else None
                        if isinstance(saved, list):
                            normalized: List[dict] = []
                            for item in saved:
                                if not isinstance(item, dict):
                                    continue
                                raw_idx = item.get("step_index")
                                raw_deps = item.get("depends_on")
                                try:
                                    step_idx = int(raw_idx) if raw_idx is not None else None
                                except Exception:
                                    step_idx = None
                                if step_idx is None or not (0 <= int(step_idx) < len(plan_titles)):
                                    continue
                                deps_list: List[int] = []
                                if isinstance(raw_deps, list):
                                    for d in raw_deps:
                                        try:
                                            dv = int(d)
                                        except Exception:
                                            continue
                                        if 0 <= int(dv) < len(plan_titles) and int(dv) != int(step_idx):
                                            deps_list.append(int(dv))
                                normalized.append({"step_index": int(step_idx), "depends_on": sorted(set(deps_list))})
                            if normalized:
                                parallel_dependencies = normalized
                    except Exception:
                        parallel_dependencies = None
                    if parallel_dependencies is None:
                        try:
                            inferred = infer_executor_assignments(
                                plan_titles=plan_titles,
                                plan_allows=plan_allows,
                                plan_artifacts=plan_artifacts,
                            )
                            inferred_deps: List[dict] = []
                            for a in inferred.assignments or []:
                                deps = getattr(a, "depends_on", None)
                                if isinstance(deps, list) and deps:
                                    inferred_deps.append(
                                        {"step_index": int(a.step_index), "depends_on": [int(d) for d in deps]}
                                    )
                            parallel_dependencies = inferred_deps or None
                        except Exception:
                            parallel_dependencies = None

                    inner_parallel = run_think_parallel_loop(
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
                        agent_state=state_obj,
                        context=context,
                        observations=observations,
                        start_step_order=int(start_step_order),
                        end_step_order_inclusive=int(parallel_end),
                        variables_source="agent_think_parallel_resume",
                        step_llm_config_resolver=_resolve_step_llm_config,
                        dependencies=parallel_dependencies,
                        executor_roles=list((think_config.executors or {}).keys()),
                        llm_call=react_loop_facade.create_llm_call,
                        execute_step_action=react_loop_facade._execute_step_action,
                        safe_write_debug=_safe_write_debug,
                    )

                    exec_started_at = time.monotonic()
                    parallel_result = None
                    async for kind, payload in pump_sync_generator(
                        inner=inner_parallel,
                        label="think_parallel_resume",
                        poll_interval_seconds=AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
                        idle_timeout_seconds=AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
                    ):
                        if kind == "msg":
                            if payload:
                                yield str(payload)
                            continue
                        if kind == "done":
                            parallel_result = payload
                            break
                        if kind == "err":
                            if isinstance(payload, BaseException):
                                raise payload  # noqa: TRY301
                            raise RuntimeError(f"think_parallel_resume 异常:{payload}")  # noqa: TRY301

                    if parallel_result is None:
                        raise RuntimeError("think_parallel_resume 返回为空")  # noqa: TRY301

                    run_status = str(parallel_result.run_status or "")
                    last_step_order = int(getattr(parallel_result, "last_step_order", 0) or 0)

                    # 并行阶段成功后，顺序执行确认满意度（保持反馈闭环与评估门闩语义）
                    if run_status == RUN_STATUS_DONE and tail_step_order is not None:
                        inner_tail = run_react_loop(
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
                            agent_state=state_obj,
                            context=context,
                            observations=observations,
                            start_step_order=int(tail_step_order),
                            variables_source="agent_think_react_tail_resume",
                            step_llm_config_resolver=_resolve_step_llm_config,
                        )

                        tail_result = None
                        async for kind, payload in pump_sync_generator(
                            inner=inner_tail,
                            label="think_react_tail_resume",
                            poll_interval_seconds=AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS,
                            idle_timeout_seconds=AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS,
                        ):
                            if kind == "msg":
                                if payload:
                                    yield str(payload)
                                continue
                            if kind == "done":
                                tail_result = payload
                                break
                            if kind == "err":
                                if isinstance(payload, BaseException):
                                    raise payload  # noqa: TRY301
                                raise RuntimeError(f"think_react_tail_resume 异常:{payload}")  # noqa: TRY301

                        if tail_result is None:
                            raise RuntimeError("think_react_tail_resume 返回为空")  # noqa: TRY301

                        run_status = str(tail_result.run_status or "")
                        last_step_order = int(getattr(tail_result, "last_step_order", 0) or 0)

                    duration_ms = int((time.monotonic() - exec_started_at) * 1000)
                    _safe_write_debug(
                        task_id,
                        run_id,
                        message="agent.think.exec_resume.done",
                        data={
                            "duration_ms": duration_ms,
                            "run_status": str(run_status),
                            "last_step_order": int(last_step_order),
                            "reflection_count": int(reflection_count),
                            "has_feedback_tail": bool(has_feedback_tail),
                        },
                        level="info",
                    )

                    if run_status != RUN_STATUS_FAILED:
                        break

                    if reflection_count >= max_reflection_rounds:
                        yield sse_json({"delta": f"{STREAM_TAG_FAIL} 已达反思次数上限（{max_reflection_rounds}次），停止执行\n"})
                        break

                    reflection_count += 1
                    state_obj["reflection_count"] = int(reflection_count)
                    yield sse_json({"delta": f"{STREAM_TAG_REFLECTION} 执行失败，启动第 {reflection_count} 次多模型反思…\n"})

                    # 反思输入：尽量用最新计划栏与观测
                    plan_briefs = _build_plan_briefs_from_items(plan_titles=plan_titles, plan_items=plan_items)
                    done_step_indices = [i for i, it in enumerate(plan_items or []) if isinstance(it, dict) and it.get("status") == "done"]
                    observations_text = "\n".join(observations[-10:]) if observations else "(无观测)"

                    reflection_progress: List[str] = []

                    def collect_reflection_progress(msg: str):
                        reflection_progress.append(msg)

                    reflection_result = await asyncio.to_thread(
                        run_reflection,
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
                        yield sse_json({"delta": f"{msg}\n"})

                    if not getattr(reflection_result, "fix_steps", None):
                        yield sse_json({"delta": f"{STREAM_TAG_FAIL} 反思未能生成修复步骤，停止执行\n"})
                        break

                    # 记录反思摘要到 agent_state（用于评估/审计）。
                    try:
                        records = state_obj.get("reflection_records") if isinstance(state_obj, dict) else None
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
                                        "title": str(s.get("title") or "").strip(),
                                        "brief": str(s.get("brief") or "").strip(),
                                        "allow": s.get("allow") if isinstance(s.get("allow"), list) else [],
                                    }
                                    for s in (reflection_result.fix_steps or [])
                                    if isinstance(s, dict)
                                ],
                            }
                        )
                        state_obj["reflection_records"] = records
                    except Exception:
                        pass

                    # docs/agent：反思接管后，“失败步骤”不应继续阻塞依赖图与最终输出门闩。
                    # 策略：
                    # 1) 将该步骤在计划栏标记为 skipped（代表旧尝试已作废）
                    # 2) 同步把对应 task_steps 从 failed 改为 skipped（保留 error 便于溯源；避免评估链路被“历史 failed”硬阻塞）
                    # 3) 在其后插入修复步骤；默认追加 1 个“重试原步骤”的步骤提升收敛概率
                    failed_step_index = max(0, int(last_step_order) - 1)
                    failed_title = (
                        str(plan_titles[failed_step_index] or "").strip()
                        if 0 <= failed_step_index < len(plan_titles)
                        else ""
                    )
                    raw_failed_allow = plan_allows[failed_step_index] if 0 <= failed_step_index < len(plan_allows) else []
                    failed_allow: List[str] = (
                        [str(a).strip() for a in raw_failed_allow if str(a).strip()]
                        if isinstance(raw_failed_allow, list)
                        else []
                    )

                    allow_set = {str(a or "").strip().lower() for a in (failed_allow or []) if str(a or "").strip()}
                    can_retry = bool(failed_title) and ("task_output" not in allow_set) and ("user_prompt" not in allow_set)

                    max_fix_steps_value = 3
                    fix_steps_for_merge = list(reflection_result.fix_steps or [])
                    if can_retry:
                        fix_steps_for_merge = fix_steps_for_merge[: max(0, int(max_fix_steps_value) - 1)]
                        fix_steps_for_merge.append(
                            {
                                "title": failed_title,
                                "brief": "重试",
                                "allow": list(failed_allow or []),
                            }
                        )
                    else:
                        fix_steps_for_merge = fix_steps_for_merge[: int(max_fix_steps_value)]

                    if not fix_steps_for_merge:
                        yield sse_json({"delta": f"{STREAM_TAG_FAIL} 修复步骤为空，停止执行\n"})
                        break

                    # 将“失败步骤”的 task_step 标记为 skipped（保留 error 字段用于溯源）
                    try:
                        step_rows = await asyncio.to_thread(
                            list_task_steps_for_run,
                            task_id=int(task_id),
                            run_id=int(run_id),
                        )
                        target_id: Optional[int] = None
                        target_error: str = ""
                        for row in reversed(step_rows or []):
                            try:
                                if int(row["step_order"] or 0) != int(last_step_order):
                                    continue
                                if str(row["status"] or "").strip() != str(STEP_STATUS_FAILED or "failed"):
                                    continue
                                target_id = int(row["id"])
                                target_error = str(row["error"] or "").strip()
                                break
                            except Exception:
                                continue
                        if target_id is not None:
                            await asyncio.to_thread(
                                mark_task_step_skipped,
                                step_id=int(target_id),
                                error=target_error or "skipped_by_reflection",
                                finished_at=now_iso(),
                            )
                    except Exception as exc:
                        _safe_write_debug(
                            task_id,
                            run_id,
                            message="agent.think.resume.reflection.mark_failed_step_skipped_failed",
                            data={"step_order": int(last_step_order), "error": str(exc)},
                            level="warning",
                        )

                    # 合并修复步骤到计划
                    new_titles, new_briefs, new_allows = merge_fix_steps_into_plan(
                        current_step_index=failed_step_index,
                        plan_titles=plan_titles,
                        plan_briefs=plan_briefs,
                        plan_allows=plan_allows,
                        fix_steps=list(fix_steps_for_merge or []),
                    )
                    fix_count = len(fix_steps_for_merge or [])

                    plan_titles = new_titles
                    plan_briefs = new_briefs
                    plan_allows = new_allows

                    # 重建 plan_items：保留 done/skipped；插入步骤为 pending
                    old_plan_items = list(plan_items or [])
                    insert_pos = failed_step_index + 1

                    new_items: List[dict] = []
                    for i, title in enumerate(plan_titles):
                        brief = plan_briefs[i] if i < len(plan_briefs) else ""
                        allow = plan_allows[i] if i < len(plan_allows) else []
                        status = "pending"
                        if i in done_step_indices:
                            status = "done"
                        elif i == failed_step_index:
                            status = "skipped"
                        elif insert_pos <= i < insert_pos + fix_count:
                            status = "pending"
                        else:
                            old_index = i
                            if i >= insert_pos + fix_count:
                                old_index = i - fix_count
                            if 0 <= old_index < len(old_plan_items) and isinstance(old_plan_items[old_index], dict):
                                raw = str(old_plan_items[old_index].get("status") or "").strip() or "pending"
                                if raw in {"running", "waiting", "planned"}:
                                    raw = "pending"
                                status = raw
                        new_items.append({"id": i + 1, "title": title, "brief": brief, "allow": allow, "status": status})
                    plan_items = new_items

                    # 更新 agent_state 与持久化（保证 resume 不丢步骤）
                    state_obj["plan_titles"] = plan_titles
                    state_obj["plan_briefs"] = plan_briefs
                    state_obj["plan_allows"] = plan_allows
                    # 计划变更后重建 executor_assignments（避免分工/审计信息漂移）
                    try:
                        assignments_payload: List[dict] = []
                        for i, title in enumerate(plan_titles or []):
                            allow = plan_allows[i] if 0 <= i < len(plan_allows or []) else []
                            role = _infer_executor_from_allow(allow or [], str(title or ""))
                            assignments_payload.append(
                                {"step_order": int(i) + 1, "executor": role, "allow": list(allow or [])}
                            )
                        state_obj["executor_assignments"] = assignments_payload
                    except Exception:
                        pass
                    updated_at = now_iso()
                    try:
                        await asyncio.to_thread(
                            update_task_run,
                            run_id=int(run_id),
                            agent_plan={
                                "titles": plan_titles,
                                "items": plan_items,
                                "allows": plan_allows,
                                "artifacts": plan_artifacts,
                            },
                            agent_state=state_obj,
                            updated_at=updated_at,
                        )
                    except Exception as exc:
                        logger.exception("agent.think.resume.reflection.persist_failed: %s", exc)
                        _safe_write_debug(
                            task_id,
                            run_id,
                            message="agent.think.resume.reflection.persist_failed",
                            data={"error": str(exc)},
                            level="warning",
                        )

                    yield sse_json({"type": "plan", "task_id": task_id, "items": plan_items})
                    yield sse_json({"delta": f"{STREAM_TAG_REFLECTION} 反思完成，继续从步骤 {last_step_order + 1} 执行…\n"})

                    start_step_order = last_step_order + 1

            else:
                # Do 模式恢复：单次 ReAct 执行
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
                    agent_state=state_obj,
                    context=context,
                    observations=observations,
                    start_step_order=resume_step_order,
                    variables_source="agent_react_resume",
                )
                react_started_at = time.monotonic()
                react_result = None
                async for kind, payload in pump_sync_generator(
                    inner=inner_react,
                    label="react_resume",
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
                        raise RuntimeError(f"react_resume 异常:{payload}")  # noqa: TRY301

                if react_result is None:
                    raise RuntimeError("react_resume 返回为空")  # noqa: TRY301
                run_status = str(react_result.run_status or "")
                last_step_order = int(getattr(react_result, "last_step_order", 0) or 0)
                duration_ms = int((time.monotonic() - react_started_at) * 1000)
                _safe_write_debug(
                    task_id,
                    run_id,
                    message="agent.react_resume.done",
                    data={
                        "duration_ms": duration_ms,
                        "run_status": str(run_status),
                        "last_step_order": int(last_step_order),
                    },
                    level="info",
                )

            # artifacts 校验：仅在任务“成功结束”时检查
            if run_status == RUN_STATUS_DONE and plan_artifacts:
                missing = check_missing_artifacts(artifacts=plan_artifacts, workdir=workdir)
                if missing:
                    run_status = RUN_STATUS_FAILED
                    _safe_write_debug(
                        task_id,
                        run_id,
                        message="agent.artifacts.missing",
                        data={"missing": missing},
                        level="error",
                    )
                    yield sse_json({"delta": f"{STREAM_TAG_FAIL} 未生成文件：{', '.join(missing)}\n"})

            # 计划栏收尾
            if plan_items and run_status in {RUN_STATUS_DONE, RUN_STATUS_FAILED}:
                for item in plan_items:
                    if item.get("status") == "running":
                        item["status"] = "done" if run_status == RUN_STATUS_DONE else "failed"
                yield sse_json({"type": "plan", "task_id": task_id, "items": plan_items})

            # 结束：落库 run/task 状态（waiting 不写 finished_at）
            await asyncio.to_thread(
                finalize_run_and_task_status,
                task_id=int(task_id),
                run_id=int(run_id),
                run_status=str(run_status),
            )

            # 评估触发点（确认满意度等待）：
            # - waiting 状态不会触发 postprocess，因此需要在此异步触发一次评估，
            #   让世界页的“评估 plan-list”能及时展示。
            if run_status == RUN_STATUS_WAITING and task_id and run_id:
                enqueue_review_on_feedback_waiting(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    agent_state=state_obj,
                )

            # 自动记忆：把本次 run 的“最终结果摘要”写入 memory_items，并通过 SSE 通知前端即时更新（避免记忆页轮询）
            if run_status == RUN_STATUS_DONE and task_id and run_id:
                try:
                    from backend.src.services.tasks.task_postprocess import write_task_result_memory_if_missing

                    title = str(task_row["title"] or "").strip() if task_row else ""
                    item = await asyncio.to_thread(
                        write_task_result_memory_if_missing,
                        task_id=int(task_id),
                        run_id=int(run_id),
                        title=title,
                    )
                    if isinstance(item, dict) and item.get("id") is not None:
                        yield sse_json(
                            {
                                "type": SSE_TYPE_MEMORY_ITEM,
                                "task_id": int(task_id),
                                "run_id": int(run_id),
                                "item": item,
                            }
                        )
                except Exception as exc:
                    _safe_write_debug(
                        task_id,
                        run_id,
                        message="agent.memory.auto_task_result_failed",
                        data={"error": str(exc)},
                        level="warning",
                    )

            # 后处理闭环：
            # - done：完整后处理（评估/技能/图谱）
            # - failed/stopped：至少保证评估记录可见（避免用户误以为“评估没触发”）
            if run_status in {RUN_STATUS_DONE, RUN_STATUS_FAILED, RUN_STATUS_STOPPED} and task_id and run_id:
                enqueue_postprocess_thread(task_id=int(task_id), run_id=int(run_id), run_status=str(run_status))

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
            yield sse_json({"type": "done"}, event="done")
        except BaseException:
            return

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(gen(), media_type="text/event-stream", headers=headers)
