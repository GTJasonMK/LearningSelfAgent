from __future__ import annotations

import asyncio
import inspect
import json
import logging
from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple

from backend.src.agent.core.run_context import AgentRunContext
from backend.src.agent.core.checkpoint_store import persist_checkpoint_async
from backend.src.agent.runner.run_stage import persist_run_stage
from backend.src.common.sql import is_sqlite_locked_error, sqlite_retry_sleep_seconds
from backend.src.common.utils import coerce_int, now_iso, parse_optional_int, parse_positive_int
from backend.src.services.llm.llm_client import sse_json
from backend.src.constants import (
    ACTION_TYPE_USER_PROMPT,
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_RUNNING,
    RUN_STATUS_STOPPED,
    SSE_TYPE_MEMORY_ITEM,
    STATUS_RUNNING,
    STEP_STATUS_DONE,
    STEP_STATUS_FAILED,
    STEP_STATUS_RUNNING,
    STEP_STATUS_SKIPPED,
    STEP_STATUS_WAITING,
    STREAM_TAG_EXEC,
    STREAM_TAG_FAIL,
    TASK_OUTPUT_TYPE_USER_ANSWER,
)
from backend.src.services.tasks.task_queries import create_task_output, mark_task_step_done, update_task
from backend.src.services.tasks.task_run_lifecycle import (
    check_missing_artifacts,
    enqueue_postprocess_thread,
    finalize_run_and_task_status,
)

logger = logging.getLogger(__name__)
_RESUME_SYNC_OP_TIMEOUT_SECONDS = 3.0


async def _run_sync_with_timeout(
    func: Callable[..., object],
    *args,
    timeout_seconds: float = _RESUME_SYNC_OP_TIMEOUT_SECONDS,
    **kwargs,
):
    """
    在线程池执行同步函数，并对单次调用施加超时保护。

    目的：
    - 防止个别同步依赖（DB/文件锁）异常卡死，拖挂整个 resume 流。
    """
    try:
        timeout_value = float(timeout_seconds)
    except Exception:
        timeout_value = float(_RESUME_SYNC_OP_TIMEOUT_SECONDS)
    if timeout_value <= 0:
        timeout_value = float(_RESUME_SYNC_OP_TIMEOUT_SECONDS)
    # 单测里的 Mock/AsyncMock 走直连执行，避免 to_thread 在线程池内卡死且不可取消。
    func_type_module = str(getattr(type(func), "__module__", "") or "")
    if func_type_module.startswith("unittest.mock"):
        result = func(*args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result
    try:
        return await asyncio.wait_for(asyncio.to_thread(func, *args, **kwargs), timeout=timeout_value)
    except asyncio.TimeoutError as exc:
        name = getattr(func, "__name__", None) or getattr(type(func), "__name__", "sync_func")
        raise TimeoutError(f"resume sync op timeout: {name}") from exc


@dataclass
class ResumeStepDecision:
    resume_step_order: int
    skip_execution: bool


def infer_resume_step_decision(
    *,
    paused_step_order: Optional[int],
    state_step_order: Optional[int],
    last_done_step: int,
    last_active_step_order: int,
    last_active_step_status: str,
    plan_total_steps: int,
    pending_planning: bool,
) -> ResumeStepDecision:
    """
    统一恢复起点推断：
    - waiting 场景优先 paused_step_order；
    - 其他场景优先 task_steps 最后活跃记录；
    - 无活跃记录再用 last_done_step 兜底。
    """
    resume_step_order = int(parse_positive_int(paused_step_order or state_step_order or 1, default=1) or 1)
    last_done_step_value = coerce_int(last_done_step, default=0)
    last_active_step_order_value = coerce_int(last_active_step_order, default=0)
    plan_total_steps_value = coerce_int(plan_total_steps, default=0)

    inferred_from_steps: Optional[int] = None
    if paused_step_order is None and last_active_step_order_value >= 1:
        active_status = str(last_active_step_status or "").strip()
        if active_status in {STEP_STATUS_DONE, STEP_STATUS_SKIPPED}:
            inferred_from_steps = last_active_step_order_value + 1
        elif active_status in {STEP_STATUS_RUNNING, STEP_STATUS_FAILED, STEP_STATUS_WAITING}:
            inferred_from_steps = last_active_step_order_value

    if inferred_from_steps is None and paused_step_order is None and last_done_step_value >= 1:
        inferred_from_steps = last_done_step_value + 1

    if inferred_from_steps is not None:
        resume_step_order = inferred_from_steps
    elif plan_total_steps_value > 0 and resume_step_order > plan_total_steps_value:
        resume_step_order = plan_total_steps_value

    skip_execution = bool(
        plan_total_steps_value > 0
        and resume_step_order > plan_total_steps_value
        and not bool(pending_planning)
    )
    return ResumeStepDecision(
        resume_step_order=int(resume_step_order),
        skip_execution=bool(skip_execution),
    )


def normalize_plan_items_for_resume(
    *,
    plan_items: List[dict],
    last_done_step: int,
) -> None:
    """
    按 task_steps 同步计划栏状态，避免 UI 与恢复点漂移。
    """
    if not isinstance(plan_items, list):
        return
    for idx, item in enumerate(plan_items, start=1):
        if not isinstance(item, dict):
            continue
        if idx <= int(last_done_step):
            item["status"] = "done"
            continue
        if item.get("status") in {"running", "waiting", "planned"}:
            item["status"] = "pending"


async def apply_resume_user_input(
    *,
    task_id: int,
    run_id: int,
    user_input: str,
    question: str,
    paused: dict,
    paused_step_order: Optional[int],
    resume_step_order: int,
    plan_titles: List[str],
    plan_items: List[dict],
    plan_allows: List[List[str]],
    plan_artifacts: List[str],
    observations: List[str],
    context: dict,
    state_obj: dict,
    safe_write_debug: Callable[..., None],
    is_task_feedback_step_title_func: Callable[[str], bool],
) -> Tuple[int, dict]:
    """
    恢复执行前处理用户输入：
    - 记录用户回答与 user_prompt 步骤结算；
    - user_prompt-only 步骤自动跳转到下一步；
    - 统一回写 run/task 运行中状态与 checkpoint。
    """
    created_at = now_iso()
    resume_step_order_value = coerce_int(resume_step_order, default=1)
    paused_step_order_value = parse_optional_int(paused_step_order, default=None)
    try:
        await _run_sync_with_timeout(
            create_task_output,
            task_id=int(task_id),
            run_id=int(run_id),
            output_type=TASK_OUTPUT_TYPE_USER_ANSWER,
            content=str(user_input or ""),
            created_at=created_at,
        )
    except Exception as exc:
        logger.exception("agent.user_answer.output_write_failed: %s", exc)
        safe_write_debug(
            task_id,
            run_id,
            message="agent.user_answer.output_write_failed",
            data={"error": str(exc)},
            level="warning",
        )

    paused_step_id = paused.get("step_id")
    if paused_step_id is not None:
        step_id = int(parse_positive_int(paused_step_id, default=0) or 0)
        if step_id > 0:
            result_value = json.dumps(
                {"question": str(question or ""), "answer": str(user_input or "")},
                ensure_ascii=False,
            )
            try:
                last_exc: Optional[BaseException] = None
                for attempt in range(0, 3):
                    try:
                        await _run_sync_with_timeout(
                            mark_task_step_done,
                            step_id=int(step_id),
                            result=result_value,
                            finished_at=created_at,
                        )
                        last_exc = None
                        break
                    except Exception as exc:
                        last_exc = exc
                        if is_sqlite_locked_error(exc) and attempt < 2:
                            await asyncio.sleep(sqlite_retry_sleep_seconds(attempt))
                            continue
                        break
                if last_exc:
                    safe_write_debug(
                        task_id,
                        run_id,
                        message="agent.user_prompt.step_done_failed",
                        data={"step_id": int(step_id), "error": str(last_exc)},
                        level="warning",
                    )
            except Exception as exc:
                safe_write_debug(
                    task_id,
                    run_id,
                    message="agent.user_prompt.step_done_failed",
                    data={"step_id": int(step_id), "error": str(exc)},
                    level="warning",
                )

    if (
        paused_step_order_value is not None
        and resume_step_order_value == paused_step_order_value
        and 1 <= paused_step_order_value <= len(plan_titles or [])
    ):
        paused_idx = paused_step_order_value - 1
        paused_title = str(plan_titles[paused_idx] or "")
        paused_allows = plan_allows[paused_idx] if 0 <= paused_idx < len(plan_allows or []) else []
        allow_set = set(str(item).strip() for item in (paused_allows or []) if str(item).strip())
        is_user_prompt_only = (ACTION_TYPE_USER_PROMPT in allow_set) and (len(allow_set) == 1)
        if is_user_prompt_only and not is_task_feedback_step_title_func(paused_title):
            if paused_step_order_value < len(plan_titles):
                resume_step_order_value = paused_step_order_value + 1
                if 0 <= paused_idx < len(plan_items) and isinstance(plan_items[paused_idx], dict):
                    plan_items[paused_idx]["status"] = "done"
            else:
                safe_write_debug(
                    task_id,
                    run_id,
                    message="agent.user_prompt.only_step_is_last",
                    data={"step_order": paused_step_order_value, "title": paused_title},
                    level="warning",
                )

    state_obj["paused"] = None
    state_obj["last_user_input"] = str(user_input or "")
    state_obj["last_user_prompt"] = str(question or "")
    state_obj["observations"] = list(observations or [])
    state_obj["context"] = dict(context or {})
    state_obj["step_order"] = int(resume_step_order_value)
    run_ctx = AgentRunContext.from_agent_state(state_obj)

    updated_at = now_iso()
    state_obj, persist_error, _ = await persist_run_stage(
        run_ctx=run_ctx,
        task_id=int(task_id),
        run_id=int(run_id),
        stage="execute",
        where="resume.running.persist",
        safe_write_debug=safe_write_debug,
        status=RUN_STATUS_RUNNING,
        clear_finished_at=True,
        emit_stage_event=False,
    )
    if not persist_error:
        persist_error = await persist_checkpoint_async(
            run_id=int(run_id),
            status=RUN_STATUS_RUNNING,
            clear_finished_at=True,
            agent_plan={
                "titles": list(plan_titles or []),
                "items": list(plan_items or []),
                "allows": [list(value or []) for value in (plan_allows or [])],
                "artifacts": [str(value or "") for value in (plan_artifacts or [])],
            },
            agent_state=state_obj,
            task_id=int(task_id),
            safe_write_debug=safe_write_debug,
            where="resume.running.persist_plan",
        )

    try:
        if persist_error:
            raise RuntimeError(str(persist_error))
        await _run_sync_with_timeout(
            update_task,
            task_id=int(task_id),
            status=STATUS_RUNNING,
            updated_at=updated_at,
        )
    except Exception as exc:
        logger.exception("agent.resume_state.persist_failed: %s", exc)
        safe_write_debug(
            task_id,
            run_id,
            message="agent.resume_state.persist_failed",
            data={"error": str(exc)},
            level="error",
        )

    return int(resume_step_order_value), state_obj


async def finalize_skip_execution_resume(
    *,
    task_id: int,
    run_id: int,
    workdir: str,
    plan_titles: List[str],
    plan_items: List[dict],
    plan_allows: List[List[str]],
    plan_artifacts: List[str],
    state_obj: dict,
    task_row,
    safe_write_debug,
) -> Tuple[str, List[str]]:
    """
    当计划已全部完成时，执行 resume 收尾：
    - 计划栏置为 done；
    - 持久化最新 checkpoint；
    - artifact 校验 + run/task 状态落库 + postprocess；
    - 自动写入结果记忆（若成功）。
    """
    events: List[str] = []
    run_status = RUN_STATUS_DONE
    plan_total_steps = len(plan_titles or [])

    if plan_items:
        for item in plan_items:
            if not isinstance(item, dict):
                continue
            if item.get("status") not in {"done", "skipped"}:
                item["status"] = "done"
        events.append(
            sse_json(
                {
                    "type": "plan",
                    "task_id": int(task_id),
                    "run_id": int(run_id),
                    "items": plan_items,
                }
            )
        )
    events.append(sse_json({"delta": f"{STREAM_TAG_EXEC} 计划已全部完成，开始收尾…\n"}))

    state_obj["step_order"] = int(plan_total_steps)
    state_obj["paused"] = None
    persist_error = await persist_checkpoint_async(
        run_id=int(run_id),
        agent_plan={
            "titles": list(plan_titles or []),
            "items": list(plan_items or []),
            "allows": [list(allow or []) for allow in (plan_allows or [])],
            "artifacts": [str(value or "") for value in (plan_artifacts or [])],
        },
        agent_state=state_obj,
        task_id=int(task_id),
        safe_write_debug=safe_write_debug,
        where="resume.skip_execution.persist",
    )
    if persist_error:
        safe_write_debug(
            task_id,
            run_id,
            message="agent.resume.skip_execution.persist_failed",
            data={"error": str(persist_error)},
            level="warning",
        )

    if run_status == RUN_STATUS_DONE and plan_artifacts:
        missing = check_missing_artifacts(artifacts=plan_artifacts, workdir=workdir)
        if missing:
            run_status = RUN_STATUS_FAILED
            safe_write_debug(
                task_id,
                run_id,
                message="agent.artifacts.missing",
                data={"missing": missing},
                level="error",
            )
            events.append(sse_json({"delta": f"{STREAM_TAG_FAIL} 未生成文件：{', '.join(missing)}\n"}))

    await _run_sync_with_timeout(
        finalize_run_and_task_status,
        task_id=int(task_id),
        run_id=int(run_id),
        run_status=str(run_status),
    )

    if run_status == RUN_STATUS_DONE and task_row and task_id and run_id:
        try:
            from backend.src.services.tasks.task_postprocess import write_task_result_memory_if_missing

            title = str(task_row["title"] or "").strip()
            item = await _run_sync_with_timeout(
                write_task_result_memory_if_missing,
                task_id=int(task_id),
                run_id=int(run_id),
                title=title,
            )
            if isinstance(item, dict) and item.get("id") is not None:
                events.append(
                    sse_json(
                        {
                            "type": SSE_TYPE_MEMORY_ITEM,
                            "task_id": int(task_id),
                            "run_id": int(run_id),
                            "item": item,
                        }
                    )
                )
        except Exception as exc:
            safe_write_debug(
                task_id,
                run_id,
                message="agent.memory.auto_task_result_failed",
                data={"error": str(exc)},
                level="warning",
            )

    if run_status in {RUN_STATUS_DONE, RUN_STATUS_FAILED, RUN_STATUS_STOPPED} and task_id and run_id:
        enqueue_postprocess_thread(task_id=int(task_id), run_id=int(run_id), run_status=str(run_status))

    return str(run_status), events
