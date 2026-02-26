from __future__ import annotations

import asyncio
from typing import Callable, List, Optional

from backend.src.agent.runner.debug_utils import safe_write_debug
from backend.src.agent.runner.failed_output_helpers import (
    build_failed_task_output_content as build_failed_task_output_content_shared,
    extract_step_error_text as extract_step_error_text_shared,
    read_row_value as read_row_value_shared,
    safe_collect_failed_step_lines,
    safe_collect_failure_debug_lines,
    safe_has_text_output,
    truncate_inline_text as truncate_inline_text_shared,
)
from backend.src.agent.runner.failed_output_injector import ensure_failed_task_output_shared
from backend.src.agent.runner.plan_events import sse_plan
from backend.src.agent.runner.stream_status_event import build_run_status_sse
from backend.src.constants import (
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_STOPPED,
    RUN_STATUS_WAITING,
    STREAM_TAG_EXEC,
    SSE_TYPE_MEMORY_ITEM,
    TASK_OUTPUT_TYPE_DEBUG,
    TASK_OUTPUT_TYPE_TEXT,
)
from backend.src.services.llm.llm_client import sse_json
from backend.src.services.tasks.task_queries import (
    create_task_output,
    list_task_outputs_for_run,
    list_task_steps_for_run,
)
from backend.src.services.tasks.task_run_lifecycle import (
    check_missing_artifacts,
    enqueue_postprocess_thread,
    enqueue_review_on_feedback_waiting,
    enqueue_stop_task_run_records,
    finalize_run_and_task_status,
    mark_run_failed,
)


async def check_and_report_missing_artifacts(
    run_status: str,
    plan_artifacts: List[str],
    workdir: str,
    yield_func: Callable,
    task_id: Optional[int] = None,
    run_id: Optional[int] = None,
) -> str:
    """
    检查 artifacts 是否缺失，返回更新后的状态。

    仅在任务"成功结束"时检查，避免出现"嘴上完成但没有落盘"。
    """
    if run_status != RUN_STATUS_DONE or not plan_artifacts:
        return run_status

    missing = check_missing_artifacts(artifacts=plan_artifacts, workdir=workdir)
    if missing:
        safe_write_debug(
            task_id, run_id,
            message="agent.artifacts.missing",
            data={"missing": missing},
            level="warning",
        )
        yield_func(sse_json({"delta": f"{STREAM_TAG_EXEC} 警告：未生成文件：{', '.join(missing)}（结果可能需要补救）\n"}))
        return run_status

    return run_status


def _truncate_inline_text(value: object, max_chars: int = 180) -> str:
    return truncate_inline_text_shared(value, max_chars=max_chars)


def _read_row_value(step_row: object, key: str):
    return read_row_value_shared(step_row, key)


def _extract_step_error_text(step_row: object) -> str:
    return extract_step_error_text_shared(step_row, read_value=_read_row_value)


def _build_failed_step_lines(task_id: int, run_id: int, max_items: int = 6) -> List[str]:
    return safe_collect_failed_step_lines(
        task_id=int(task_id),
        run_id=int(run_id),
        list_steps_for_run=list_task_steps_for_run,
        read_value=_read_row_value,
        handled_errors=(Exception,),
        max_items=max_items,
    )


def _build_failure_debug_lines(task_id: int, run_id: int, max_items: int = 3) -> List[str]:
    return safe_collect_failure_debug_lines(
        task_id=int(task_id),
        run_id=int(run_id),
        list_outputs_for_run=list_task_outputs_for_run,
        debug_output_type=str(TASK_OUTPUT_TYPE_DEBUG),
        read_value=_read_row_value,
        handled_errors=(Exception,),
        max_items=max_items,
        limit=30,
    )


def _has_text_task_output(task_id: int, run_id: int) -> bool:
    return safe_has_text_output(
        task_id=int(task_id),
        run_id=int(run_id),
        list_outputs_for_run=list_task_outputs_for_run,
        text_output_type=str(TASK_OUTPUT_TYPE_TEXT),
        read_value=_read_row_value,
        handled_errors=(Exception,),
        limit=20,
    )


def _build_failed_task_output_content(task_id: int, run_id: int) -> str:
    failed_steps = _build_failed_step_lines(task_id=int(task_id), run_id=int(run_id))
    debug_lines = _build_failure_debug_lines(task_id=int(task_id), run_id=int(run_id))
    return build_failed_task_output_content_shared(
        task_id=int(task_id),
        run_id=int(run_id),
        failed_steps=failed_steps,
        debug_lines=debug_lines,
    )


async def ensure_failed_task_output(
    task_id: int,
    run_id: int,
    run_status: str,
    yield_func: Callable,
) -> None:
    """
    failed 终态兜底：若本次 run 尚无文本结果，则自动写入结构化失败总结。
    """
    await ensure_failed_task_output_shared(
        task_id=int(task_id),
        run_id=int(run_id),
        run_status=str(run_status),
        run_status_failed=str(RUN_STATUS_FAILED),
        yield_func=yield_func,
        has_text_task_output_func=_has_text_task_output,
        build_failed_task_output_content_func=_build_failed_task_output_content,
        create_task_output_func=create_task_output,
        task_output_type_text=str(TASK_OUTPUT_TYPE_TEXT),
        safe_write_debug_func=safe_write_debug,
        to_thread_func=asyncio.to_thread,
        sse_json_func=sse_json,
        handled_errors=(Exception,),
    )


def finalize_plan_items_status(
    plan_items: List[dict],
    run_status: str,
    yield_func: Callable,
    task_id: int,
    run_id: int,
) -> None:
    """
    计划栏收尾：把 running 状态结算为 done/failed。

    只有在"真正结束(done/failed)"时才结算，waiting 应保留状态。
    """
    if not plan_items or run_status not in {RUN_STATUS_DONE, RUN_STATUS_FAILED}:
        return

    for item in plan_items:
        if item.get("status") == "running":
            item["status"] = "done" if run_status == RUN_STATUS_DONE else "failed"

    yield_func(sse_plan(task_id=int(task_id), run_id=int(run_id), plan_items=plan_items))


async def finalize_run_status(
    task_id: int,
    run_id: int,
    run_status: str,
) -> None:
    """
    落库 run/task 状态（waiting 不写 finished_at）。
    """
    await asyncio.to_thread(
        finalize_run_and_task_status,
        task_id=int(task_id),
        run_id=int(run_id),
        run_status=str(run_status),
    )


async def trigger_review_if_waiting(
    task_id: int,
    run_id: int,
    run_status: str,
    agent_state: dict,
) -> None:
    """
    评估触发点：当等待原因是"确认满意度"时触发评估。
    """
    if run_status == RUN_STATUS_WAITING:
        enqueue_review_on_feedback_waiting(
            task_id=int(task_id),
            run_id=int(run_id),
            agent_state=agent_state,
        )


async def write_auto_memory_if_done(
    task_id: int,
    run_id: int,
    run_status: str,
    message: str,
    yield_func: Callable,
) -> Optional[dict]:
    """
    自动记忆：把本次 run 的"最终结果摘要"写入 memory_items。

    通过 SSE 通知前端即时更新。
    """
    if run_status != RUN_STATUS_DONE:
        return None

    try:
        from backend.src.services.tasks.task_postprocess import write_task_result_memory_if_missing

        item = await asyncio.to_thread(
            write_task_result_memory_if_missing,
            task_id=int(task_id),
            run_id=int(run_id),
            title=str(message or "").strip(),
        )
        if isinstance(item, dict) and item.get("id") is not None:
            yield_func(sse_json({
                "type": SSE_TYPE_MEMORY_ITEM,
                "task_id": int(task_id),
                "run_id": int(run_id),
                "item": item,
            }))
            return item
    except Exception as exc:
        safe_write_debug(
            task_id, run_id,
            message="agent.memory.auto_task_result_failed",
            data={"error": str(exc)},
            level="warning",
        )

    return None


def enqueue_postprocess_if_terminal(
    task_id: int,
    run_id: int,
    run_status: str,
) -> None:
    """
    入队后处理线程（评估/技能/图谱）。

    仅在终态（done/failed/stopped）时触发。
    """
    if run_status in {RUN_STATUS_DONE, RUN_STATUS_FAILED, RUN_STATUS_STOPPED}:
        enqueue_postprocess_thread(
            task_id=int(task_id),
            run_id=int(run_id),
            run_status=str(run_status),
        )


async def run_finalization_sequence(
    task_id: int,
    run_id: int,
    run_status: str,
    agent_state: dict,
    plan_items: List[dict],
    plan_artifacts: List[str],
    message: str,
    workdir: str,
    yield_func: Callable,
) -> str:
    """
    统一后处理闭环序列。
    """
    run_status = await check_and_report_missing_artifacts(
        run_status, plan_artifacts, workdir, yield_func, task_id, run_id
    )

    finalize_plan_items_status(plan_items, run_status, yield_func, task_id, run_id)

    await ensure_failed_task_output(task_id, run_id, run_status, yield_func)

    await finalize_run_status(task_id, run_id, run_status)
    yield_func(
        build_run_status_sse(
            status=run_status,
            task_id=int(task_id),
            run_id=int(run_id),
        )
    )

    await trigger_review_if_waiting(task_id, run_id, run_status, agent_state)

    await write_auto_memory_if_done(task_id, run_id, run_status, message, yield_func)

    enqueue_postprocess_if_terminal(task_id, run_id, run_status)

    return run_status


def handle_stream_cancellation(
    task_id: Optional[int],
    run_id: Optional[int],
    reason: str = "agent_stream_cancelled",
) -> None:
    """
    处理 SSE 流取消（客户端断开）。
    """
    if run_id is not None:
        enqueue_stop_task_run_records(
            task_id=task_id,
            run_id=int(run_id),
            reason=reason,
        )


async def handle_execution_exception(
    exc: Exception,
    task_id: Optional[int],
    run_id: Optional[int],
    yield_func: Callable,
    mode_prefix: str = "agent",
) -> None:
    """
    处理执行异常。
    """
    if task_id is not None and run_id is not None:
        await asyncio.to_thread(
            mark_run_failed,
            task_id=int(task_id),
            run_id=int(run_id),
            reason=f"exception:{exc}",
        )
        await ensure_failed_task_output(int(task_id), int(run_id), RUN_STATUS_FAILED, yield_func)
        enqueue_postprocess_thread(
            task_id=int(task_id),
            run_id=int(run_id),
            run_status=RUN_STATUS_FAILED,
        )
        safe_write_debug(
            task_id, run_id,
            message=f"{mode_prefix}.exception",
            data={"error": f"{exc}"},
            level="error",
        )

    suffix = f"（task_id={task_id} run_id={run_id}）" if task_id else ""
    try:
        yield_func(sse_json({"message": f"{mode_prefix} 执行失败:{exc}{suffix}"}, event="error"))
    except BaseException:
        pass


def yield_done_event(yield_func: Callable) -> None:
    """
    发送 done 事件，若客户端已断开则忽略。
    """
    try:
        yield_func(sse_json({"type": "done"}, event="done"))
    except BaseException:
        pass

