# -*- coding: utf-8 -*-
"""
Agent 执行管道公共逻辑。

提取 stream_new_run.py 和 stream_think_run.py 的重复代码，
提供统一的：
- 调试输出
- 知识检索
- 状态持久化
- 后处理闭环
- 异常处理
"""

import asyncio
import json
import logging
import sqlite3
from typing import Any, AsyncGenerator, Callable, List, Optional

from backend.src.constants import RUN_STATUS_FAILED, TASK_OUTPUT_TYPE_DEBUG, TASK_OUTPUT_TYPE_TEXT
from backend.src.services.llm.llm_client import sse_json
from backend.src.agent.runner.debug_utils import safe_write_debug
from backend.src.agent.runner.pending_planning_flow import (
    enter_pending_planning_waiting,
    resume_pending_planning_after_user_input,
)
from backend.src.agent.runner.finalization_pipeline import (
    check_and_report_missing_artifacts,
    enqueue_postprocess_if_terminal,
    finalize_plan_items_status,
    finalize_run_status,
    handle_stream_cancellation,
    run_finalization_sequence,
    trigger_review_if_waiting,
    write_auto_memory_if_done,
    yield_done_event,
)
from backend.src.agent.core.checkpoint_store import persist_checkpoint_async
from backend.src.agent.core.run_context import AgentRunContext
from backend.src.services.tasks.task_run_lifecycle import (
    enqueue_postprocess_thread,
    mark_run_failed,
)
from backend.src.repositories.task_outputs_repo import create_task_output, list_task_outputs_for_run
from backend.src.repositories.task_steps_repo import list_task_steps_for_run
logger = logging.getLogger(__name__)


NON_FATAL_STORAGE_ERRORS = (sqlite3.Error, RuntimeError, TypeError, ValueError, OSError)


async def pump_async_task_messages(
    task: "asyncio.Task[Any]",
    out_q: "asyncio.Queue[str]",
) -> AsyncGenerator[str, None]:
    """
    将“内部异步任务通过 out_q 输出的 SSE 字符串”转发为可 yield 的流。

    典型用法：
    - 内部函数签名：fn(..., yield_func=emit)
    - emit(msg) 只负责把 msg 写入 out_q（同步、非阻塞）
    - 外层 async generator 用本函数把 out_q 的消息逐条 yield 给客户端

    说明：
    - 避免使用 sleep/poll 的忙等方式；
    - 当 task 已完成且 out_q 为空时结束；
    - 不负责处理 task 的异常：由调用方 await task 时抛出。
    """
    while True:
        if task.done() and out_q.empty():
            break

        get_msg_task: "asyncio.Task[str]" = asyncio.create_task(out_q.get())
        done, pending = await asyncio.wait(
            {task, get_msg_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if get_msg_task in done:
            try:
                msg = get_msg_task.result()
            except (RuntimeError, asyncio.CancelledError):
                msg = ""
            if msg:
                yield str(msg)
        else:
            get_msg_task.cancel()

        # task 完成后继续循环 drain 队列（直到 empty）


# ==============================================================================
# 兼容导出（知识检索 / planning enrich）
# ==============================================================================

from backend.src.agent.runner.knowledge_retrieval_pipeline import (
    retrieve_all_knowledge,
    retrieve_domains,
    retrieve_graph_nodes,
    retrieve_memories,
    retrieve_skills,
    retrieve_solutions,
)
from backend.src.agent.runner.planning_enrich_pipeline import (
    maybe_draft_solution_for_planning,
    prepare_planning_knowledge_do,
    prepare_planning_knowledge_think,
)


# ==============================================================================
# 状态持久化
# ==============================================================================

async def persist_agent_state(
    run_id: int,
    agent_plan: dict,
    agent_state: dict,
) -> None:
    """
    持久化 Agent 运行态到数据库。

    Args:
        run_id: 执行尝试 ID
        agent_plan: 计划数据
        agent_state: 状态数据
    """
    error = await persist_checkpoint_async(
        run_id=int(run_id),
        agent_plan=dict(agent_plan or {}),
        agent_state=dict(agent_state or {}),
        where="persist_agent_state",
    )
    if error:
        raise RuntimeError(str(error))


def build_base_agent_state(
    message: str,
    model: str,
    parameters: dict,
    max_steps: int,
    workdir: str,
    tools_hint: str,
    skills_hint: str,
    memories_hint: str,
    graph_hint: str,
) -> dict:
    """
    构建基础 Agent 状态。

    子类可扩展此状态添加模式特定字段。
    """
    run_ctx = AgentRunContext.from_agent_state(
        {},
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
    return run_ctx.to_agent_state()


# ==============================================================================
# 兼容导出（失败兜底 / 异常收敛）
# ==============================================================================

def _truncate_inline_text(value: object, max_chars: int = 180) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    limit = max(1, int(max_chars))
    if len(raw) <= limit:
        return raw
    return f"{raw[: max(0, limit - 1)]}…"


def _read_row_value(step_row: object, key: str):
    if isinstance(step_row, dict):
        return step_row.get(key)
    try:
        return step_row[key]
    except (TypeError, IndexError, KeyError):
        return None


def _extract_step_error_text(step_row: object) -> str:
    direct = _truncate_inline_text(_read_row_value(step_row, "error"), 180)
    if direct:
        return direct

    result_raw = str(_read_row_value(step_row, "result") or "").strip()
    if not result_raw:
        return ""

    try:
        parsed = json.loads(result_raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return _truncate_inline_text(result_raw, 180)

    if isinstance(parsed, dict):
        for key in ("error", "stderr", "message"):
            picked = _truncate_inline_text(parsed.get(key), 180)
            if picked:
                return picked

    return _truncate_inline_text(result_raw, 180)


def _build_failed_step_lines(task_id: int, run_id: int, max_items: int = 6) -> List[str]:
    try:
        rows = list_task_steps_for_run(task_id=int(task_id), run_id=int(run_id))
    except NON_FATAL_STORAGE_ERRORS:
        return []

    lines: List[str] = []
    for row in rows or []:
        status = str(_read_row_value(row, "status") or "").strip().lower()
        if status != "failed":
            continue

        step_order = _read_row_value(row, "step_order")
        order_text = str(step_order) if step_order is not None else "?"
        title = str(_read_row_value(row, "title") or "").strip()
        error_text = _extract_step_error_text(row) or "未记录错误详情"

        lines.append(f"- step#{order_text} {title or '(未命名步骤)'} -> {error_text}")
        if len(lines) >= int(max_items):
            break
    return lines


def _build_failure_debug_lines(task_id: int, run_id: int, max_items: int = 3) -> List[str]:
    try:
        rows = list_task_outputs_for_run(
            task_id=int(task_id),
            run_id=int(run_id),
            order="DESC",
            limit=30,
        )
    except NON_FATAL_STORAGE_ERRORS:
        return []

    matched: List[str] = []
    for row in rows or []:
        output_type = str(_read_row_value(row, "output_type") or "").strip().lower()
        if output_type != str(TASK_OUTPUT_TYPE_DEBUG):
            continue
        content = _truncate_inline_text(_read_row_value(row, "content"), 180)
        if not content:
            continue
        lowered = content.lower()
        if not any(
            key in lowered
            for key in ("failed", "error", "exception", "missing", "warning", "失败", "错误", "异常", "缺失")
        ):
            continue
        matched.append(f"- {content}")
        if len(matched) >= int(max_items):
            break
    return list(reversed(matched))


def _has_text_task_output(task_id: int, run_id: int) -> bool:
    try:
        rows = list_task_outputs_for_run(
            task_id=int(task_id),
            run_id=int(run_id),
            order="DESC",
            limit=20,
        )
    except NON_FATAL_STORAGE_ERRORS:
        return False

    for row in rows or []:
        output_type = str(_read_row_value(row, "output_type") or "").strip().lower()
        if output_type != str(TASK_OUTPUT_TYPE_TEXT):
            continue
        content = str(_read_row_value(row, "content") or "").strip()
        if content:
            return True
    return False


def _build_failed_task_output_content(task_id: int, run_id: int) -> str:
    failed_steps = _build_failed_step_lines(task_id=int(task_id), run_id=int(run_id))
    debug_lines = _build_failure_debug_lines(task_id=int(task_id), run_id=int(run_id))
    if not failed_steps:
        failed_steps = ["- 无（可能在规划阶段或执行初始化阶段失败）"]
    if not debug_lines:
        debug_lines = ["- 无（未捕获到额外 debug 证据）"]
    lines = [
        "【失败总结】",
        f"- task_id: {int(task_id)}",
        f"- run_id: {int(run_id)}",
        "- 结论：本次执行未完成，状态为 failed。",
        "",
        "[失败步骤]",
        *failed_steps,
        "",
        "[关键证据]",
        *debug_lines,
        "",
        "[建议下一步]",
        "- 优先修复首个失败步骤，再重试执行。",
        "- 若失败来自外部依赖，请补齐输入并增加校验步骤。",
    ]
    return "\n".join(lines).strip()


async def ensure_failed_task_output(
    task_id: int,
    run_id: int,
    run_status: str,
    yield_func: Callable,
) -> None:
    """
    failed 终态兜底：若本次 run 尚无文本结果，则自动写入结构化失败总结。

    说明：保留在本模块，便于单测通过 patch `execution_pipeline.*` 注入行为。
    """
    if str(run_status) != str(RUN_STATUS_FAILED):
        return

    if _has_text_task_output(task_id=int(task_id), run_id=int(run_id)):
        return

    content = _build_failed_task_output_content(task_id=int(task_id), run_id=int(run_id))

    try:
        await asyncio.to_thread(
            create_task_output,
            task_id=int(task_id),
            run_id=int(run_id),
            output_type=str(TASK_OUTPUT_TYPE_TEXT),
            content=content,
        )
        safe_write_debug(
            task_id,
            run_id,
            message="agent.failed_output.injected",
            data={"bytes": len(content)},
            level="info",
        )
        try:
            yield_func(sse_json({"delta": "【失败总结】已写入结构化失败报告。\n"}))
        except BaseException:
            pass
    except NON_FATAL_STORAGE_ERRORS as exc:
        safe_write_debug(
            task_id,
            run_id,
            message="agent.failed_output.inject_failed",
            data={"error": str(exc)},
            level="warning",
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

    说明：保留在本模块，便于单测通过 patch `execution_pipeline.*` 注入行为。
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


# ==============================================================================
# SSE 响应构建
# ==============================================================================

def create_sse_response(gen, headers: Optional[dict] = None):
    """
    创建 SSE StreamingResponse。
    """
    from fastapi.responses import StreamingResponse

    default_headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    }
    if headers:
        default_headers.update(headers)

    return StreamingResponse(gen(), media_type="text/event-stream", headers=default_headers)
