from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Callable, Dict, List

from backend.src.agent.runner.finalization_pipeline import (
    check_and_report_missing_artifacts,
    enforce_done_visible_output_contract,
)
from backend.src.agent.runner.plan_events import sse_plan
from backend.src.constants import (
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_STOPPED,
    RUN_STATUS_WAITING,
    SSE_TYPE_MEMORY_ITEM,
)
from backend.src.services.llm.llm_client import sse_json


@dataclass
class ResumeFinalizationConfig:
    task_id: int
    run_id: int
    run_status: str
    workdir: str
    message_title: str
    agent_state: dict
    plan_items: List[dict]
    plan_artifacts: List[str]
    safe_write_debug: Callable[..., None]
    finalize_run_and_task_status: Callable[..., None]
    enqueue_review_on_feedback_waiting: Callable[..., None]
    enqueue_postprocess_thread: Callable[..., None]


async def iter_resume_finalization_events(
    *,
    config: ResumeFinalizationConfig,
) -> AsyncGenerator[tuple[str, Any], None]:
    """
    resume 链路的终态收敛：
    - artifacts 校验（缺失时降级为 failed）
    - plan 运行态收尾
    - run/task 状态落库
    - waiting 触发评估、done 触发自动记忆、终态触发后处理
    """
    run_status = str(config.run_status or "")
    task_id = int(config.task_id)
    run_id = int(config.run_id)
    workdir = str(config.workdir or "")
    plan_items = list(config.plan_items or [])
    plan_artifacts = list(config.plan_artifacts or [])
    agent_state = dict(config.agent_state or {})

    buffered_chunks: List[str] = []

    def _emit(chunk: str) -> None:
        text = str(chunk or "")
        if text:
            buffered_chunks.append(text)

    run_status = await check_and_report_missing_artifacts(
        run_status=str(run_status),
        plan_artifacts=list(plan_artifacts or []),
        workdir=str(workdir or ""),
        yield_func=_emit,
        task_id=int(task_id),
        run_id=int(run_id),
    )
    run_status = await enforce_done_visible_output_contract(
        run_status=str(run_status),
        task_id=int(task_id),
        run_id=int(run_id),
        yield_func=_emit,
    )
    for chunk in buffered_chunks:
        yield ("msg", chunk)

    if plan_items and run_status in {RUN_STATUS_DONE, RUN_STATUS_FAILED}:
        for item in plan_items:
            if isinstance(item, dict) and item.get("status") == "running":
                item["status"] = "done" if run_status == RUN_STATUS_DONE else "failed"
        yield ("msg", sse_plan(task_id=task_id, run_id=run_id, plan_items=plan_items))

    await asyncio.to_thread(
        config.finalize_run_and_task_status,
        task_id=int(task_id),
        run_id=int(run_id),
        run_status=str(run_status),
    )

    if run_status == RUN_STATUS_WAITING:
        config.enqueue_review_on_feedback_waiting(
            task_id=int(task_id),
            run_id=int(run_id),
            agent_state=agent_state,
        )

    if run_status == RUN_STATUS_DONE:
        try:
            from backend.src.services.tasks.task_postprocess import write_task_result_memory_if_missing

            item = await asyncio.to_thread(
                write_task_result_memory_if_missing,
                task_id=int(task_id),
                run_id=int(run_id),
                title=str(config.message_title or "").strip(),
            )
            if isinstance(item, dict) and item.get("id") is not None:
                yield (
                    "msg",
                    sse_json(
                        {
                            "type": SSE_TYPE_MEMORY_ITEM,
                            "task_id": int(task_id),
                            "run_id": int(run_id),
                            "item": item,
                        }
                    ),
                )
        except Exception as exc:
            config.safe_write_debug(
                task_id,
                run_id,
                message="agent.memory.auto_task_result_failed",
                data={"error": str(exc)},
                level="warning",
            )

    if run_status in {RUN_STATUS_DONE, RUN_STATUS_FAILED, RUN_STATUS_STOPPED}:
        config.enqueue_postprocess_thread(
            task_id=int(task_id),
            run_id=int(run_id),
            run_status=str(run_status),
        )

    yield ("status", str(run_status))
