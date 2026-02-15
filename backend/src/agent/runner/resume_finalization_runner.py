from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Callable, Dict, List

from backend.src.constants import (
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_STOPPED,
    RUN_STATUS_WAITING,
    STREAM_TAG_FAIL,
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
    check_missing_artifacts: Callable[..., List[str]]
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

    if run_status == RUN_STATUS_DONE and plan_artifacts:
        missing = list(config.check_missing_artifacts(artifacts=plan_artifacts, workdir=workdir) or [])
        if missing:
            run_status = RUN_STATUS_FAILED
            config.safe_write_debug(
                task_id,
                run_id,
                message="agent.artifacts.missing",
                data={"missing": missing},
                level="error",
            )
            yield ("msg", sse_json({"delta": f"{STREAM_TAG_FAIL} 未生成文件：{', '.join(missing)}\n"}))

    if plan_items and run_status in {RUN_STATUS_DONE, RUN_STATUS_FAILED}:
        for item in plan_items:
            if isinstance(item, dict) and item.get("status") == "running":
                item["status"] = "done" if run_status == RUN_STATUS_DONE else "failed"
        yield ("msg", sse_json({"type": "plan", "task_id": task_id, "run_id": run_id, "items": plan_items}))

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
