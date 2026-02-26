from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Dict, Optional

from backend.src.agent.core.run_context import AgentRunContext
from backend.src.agent.contracts.stream_events import STREAM_EVENT_TYPE_RUN_CREATED
from backend.src.agent.runner.run_config_snapshot import apply_run_config_snapshot_if_missing
from backend.src.agent.runner.run_stage import persist_run_stage
from backend.src.agent.runner.session_runtime import apply_session_key_to_state, resolve_or_create_session_key
from backend.src.common.utils import is_test_env, now_iso, parse_optional_int
from backend.src.services.llm.llm_client import sse_json
from backend.src.services.tasks.task_run_lifecycle import create_task_and_run_records_for_agent


@dataclass
class BootstrappedRun:
    task_id: int
    run_id: int
    run_ctx: AgentRunContext
    run_created_event: str
    stage_event: Optional[str]


async def bootstrap_new_mode_run(
    *,
    message: str,
    mode: str,
    model: str,
    parameters: dict,
    max_steps: Optional[int],
    workdir: str,
    stage_where_prefix: str,
    safe_write_debug=None,
    state_overrides: Optional[Dict] = None,
    tools_hint: Optional[str] = None,
    skills_hint: Optional[str] = None,
    solutions_hint: Optional[str] = None,
    memories_hint: Optional[str] = None,
    graph_hint: Optional[str] = None,
) -> BootstrappedRun:
    """
    新建 run 的公共启动流程：
    - 创建 task/run 记录；
    - 生成 run_created SSE；
    - 初始化 AgentRunContext；
    - 落 retrieval 阶段状态。
    """
    created_at = now_iso()
    if is_test_env():
        task_id, run_id = create_task_and_run_records_for_agent(
            message=message,
            created_at=created_at,
        )
    else:
        task_id, run_id = await asyncio.to_thread(
            create_task_and_run_records_for_agent,
            message=message,
            created_at=created_at,
        )

    run_ctx = AgentRunContext.from_agent_state(
        {},
        mode=str(mode or "").strip() or None,
        message=message,
        model=model,
        parameters=parameters,
        max_steps=parse_optional_int(max_steps, default=None),
        workdir=workdir,
        tools_hint=tools_hint,
        skills_hint=skills_hint,
        solutions_hint=solutions_hint,
        memories_hint=memories_hint,
        graph_hint=graph_hint,
    )
    run_ctx.merge_state_overrides(state_overrides)
    session_key = resolve_or_create_session_key(
        agent_state=run_ctx.to_agent_state(),
        task_id=int(task_id),
        run_id=int(run_id),
        created_at=created_at,
    )
    state_with_session = apply_session_key_to_state(run_ctx.to_agent_state(), session_key)
    state_with_snapshot = apply_run_config_snapshot_if_missing(
        agent_state=state_with_session,
        mode=mode,
        requested_model=model,
        parameters=parameters,
    )
    run_ctx.merge_state_overrides(state_with_snapshot)

    _, _, stage_event = await persist_run_stage(
        run_ctx=run_ctx,
        task_id=int(task_id),
        run_id=int(run_id),
        stage="retrieval",
        where=f"{str(stage_where_prefix or '').strip()}.stage.retrieval",
        safe_write_debug=safe_write_debug,
    )

    return BootstrappedRun(
        task_id=int(task_id),
        run_id=int(run_id),
        run_ctx=run_ctx,
        run_created_event=sse_json(
            {
                "type": STREAM_EVENT_TYPE_RUN_CREATED,
                "task_id": int(task_id),
                "run_id": int(run_id),
                "session_key": str(session_key),
            }
        ),
        stage_event=stage_event,
    )
