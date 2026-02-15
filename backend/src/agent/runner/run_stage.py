from __future__ import annotations

from typing import Optional, Tuple

from backend.src.agent.core.checkpoint_store import persist_checkpoint_async
from backend.src.agent.core.run_context import AgentRunContext
from backend.src.common.utils import now_iso
from backend.src.services.llm.llm_client import sse_json


async def persist_run_stage(
    *,
    run_ctx: AgentRunContext,
    task_id: Optional[int],
    run_id: int,
    stage: str,
    where: str,
    safe_write_debug=None,
    status: Optional[str] = None,
    clear_finished_at: bool = False,
    emit_stage_event: bool = True,
) -> Tuple[dict, Optional[str], Optional[str]]:
    """
    统一阶段持久化：
    - 写入 agent_state.stage/stage_at；
    - 持久化 task_runs.agent_state；
    - 可选返回前端 stage 事件（SSE）。
    """
    run_ctx.set_stage(str(stage or "").strip(), now_iso())
    agent_state = run_ctx.to_agent_state()
    persist_error = await persist_checkpoint_async(
        run_id=int(run_id),
        status=str(status).strip() if isinstance(status, str) and str(status).strip() else None,
        clear_finished_at=bool(clear_finished_at),
        agent_state=agent_state,
        task_id=task_id,
        safe_write_debug=safe_write_debug,
        where=str(where or "run_stage.persist"),
    )
    stage_event: Optional[str] = None
    if not persist_error and emit_stage_event:
        stage_event = sse_json({"type": "agent_stage", "task_id": task_id, "run_id": run_id, "stage": stage})
    return agent_state, persist_error, stage_event
