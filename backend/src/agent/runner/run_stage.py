from __future__ import annotations

from typing import Callable, Optional, Tuple

from backend.src.agent.core.checkpoint_store import persist_checkpoint_async
from backend.src.agent.core.run_context import AgentRunContext
from backend.src.common.utils import now_iso, parse_optional_int
from backend.src.services.llm.llm_client import sse_json


_STAGE_TRANSITION_ALLOWLIST = {
    "": {"retrieval", "planning", "execute"},
    "retrieval": {"retrieval", "planning", "execute"},
    "planning": {"planning", "planned", "execute"},
    "planned": {"planned", "planning", "execute"},
    "execute": {"execute", "planning", "planned", "waiting_input", "stopped", "failed", "done"},
    "waiting_input": {"waiting_input", "planning", "execute", "stopped", "failed", "done"},
    "stopped": {"stopped", "execute"},
    "failed": {"failed"},
    "done": {"done"},
}


def _normalize_run_stage(value: object) -> str:
    return str(value or "").strip().lower()


def is_legal_run_stage_transition(current_stage: object, next_stage: object) -> bool:
    current = _normalize_run_stage(current_stage)
    nxt = _normalize_run_stage(next_stage)
    if not nxt:
        return False
    if current == nxt:
        return True
    allowed = _STAGE_TRANSITION_ALLOWLIST.get(current)
    if isinstance(allowed, set):
        return nxt in allowed
    # 未登记 stage：宽松放行，避免自定义阶段被硬阻断。
    return True


async def persist_run_stage(
    *,
    run_ctx: AgentRunContext,
    task_id: Optional[int],
    run_id: int,
    stage: str,
    where: str,
    safe_write_debug: Optional[Callable] = None,
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
    requested_stage = _normalize_run_stage(stage)
    current_stage = _normalize_run_stage(run_ctx.to_agent_state().get("stage"))
    blocked_transition = (
        bool(requested_stage)
        and bool(current_stage)
        and not is_legal_run_stage_transition(current_stage, requested_stage)
    )

    next_stage = requested_stage
    if blocked_transition:
        # 非法转移不覆盖当前 stage，避免状态机回退导致恢复链路错判。
        next_stage = current_stage or requested_stage
        if callable(safe_write_debug):
            safe_write_debug(
                task_id=parse_optional_int(task_id, default=None),
                run_id=int(run_id),
                message="agent.stage.transition_blocked",
                data={
                    "from": str(current_stage),
                    "to": str(requested_stage),
                    "where": str(where or ""),
                },
                level="warning",
            )
        run_ctx.set_extra(
            "stage_barrier",
            {
                "blocked": True,
                "from": str(current_stage),
                "to": str(requested_stage),
                "at": now_iso(),
            },
        )

    run_ctx.set_stage(str(next_stage), now_iso())
    agent_state = run_ctx.to_agent_state()
    normalized_status = status.strip() if isinstance(status, str) else None
    persist_error = await persist_checkpoint_async(
        run_id=int(run_id),
        status=normalized_status or None,
        clear_finished_at=bool(clear_finished_at),
        agent_state=agent_state,
        task_id=task_id,
        safe_write_debug=safe_write_debug,
        where=str(where or "run_stage.persist"),
    )
    stage_event: Optional[str] = None
    if not persist_error and emit_stage_event and not blocked_transition:
        stage_event = sse_json({"type": "agent_stage", "task_id": task_id, "run_id": run_id, "stage": next_stage})
    return agent_state, persist_error, stage_event
