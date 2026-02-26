from __future__ import annotations

from typing import Optional

from backend.src.agent.contracts.stream_events import coerce_session_key, generate_session_key


def resolve_or_create_session_key(
    *,
    agent_state: Optional[dict],
    task_id: int,
    run_id: int,
    created_at: str = "",
) -> str:
    state = agent_state if isinstance(agent_state, dict) else {}
    existing = coerce_session_key(state.get("session_key"))
    if existing:
        return existing
    return generate_session_key(task_id=int(task_id), run_id=int(run_id), created_at=str(created_at or ""))


def apply_session_key_to_state(agent_state: Optional[dict], session_key: str) -> dict:
    state = dict(agent_state or {})
    normalized = coerce_session_key(session_key)
    if normalized:
        state["session_key"] = normalized
    return state

