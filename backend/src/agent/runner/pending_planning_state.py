from __future__ import annotations

from typing import Dict, List, Optional

from backend.src.agent.core.run_context import AgentRunContext


def _normalize_entity_ids(items: List[dict]) -> List[int]:
    result: List[int] = []
    for value in items or []:
        if not isinstance(value, dict):
            continue
        raw_id = value.get("id")
        try:
            entity_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        if entity_id > 0:
            result.append(int(entity_id))
    return result


def _set_pending_flags(run_ctx: AgentRunContext, *, pending: bool, reason: str = "") -> None:
    if pending:
        run_ctx.set_extra("pending_planning", True)
        run_ctx.set_extra("pending_planning_reason", str(reason or "knowledge_sufficiency"))
        return
    run_ctx.extras.pop("pending_planning", None)
    run_ctx.extras.pop("pending_planning_reason", None)


def build_initial_pending_state(
    *,
    message: str,
    model: str,
    parameters: dict,
    max_steps: int,
    workdir: str,
    mode: str,
    tools_hint: str,
    skills_hint: str,
    solutions_hint: str,
    memories_hint: str,
    graph_hint: str,
    domain_ids: List[str],
    skills: List[dict],
    solutions: List[dict],
    draft_solution_id: Optional[int],
    think_config: Optional[dict],
) -> dict:
    run_ctx = AgentRunContext.from_agent_state(
        {},
        mode=str(mode or "").strip().lower() or "do",
        message=str(message or ""),
        model=str(model or ""),
        parameters=dict(parameters or {}),
        max_steps=int(max_steps),
        workdir=str(workdir or ""),
        tools_hint=str(tools_hint or ""),
        skills_hint=str(skills_hint or ""),
        solutions_hint=str(solutions_hint or "(无)"),
        memories_hint=str(memories_hint or "(无)"),
        graph_hint=str(graph_hint or ""),
    )
    run_ctx.set_extra("domain_ids", list(domain_ids or []))
    run_ctx.set_extra("skill_ids", _normalize_entity_ids(list(skills or [])))
    run_ctx.set_extra("solution_ids", _normalize_entity_ids(list(solutions or [])))
    if isinstance(draft_solution_id, int) and int(draft_solution_id) > 0:
        run_ctx.set_extra("draft_solution_id", int(draft_solution_id))
    if run_ctx.mode == "think" and isinstance(think_config, dict) and think_config:
        run_ctx.set_extra("think_config", dict(think_config))
    _set_pending_flags(run_ctx, pending=True, reason="knowledge_sufficiency")
    return run_ctx.to_agent_state()


def build_waiting_followup_state(
    *,
    agent_state: dict,
    mode: str,
    message: str,
    tools_hint: str,
    skills_hint: str,
    solutions_hint: str,
    graph_hint: str,
    domain_ids: List[str],
    step_order: int,
) -> dict:
    run_ctx = AgentRunContext.from_agent_state(agent_state or {})
    run_ctx.mode = str(mode or "").strip().lower() or "do"
    run_ctx.message = str(message or "").strip()
    run_ctx.step_order = int(step_order or 1)
    run_ctx.set_hints(
        tools_hint=str(tools_hint or "(无)"),
        skills_hint=str(skills_hint or "(无)"),
        solutions_hint=str(solutions_hint or "(无)"),
        graph_hint=str(graph_hint or ""),
    )
    run_ctx.set_extra("domain_ids", list(domain_ids or []))
    _set_pending_flags(run_ctx, pending=True, reason="knowledge_sufficiency")
    return run_ctx.to_agent_state()


def build_planned_state_after_pending(
    *,
    agent_state: dict,
    mode: str,
    message: str,
    tools_hint: str,
    skills_hint: str,
    solutions_hint: str,
    graph_hint: str,
    domain_ids: List[str],
    skills: List[dict],
    solutions: List[dict],
    draft_solution_id: Optional[int],
    step_order: int,
    extra_state: Optional[Dict] = None,
) -> dict:
    run_ctx = AgentRunContext.from_agent_state(agent_state or {})
    run_ctx.mode = str(mode or "").strip().lower() or "do"
    run_ctx.message = str(message or "").strip()
    run_ctx.step_order = int(step_order or 1)
    run_ctx.set_hints(
        tools_hint=str(tools_hint or "(无)"),
        skills_hint=str(skills_hint or "(无)"),
        solutions_hint=str(solutions_hint or "(无)"),
        graph_hint=str(graph_hint or ""),
    )
    run_ctx.set_extra("domain_ids", list(domain_ids or []))
    run_ctx.set_extra("skill_ids", _normalize_entity_ids(list(skills or [])))
    run_ctx.set_extra("solution_ids", _normalize_entity_ids(list(solutions or [])))
    if isinstance(draft_solution_id, int) and int(draft_solution_id) > 0:
        run_ctx.set_extra("draft_solution_id", int(draft_solution_id))
    _set_pending_flags(run_ctx, pending=False)
    if isinstance(extra_state, dict):
        for key, value in extra_state.items():
            run_ctx.set_extra(str(key), value)
    return run_ctx.to_agent_state()
