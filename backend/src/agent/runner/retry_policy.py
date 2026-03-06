# -*- coding: utf-8 -*-
"""重试变化策略。"""

from __future__ import annotations

from typing import Dict, Optional

from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.runner.attempt_controller import rotate_strategy
from backend.src.common.utils import coerce_int


def maybe_enforce_retry_change(
    *,
    agent_state: Dict,
    plan_struct: PlanStructure,
    step_order: int,
) -> Optional[dict]:
    if not isinstance(agent_state, dict):
        return None
    pending = agent_state.get("pending_retry_requirements")
    if not isinstance(pending, dict) or not bool(pending.get("active")):
        return None

    blocked_fp = str(pending.get("blocked_strategy_fingerprint") or "").strip()
    current_fp = str(agent_state.get("strategy_fingerprint") or "").strip()
    if current_fp and blocked_fp and current_fp != blocked_fp:
        pending["enforced_strategy_fingerprint"] = current_fp
        pending["active"] = True
        return None

    event = rotate_strategy(
        agent_state=agent_state,
        plan_struct=plan_struct,
        reason="retry_must_change_enforced",
        failure_class=str(pending.get("failure_class") or "").strip(),
    )
    pending["active"] = True
    pending["enforced_strategy_fingerprint"] = str(event.get("strategy_fingerprint") or "").strip()
    pending["enforced_at_step_order"] = int(coerce_int(step_order, default=0))
    pending["enforcement_count"] = int(coerce_int(pending.get("enforcement_count"), default=0)) + 1
    agent_state["pending_retry_requirements"] = pending
    return event
