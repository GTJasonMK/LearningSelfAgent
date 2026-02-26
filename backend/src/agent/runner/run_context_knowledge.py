# -*- coding: utf-8 -*-
"""
AgentRunContext 与知识检索结果映射助手。
"""

from __future__ import annotations

from typing import List, Optional

from backend.src.agent.core.run_context import AgentRunContext
from backend.src.common.utils import parse_positive_int


def _extract_positive_int_ids(items: List[dict]) -> List[int]:
    ids: List[int] = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        value = parse_positive_int(item.get("id"), default=None)
        if value is not None:
            ids.append(value)
    return ids


def apply_knowledge_identity_to_run_ctx(
    run_ctx: AgentRunContext,
    *,
    domain_ids: List[str],
    skills: List[dict],
    solutions: List[dict],
    draft_solution_id: Optional[int],
) -> None:
    """
    将 knowledge 相关身份字段统一写入 run_ctx。
    """
    run_ctx.set_extra("domain_ids", list(domain_ids or []))
    run_ctx.set_extra("skill_ids", _extract_positive_int_ids(list(skills or [])))
    run_ctx.set_extra("solution_ids", _extract_positive_int_ids(list(solutions or [])))
    run_ctx.set_extra(
        "draft_solution_id",
        parse_positive_int(draft_solution_id, default=None),
    )
