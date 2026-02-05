"""
兼容层：集中导出 Agent 常用的内部函数。

背景：
- 早期实现把这些函数放在 backend/src/services/agent_support.py；
- 激进重构后按职责拆分到 backend/src/agent/*；
- 为了减少上层（API/后处理）的 import 改动，这里做一次集中导出。
"""

from backend.src.agent.json_utils import _extract_json_object
from backend.src.agent.observation import _truncate_observation
from backend.src.agent.plan_utils import (
    _fallback_brief_from_title,
    _normalize_plan_titles,
    apply_next_step_patch,
    coerce_file_write_payload_path_from_title,
    extract_file_write_target_path,
    repair_plan_artifacts_with_file_write_steps,
)
from backend.src.agent.retrieval import (
    _assess_knowledge_sufficiency,
    _collect_tools_from_solutions,
    _compose_skills,
    _draft_skill_from_message,
    _draft_solution_from_skills,
    _filter_relevant_domains,
    _format_graph_for_prompt,
    _format_memories_for_prompt,
    _format_skills_for_prompt,
    _format_solutions_for_prompt,
    _list_tool_hints,
    _select_relevant_graph_nodes,
    _select_relevant_memories,
    _select_relevant_skills,
    _select_relevant_solutions,
    ComposedSkillResult,
    DraftSkillResult,
    DraftSolutionResult,
    KnowledgeSufficiencyResult,
)
from backend.src.agent.validation import _validate_action

__all__ = [
    "_extract_json_object",
    "_truncate_observation",
    "_fallback_brief_from_title",
    "_normalize_plan_titles",
    "apply_next_step_patch",
    "coerce_file_write_payload_path_from_title",
    "extract_file_write_target_path",
    "repair_plan_artifacts_with_file_write_steps",
    "_assess_knowledge_sufficiency",
    "_collect_tools_from_solutions",
    "_compose_skills",
    "_draft_skill_from_message",
    "_draft_solution_from_skills",
    "_filter_relevant_domains",
    "_format_graph_for_prompt",
    "_format_memories_for_prompt",
    "_format_skills_for_prompt",
    "_format_solutions_for_prompt",
    "_list_tool_hints",
    "_select_relevant_graph_nodes",
    "_select_relevant_memories",
    "_select_relevant_skills",
    "_select_relevant_solutions",
    "_validate_action",
    "ComposedSkillResult",
    "DraftSkillResult",
    "DraftSolutionResult",
    "KnowledgeSufficiencyResult",
]
