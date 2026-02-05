from typing import List


def build_agent_plan_payload(
    *,
    plan_titles: List[str],
    plan_items: List[dict],
    plan_allows: List[List[str]],
    plan_artifacts: List[str],
) -> dict:
    """
    统一构造 task_runs.agent_plan 的 payload，避免各处散落硬编码键名。
    """
    return {
        "titles": plan_titles,
        "items": plan_items,
        "allows": plan_allows,
        "artifacts": plan_artifacts,
    }
