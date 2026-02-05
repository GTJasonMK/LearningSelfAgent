from __future__ import annotations

from typing import Optional

from backend.src.agent.support import _extract_json_object
from backend.src.agent.runner.react_helpers import call_llm_for_text


def build_review_repair_prompt(
    *,
    review_status: str,
    review_summary: str,
    review_next_actions: str,
) -> str:
    """
    构造“评估未通过 -> 插入修复步骤”的提示词。

    注意：
    - 本模块的主要目的，是把“评估修复”的 LLM 调用从 react_loop 中隔离出来，便于单测 patch；
    - 输出必须是 JSON，交给上层去解析并应用 plan_patch.insert_steps。
    """
    status = str(review_status or "").strip()
    summary = str(review_summary or "").strip()
    next_actions = str(review_next_actions or "").strip()

    return (
        "你是一个本地桌宠 Agent 的“评估未通过修复器”。\n"
        "你的任务：根据评估信息，给出最小的修复步骤插入建议，让任务在进入“确认满意度”前真正完成。\n"
        "\n"
        "只输出 JSON（不要代码块、不要解释），格式如下：\n"
        "{\"insert_steps\":[{\"title\":\"...\",\"brief\":\"...\",\"allow\":[\"tool_call|shell_command|file_write|llm_call|task_output|memory_write\"]}]}\n"
        "\n"
        f"评估状态：{status}\n"
        f"评估摘要：{summary}\n"
        f"下一步建议：{next_actions}\n"
    )


def parse_insert_steps_from_text(text: str) -> Optional[list]:
    """
    从 LLM 文本中抽取 insert_steps（兼容 steps 字段）。
    """
    obj = _extract_json_object(text or "")
    if not isinstance(obj, dict):
        return None
    steps = obj.get("insert_steps")
    if steps is None:
        steps = obj.get("steps")
    return steps if isinstance(steps, list) else None


__all__ = [
    "build_review_repair_prompt",
    "call_llm_for_text",
    "parse_insert_steps_from_text",
]
