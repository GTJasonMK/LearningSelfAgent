from __future__ import annotations

from typing import List, Optional

from backend.src.constants import (
    ACTION_TYPE_USER_PROMPT,
    AGENT_PLAN_BRIEF_MAX_CHARS,
    AGENT_TASK_FEEDBACK_KIND,
    AGENT_TASK_FEEDBACK_QUESTION,
    AGENT_TASK_FEEDBACK_STEP_BRIEF,
    AGENT_TASK_FEEDBACK_STEP_TITLE,
)


def append_task_feedback_step(
    *,
    plan_titles: List[str],
    plan_items: List[dict],
    plan_allows: List[List[str]],
    max_steps: Optional[int],
) -> bool:
    """
    在计划末尾追加“确认满意度”步骤（用于任务完成后的用户反馈闭环）。

    - 若最后一步已是反馈步骤，则不重复追加
    - 若超出 max_steps，则不追加（返回 False）
    """
    title = str(AGENT_TASK_FEEDBACK_STEP_TITLE or "").strip()
    if not title:
        return False
    if plan_titles and str(plan_titles[-1] or "").strip() == title:
        return False

    if isinstance(max_steps, int) and max_steps > 0 and len(plan_titles) + 1 > int(max_steps):
        return False

    brief = str(AGENT_TASK_FEEDBACK_STEP_BRIEF or title).strip() or title
    brief = brief.replace(" ", "").replace("：", "").replace(":", "")
    if len(brief) > int(AGENT_PLAN_BRIEF_MAX_CHARS or 0):
        brief = brief[: int(AGENT_PLAN_BRIEF_MAX_CHARS or 0)]

    plan_titles.append(title)
    plan_allows.append([ACTION_TYPE_USER_PROMPT])
    plan_items.append({"id": 0, "brief": brief, "status": "pending"})

    # 重新编号：保持 plan_items.id 与顺序一致
    for idx, item in enumerate(plan_items, start=1):
        if isinstance(item, dict):
            item["id"] = idx
    return True


def is_task_feedback_step_title(title: str) -> bool:
    return str(title or "").strip() == str(AGENT_TASK_FEEDBACK_STEP_TITLE or "").strip()


def build_task_feedback_question() -> str:
    return str(AGENT_TASK_FEEDBACK_QUESTION or "").strip() or "你对本次任务的执行满意吗？"


def task_feedback_need_input_kind() -> str:
    return str(AGENT_TASK_FEEDBACK_KIND or "").strip() or "task_feedback"


def is_positive_feedback(answer: str) -> bool:
    """
    简单启发式：把“无法明确判定”的输入也当作“不满意反馈”（这样才能触发继续改进）。
    """
    text = str(answer or "").strip()
    if not text:
        return False
    lower = text.lower()

    # 明确否定
    if "不满意" in text or "不符合" in text or "不对" in text:
        return False
    if lower in {"no", "n"}:
        return False
    if text.startswith("否"):
        return False

    # 明确肯定
    if lower in {"yes", "y", "ok", "okay"}:
        return True
    if text in {"是", "满意", "可以", "好的"}:
        return True
    if text.startswith("是") and "不是" not in text:
        return True
    if "满意" in text and "不满意" not in text:
        return True

    return False

