from __future__ import annotations

from typing import List, Optional

from backend.src.agent.plan_utils import sanitize_plan_brief
from backend.src.common.utils import coerce_int, parse_positive_int
from backend.src.constants import (
    ACTION_TYPE_USER_PROMPT,
    AGENT_TASK_FEEDBACK_KIND,
    AGENT_TASK_FEEDBACK_QUESTION,
    AGENT_TASK_FEEDBACK_STEP_BRIEF,
    AGENT_TASK_FEEDBACK_STEP_TITLE,
    RUN_STATUS_WAITING,
)


def canonicalized_feedback_meta(result: Optional[dict]) -> dict:
    obj = result if isinstance(result, dict) else {}
    return {
        "found": coerce_int(obj.get("found"), default=0),
        "removed": coerce_int(obj.get("removed"), default=0),
        "appended": bool(obj.get("appended")),
        "changed": bool(obj.get("changed")),
        "reask_feedback": bool(obj.get("reask_feedback")),
        "task_feedback_asked": bool(obj.get("task_feedback_asked")),
    }


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

    brief = sanitize_plan_brief(
        str(AGENT_TASK_FEEDBACK_STEP_BRIEF or "").strip(),
        fallback_title=title,
    )

    plan_titles.append(title)
    plan_allows.append([ACTION_TYPE_USER_PROMPT])
    plan_items.append(
        {
            "id": 0,
            "brief": brief,
            "status": "pending",
            "kind": "task_feedback",
            "prompt": {
                "question": build_task_feedback_question(),
                "kind": task_feedback_need_input_kind(),
            },
        }
    )

    # 重新编号：保持 plan_items.id 与顺序一致
    for idx, item in enumerate(plan_items, start=1):
        if isinstance(item, dict):
            item["id"] = idx
    return True


def canonicalize_task_feedback_steps(
    *,
    plan_titles: List[str],
    plan_items: List[dict],
    plan_allows: List[List[str]],
    keep_single_tail: bool,
    feedback_asked: bool,
    max_steps: Optional[int],
) -> dict:
    """
    规范化计划中的“确认满意度”步骤，避免重规划/补丁后出现重复或位置漂移。

    规则：
    - 先移除计划中的所有反馈步骤（若存在）；
    - 若 keep_single_tail=True 且 feedback_asked=False，则只在末尾追加一个反馈步骤；
    - 始终保证 titles/items/allows 长度与 id 连续性一致。
    """
    feedback_title = str(AGENT_TASK_FEEDBACK_STEP_TITLE or "").strip()
    if not feedback_title:
        return {"found": 0, "removed": 0, "appended": False, "changed": False}

    source_titles = list(plan_titles or [])
    source_allows = [list(value or []) for value in (plan_allows or [])]
    source_items = [
        dict(value) if isinstance(value, dict) else {"id": 0, "brief": "", "status": "pending"}
        for value in (plan_items or [])
    ]

    while len(source_allows) < len(source_titles):
        source_allows.append([])
    while len(source_items) < len(source_titles):
        source_items.append({"id": 0, "brief": "", "status": "pending"})

    kept_titles: List[str] = []
    kept_allows: List[List[str]] = []
    kept_items: List[dict] = []
    found_feedback = 0

    for idx, title_value in enumerate(source_titles):
        title_text = str(title_value or "").strip()
        if is_task_feedback_step_title(title_text):
            found_feedback += 1
            continue
        kept_titles.append(title_text)
        kept_allows.append(list(source_allows[idx] if idx < len(source_allows) else []))
        item = dict(source_items[idx] if idx < len(source_items) else {"id": 0, "brief": "", "status": "pending"})
        item["id"] = len(kept_items) + 1
        if not str(item.get("status") or "").strip():
            item["status"] = "pending"
        kept_items.append(item)

    appended = False
    should_append = bool(keep_single_tail) and (not bool(feedback_asked))
    if should_append:
        appended = append_task_feedback_step(
            plan_titles=kept_titles,
            plan_items=kept_items,
            plan_allows=kept_allows,
            max_steps=max_steps,
        )

    removed = max(0, int(found_feedback) - (1 if appended else 0))
    changed = (
        int(found_feedback) > 0
        or bool(appended)
        or len(kept_titles) != len(source_titles)
        or kept_titles != source_titles
    )

    plan_titles[:] = kept_titles
    plan_items[:] = kept_items
    plan_allows[:] = kept_allows
    return {
        "found": int(found_feedback),
        "removed": int(removed),
        "appended": bool(appended),
        "changed": bool(changed),
    }


def realign_feedback_step_for_resume(
    *,
    run_status: str,
    plan_titles: List[str],
    plan_items: List[dict],
    plan_allows: List[List[str]],
    paused_step_order: Optional[int],
    paused_step_title: str,
    task_feedback_asked: bool,
    max_steps: Optional[int],
) -> dict:
    """
    统一规范 resume 前的“确认满意度”步骤位置，避免旧 run 漂移导致提前 waiting。

    关键规则：
    - waiting 且当前 paused 在“确认满意度”，并且其后仍有非反馈步骤，判定为“提前反馈”；
    - 对“提前反馈”强制重置 feedback_asked=False，并把反馈步骤挪到计划尾部；
    - 其余场景保持原语义，仅做去重/位置归一化（避免重复反馈步骤）。
    """
    titles = list(plan_titles or [])
    normalized_status = str(run_status or "").strip().lower()
    paused_title = str(paused_step_title or "").strip()

    paused_index: Optional[int] = None
    if isinstance(paused_step_order, int):
        idx = int(paused_step_order) - 1
        if 0 <= idx < len(titles):
            paused_index = idx
            if not paused_title:
                paused_title = str(titles[idx] or "").strip()

    has_remaining_non_feedback = False
    if paused_index is not None:
        for raw in titles[paused_index + 1 :]:
            title_text = str(raw or "").strip()
            if title_text and (not is_task_feedback_step_title(title_text)):
                has_remaining_non_feedback = True
                break

    reask_feedback = bool(
        normalized_status == RUN_STATUS_WAITING
        and paused_title
        and is_task_feedback_step_title(paused_title)
        and has_remaining_non_feedback
    )
    paused_is_feedback = bool(paused_title and is_task_feedback_step_title(paused_title))
    # task_feedback_asked 只在“当前确实停在反馈问题上”时保留；
    # 其他场景（包括中途误触发 feedback）都重置，避免后续把普通步骤误判为“已收到反馈答案”。
    effective_feedback_asked = bool(task_feedback_asked) and paused_is_feedback and (not reask_feedback)
    max_steps_value = parse_positive_int(max_steps, default=None)

    canonicalized = canonicalize_task_feedback_steps(
        plan_titles=plan_titles,
        plan_items=plan_items,
        plan_allows=plan_allows,
        keep_single_tail=True,
        # 计划层保持“单一尾部 feedback 步骤”；是否已提问由 state.task_feedback_asked 管。
        feedback_asked=False,
        max_steps=max_steps_value,
    )
    canonicalized["reask_feedback"] = bool(reask_feedback)
    canonicalized["task_feedback_asked"] = bool(effective_feedback_asked)
    return canonicalized


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

