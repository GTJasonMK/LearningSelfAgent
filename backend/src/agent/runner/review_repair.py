from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from backend.src.actions.registry import list_action_types, normalize_action_type
from backend.src.agent.support import _extract_json_object
from backend.src.agent.runner.react_helpers import call_llm_for_text


REVIEW_GATE_DECISION_REPAIR = "repair"
REVIEW_GATE_DECISION_PROCEED_FEEDBACK = "proceed_feedback"
REVIEW_GATE_DECISION_ASK_USER = "ask_user"
REVIEW_GATE_DECISION_FINALIZE_WITH_RISK = "finalize_with_risk"

_REVIEW_GATE_DECISIONS = {
    REVIEW_GATE_DECISION_REPAIR,
    REVIEW_GATE_DECISION_PROCEED_FEEDBACK,
    REVIEW_GATE_DECISION_ASK_USER,
    REVIEW_GATE_DECISION_FINALIZE_WITH_RISK,
}


@dataclass
class ReviewGateDecision:
    decision: str
    reasons: List[str]
    evidence: List[str]
    insert_steps: Optional[List[dict]]
    parse_error: Optional[str] = None


def build_review_repair_prompt(
    *,
    review_status: str,
    review_summary: str,
    review_next_actions: str,
) -> str:
    """
    构造“评估未通过 -> 自然决策（修复/继续反馈）”提示词。

    输出协议：
    - decision=repair 时必须给 insert_steps；
    - 其他 decision 不需要 insert_steps；
    - 始终要求 reasons/evidence，便于后端留痕。
    """
    status = str(review_status or "").strip()
    summary = str(review_summary or "").strip()
    next_actions = str(review_next_actions or "").strip()
    action_types_line = "|".join(list_action_types())

    return (
        "你是本地桌宠 Agent 的\"评估决策器\"。\\n"
        "你要在\"继续修复\"与\"先进入用户反馈\"之间做最小充分决策。\\n"
        "必须严格只输出一个 JSON 对象（不要代码块、不要解释）。\\n"
        "首字符必须是 {，尾字符必须是 }。\\n"
        "\\n"
        "输出格式：\\n"
        "{\\n"
        "  \"decision\": \"repair|proceed_feedback|ask_user|finalize_with_risk\",\\n"
        "  \"reasons\": [\"原因1\", \"原因2\"],\\n"
        "  \"evidence\": [\"证据1\", \"证据2\"],\\n"
        "  \"insert_steps\": [\\n"
        "    {\"title\":\"...\",\"brief\":\"...\",\"allow\":[\"action_type\"]}\\n"
        "  ]\\n"
        "}\\n"
        "\\n"
        "决策规则：\\n"
        "1) 若已满足目标且风险可接受，可选 proceed_feedback 或 finalize_with_risk。\\n"
        "2) 若关键证据缺失、产物不实或步骤未真实执行，必须选 repair。\\n"
        "3) decision=repair 时，insert_steps 数量 1..4，且每步 allow 必须来自："
        f"{action_types_line}。\\n"
        "4) decision!=repair 时，insert_steps 留空数组即可。\\n"
        "5) 禁止通过 file_write 伪造执行结果或伪造证据。\\n"
        "6) 若使用 file_read/json_parse，title 必须写成 file_read:<相对路径> 或 json_parse:<相对路径>。\\n"
        "   若要验证 task_output 文本，先用 file_write:<相对路径> 落盘已有输出，再做 file_read/json_parse 校验。\\n"
        "\\n"
        f"评估状态：{status}\\n"
        f"评估摘要：{summary}\\n"
        f"下一步建议：{next_actions}\\n"
    )


def _normalize_allow_list_from_step(step_obj: dict) -> List[str]:
    """
    归一化修复步骤 allow：
    - 优先使用模型给出的 allow；
    - allow 缺失时，尝试从 title 前缀推断（如 shell_command:...）；
    - 仍无法推断时，降级为 task_output，确保 patch 不因 allow 为空而整体失败。
    """
    allow_raw = step_obj.get("allow")
    allows: List[str] = []

    if isinstance(allow_raw, str):
        allow_raw = [allow_raw]

    if isinstance(allow_raw, list):
        for item in allow_raw:
            normalized = normalize_action_type(str(item or ""))
            if normalized and normalized not in allows:
                allows.append(normalized)

    if allows:
        return allows

    title = str(step_obj.get("title") or "").strip()
    head = title.split(":", 1)[0].strip() if title else ""
    inferred = normalize_action_type(head)
    if inferred:
        return [inferred]

    return ["task_output"]


def _collect_text_list(value: object) -> List[str]:
    items: List[str] = []
    if isinstance(value, str):
        text = value.strip()
        if text:
            items.append(text)
        return items
    if not isinstance(value, list):
        return items
    for raw in value:
        text = str(raw or "").strip()
        if text:
            items.append(text)
    return items


def _parse_insert_steps_from_obj(obj: dict) -> Optional[List[dict]]:
    steps = obj.get("insert_steps")
    if steps is None:
        steps = obj.get("steps")
    if not isinstance(steps, list):
        return None

    normalized_steps: List[dict] = []
    for raw in steps:
        if not isinstance(raw, dict):
            continue
        title = str(raw.get("title") or "").strip()
        if not title:
            continue
        brief = str(raw.get("brief") or "").strip() or "修复问题"
        normalized_steps.append(
            {
                "title": title,
                "brief": brief,
                "allow": _normalize_allow_list_from_step(raw),
            }
        )

    return normalized_steps or None


def parse_insert_steps_from_text(text: str) -> Optional[list]:
    """
    从 LLM 文本中抽取 insert_steps（兼容 steps 字段），并做最小归一化。
    """
    obj = _extract_json_object(text or "")
    if not isinstance(obj, dict):
        return None
    return _parse_insert_steps_from_obj(obj)


def parse_review_gate_decision_from_text(text: str) -> ReviewGateDecision:
    """
    解析评估门闩决策：
    - 优先读取 decision/reasons/evidence；
    - 若缺少 decision，但存在 insert_steps，则推断为 repair（兼容旧输出）；
    - 解析失败时降级为 proceed_feedback，避免链路阻塞。
    """
    fallback = ReviewGateDecision(
        decision=REVIEW_GATE_DECISION_PROCEED_FEEDBACK,
        reasons=["决策输出不可解析，降级进入反馈"],
        evidence=[],
        insert_steps=None,
        parse_error="decision_output_invalid",
    )

    obj = _extract_json_object(text or "")
    if not isinstance(obj, dict):
        return fallback

    insert_steps = _parse_insert_steps_from_obj(obj)
    decision_raw = str(obj.get("decision") or "").strip().lower()
    if decision_raw not in _REVIEW_GATE_DECISIONS:
        decision_raw = (
            REVIEW_GATE_DECISION_REPAIR
            if isinstance(insert_steps, list) and len(insert_steps) > 0
            else REVIEW_GATE_DECISION_PROCEED_FEEDBACK
        )

    reasons = _collect_text_list(obj.get("reasons"))
    reason_single = str(obj.get("reason") or "").strip()
    if reason_single and reason_single not in reasons:
        reasons.append(reason_single)

    evidence = _collect_text_list(obj.get("evidence"))

    parse_error = None
    if decision_raw == REVIEW_GATE_DECISION_REPAIR and not insert_steps:
        parse_error = "repair_steps_missing"

    return ReviewGateDecision(
        decision=decision_raw,
        reasons=reasons,
        evidence=evidence,
        insert_steps=insert_steps,
        parse_error=parse_error,
    )


__all__ = [
    "REVIEW_GATE_DECISION_REPAIR",
    "REVIEW_GATE_DECISION_PROCEED_FEEDBACK",
    "REVIEW_GATE_DECISION_ASK_USER",
    "REVIEW_GATE_DECISION_FINALIZE_WITH_RISK",
    "ReviewGateDecision",
    "build_review_repair_prompt",
    "call_llm_for_text",
    "parse_insert_steps_from_text",
    "parse_review_gate_decision_from_text",
]
