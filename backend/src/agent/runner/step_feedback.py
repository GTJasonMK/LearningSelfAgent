# -*- coding: utf-8 -*-
"""步骤反馈与重试约束汇总。"""

from __future__ import annotations

import json
from typing import Dict, List, Optional

from backend.src.agent.runner.failure_guidance import build_failure_guidance
from backend.src.agent.runner.goal_progress import evaluate_goal_progress
from backend.src.common.task_error_codes import extract_task_error_code
from backend.src.common.utils import coerce_int, now_iso

_STEP_FEEDBACK_HISTORY_LIMIT = 20


def _normalize_text_list(raw: object) -> List[str]:
    if not isinstance(raw, list):
        return []
    items: List[str] = []
    for item in raw:
        text = str(item or "").strip()
        if not text or text in items:
            continue
        items.append(text)
    return items


def _build_retry_requirements(
    *,
    status: str,
    action_type: str,
    failure_class: str,
    warnings: List[str],
    goal_progress_state: str,
) -> tuple[List[str], List[str]]:
    must_change: List[str] = []
    constraints: List[str] = []
    normalized_status = str(status or "").strip().lower()
    normalized_failure_class = str(failure_class or "").strip().lower()
    normalized_goal = str(goal_progress_state or "").strip().lower()
    normalized_action = str(action_type or "").strip().lower()

    if normalized_status in {"failed", "invalid", "error"} or normalized_goal in {"none", "regressed"}:
        if normalized_failure_class == "source_unavailable":
            must_change.extend(["source_selection", "query_strategy"])
            constraints.extend([
                "禁止继续使用同一外部源或相同 host。",
                "下一轮必须先扩大搜索候选源，再决定抓取目标。",
            ])
        elif normalized_failure_class == "contract_error":
            must_change.extend(["payload_shape", "tool_path"])
            constraints.extend([
                "优先修正 action/payload 契约，不要只重复调用同一步。",
                "下一轮必须输出更严格、更小的 JSON 载荷。",
            ])
        elif normalized_failure_class in {"llm_transient", "llm_rate_limit"}:
            must_change.extend(["llm_budget_profile", "action_shape"])
            constraints.extend([
                "下一轮必须降低动作复杂度，优先最小 JSON。",
                "如可拆分，请先拆成更小步骤再执行。",
            ])
        elif normalized_failure_class == "artifact_missing":
            must_change.extend(["execution_order", "artifact_strategy"])
            constraints.extend([
                "先补齐中间产物，再执行依赖该产物的步骤。",
            ])
        else:
            must_change.extend(["execution_order"])
            constraints.extend([
                "下一轮必须调整实现路径，不能原样重复当前动作。",
            ])

    if warnings and normalized_goal in {"none", "partial"}:
        if normalized_action in {"tool_call", "http_request"} and "source_selection" not in must_change:
            must_change.append("source_selection")
        constraints.append("最近步骤带 warning 且目标进展不足，不能把当前结果直接当最终答案。")

    normalized_must_change: List[str] = []
    for item in must_change:
        text = str(item or "").strip()
        if not text or text in normalized_must_change:
            continue
        normalized_must_change.append(text)
    normalized_constraints: List[str] = []
    for item in constraints:
        text = str(item or "").strip()
        if not text or text in normalized_constraints:
            continue
        normalized_constraints.append(text)
    return normalized_must_change, normalized_constraints


def _build_summary_for_llm(
    *,
    status: str,
    goal_progress_state: str,
    primary_error: str,
    warnings: List[str],
    matched_keywords: List[str],
    must_change: List[str],
) -> str:
    parts: List[str] = [f"status={str(status or '').strip() or 'unknown'}"]
    goal_text = str(goal_progress_state or "").strip()
    if goal_text:
        parts.append(f"goal_progress={goal_text}")
    if primary_error:
        parts.append(f"error={primary_error}")
    if warnings:
        parts.append(f"warning={warnings[0]}")
    if matched_keywords:
        parts.append(f"matched={','.join(matched_keywords[:4])}")
    if must_change:
        parts.append(f"must_change={','.join(must_change[:4])}")
    return "；".join(parts)


def build_step_feedback(
    *,
    message: str,
    step_order: int,
    title: str,
    action_type: str,
    status: str,
    result: Optional[dict] = None,
    error_message: str = "",
    failure_class: str = "",
    failure_signature: str = "",
    visible_content: str = "",
    context: Optional[Dict] = None,
    strategy_fingerprint: str = "",
    attempt_index: int = 0,
    previous_goal_progress_score: Optional[int] = None,
) -> Dict:
    result_obj = result if isinstance(result, dict) else {}
    warnings = _normalize_text_list(result_obj.get("warnings"))
    primary_error = str(error_message or result_obj.get("error") or result_obj.get("message") or "").strip()
    error_code = str(extract_task_error_code(primary_error) or result_obj.get("error_code") or "").strip()

    goal_progress = evaluate_goal_progress(
        message=message,
        title=title,
        action_type=action_type,
        result=result_obj,
        error_message=primary_error,
        visible_content=visible_content,
        context=context,
        previous_score=previous_goal_progress_score,
    )
    must_change, retry_constraints = _build_retry_requirements(
        status=status,
        action_type=action_type,
        failure_class=failure_class,
        warnings=warnings,
        goal_progress_state=str(goal_progress.get("state") or ""),
    )
    summary_for_llm = _build_summary_for_llm(
        status=status,
        goal_progress_state=str(goal_progress.get("state") or ""),
        primary_error=primary_error,
        warnings=warnings,
        matched_keywords=list(goal_progress.get("matched_keywords") or []) + list(goal_progress.get("matched_requirements") or []),
        must_change=must_change,
    )

    return {
        "step_order": int(coerce_int(step_order, default=0)),
        "title": str(title or "").strip(),
        "action_type": str(action_type or "").strip(),
        "status": str(status or "").strip().lower() or "unknown",
        "goal_progress": str(goal_progress.get("state") or "none"),
        "goal_progress_score": int(coerce_int(goal_progress.get("score"), default=0)),
        "goal_progress_reason": str(goal_progress.get("reason") or "").strip(),
        "failure_class": str(failure_class or "").strip(),
        "failure_signature": str(failure_signature or "").strip(),
        "error_code": error_code,
        "primary_error": primary_error,
        "warnings": warnings,
        "matched_keywords": list(goal_progress.get("matched_keywords") or []),
        "evidence_overlap": int(coerce_int(goal_progress.get("matched_keyword_count"), default=0)),
        "must_change": must_change,
        "retry_constraints": retry_constraints,
        "summary_for_llm": summary_for_llm,
        "strategy_fingerprint": str(strategy_fingerprint or "").strip(),
        "attempt_index": int(coerce_int(attempt_index, default=0)),
        "created_at": now_iso(),
    }


def register_step_feedback(agent_state: Dict, feedback: Dict) -> Dict:
    if not isinstance(agent_state, dict):
        return feedback
    item = dict(feedback or {})
    history = agent_state.get("step_feedback_history") if isinstance(agent_state.get("step_feedback_history"), list) else []
    history = list(history)
    history.append(item)
    if len(history) > _STEP_FEEDBACK_HISTORY_LIMIT:
        history = history[-_STEP_FEEDBACK_HISTORY_LIMIT:]
    agent_state["step_feedback_history"] = history
    agent_state["last_step_feedback"] = item
    agent_state["goal_progress"] = {
        "state": str(item.get("goal_progress") or "none"),
        "score": int(coerce_int(item.get("goal_progress_score"), default=0)),
        "reason": str(item.get("goal_progress_reason") or "").strip(),
        "step_order": int(coerce_int(item.get("step_order"), default=0)),
    }

    pending = agent_state.get("pending_retry_requirements") if isinstance(agent_state.get("pending_retry_requirements"), dict) else {}
    current_score = int(coerce_int(item.get("goal_progress_score"), default=0))
    baseline_score = int(coerce_int((pending or {}).get("baseline_goal_progress_score"), default=-1))
    improved = baseline_score < 0 or current_score > baseline_score
    if item.get("status") in {"failed", "invalid", "error"} or (
        item.get("must_change") and str(item.get("goal_progress") or "") in {"none", "regressed", "partial"}
    ):
        agent_state["pending_retry_requirements"] = {
            "active": True,
            "source_step_order": int(coerce_int(item.get("step_order"), default=0)),
            "failure_class": str(item.get("failure_class") or "").strip(),
            "failure_signature": str(item.get("failure_signature") or "").strip(),
            "blocked_strategy_fingerprint": str(item.get("strategy_fingerprint") or "").strip(),
            "baseline_goal_progress_score": current_score,
            "must_change": list(item.get("must_change") or []),
            "retry_constraints": list(item.get("retry_constraints") or []),
            "primary_error": str(item.get("primary_error") or "").strip(),
            "summary_for_llm": str(item.get("summary_for_llm") or "").strip(),
            "enforcement_count": int(coerce_int((pending or {}).get("enforcement_count"), default=0)),
        }
    elif pending and improved:
        agent_state.pop("pending_retry_requirements", None)
    agent_state["failure_guidance"] = build_failure_guidance(agent_state)
    return item


def summarize_recent_step_feedback_for_prompt(agent_state: Dict, limit: int = 3) -> str:
    if not isinstance(agent_state, dict):
        return "(无)"
    history = agent_state.get("step_feedback_history") if isinstance(agent_state.get("step_feedback_history"), list) else []
    rows: List[str] = []
    for raw in history[-max(1, int(limit)) :]:
        if not isinstance(raw, dict):
            continue
        head = f"step#{int(coerce_int(raw.get('step_order'), default=0))} {str(raw.get('action_type') or '').strip() or '-'}"
        title = str(raw.get("title") or "").strip()
        goal = str(raw.get("goal_progress") or "none").strip()
        failure_class = str(raw.get("failure_class") or "").strip()
        summary = str(raw.get("summary_for_llm") or "").strip()
        if title and failure_class:
            rows.append(f"- {head} | {title} | class={failure_class} | {summary or f'goal={goal}'}")
        elif title:
            rows.append(f"- {head} | {title} | {summary or f'goal={goal}'}")
        elif failure_class:
            rows.append(f"- {head} | class={failure_class} | {summary or f'goal={goal}'}")
        else:
            rows.append(f"- {head} | {summary or f'goal={goal}'}")
    return "\n".join(rows) if rows else "(无)"


def summarize_retry_requirements_for_prompt(agent_state: Dict) -> str:
    if not isinstance(agent_state, dict):
        return "(无)"
    pending = agent_state.get("pending_retry_requirements")
    if not isinstance(pending, dict) or not bool(pending.get("active")):
        return "(无)"
    rows: List[str] = []
    must_change = pending.get("must_change") if isinstance(pending.get("must_change"), list) else []
    constraints = pending.get("retry_constraints") if isinstance(pending.get("retry_constraints"), list) else []
    failure_class = str(pending.get("failure_class") or "").strip()
    primary_error = str(pending.get("primary_error") or "").strip()
    if failure_class:
        rows.append(f"- failure_class={failure_class}")
    if primary_error:
        rows.append(f"- primary_error={primary_error}")
    if must_change:
        rows.append(f"- must_change={','.join(str(item or '').strip() for item in must_change if str(item or '').strip())}")
    for item in constraints[:4]:
        text = str(item or "").strip()
        if text:
            rows.append(f"- {text}")
    return "\n".join(rows) if rows else "(无)"



def summarize_failure_guidance_for_prompt(agent_state: Dict) -> str:
    if not isinstance(agent_state, dict):
        return "(无)"
    guidance = str(agent_state.get("failure_guidance") or "").strip()
    if guidance:
        return guidance
    return build_failure_guidance(agent_state)
