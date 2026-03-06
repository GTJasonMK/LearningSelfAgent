# -*- coding: utf-8 -*-
"""
运行级尝试控制器（收敛/策略/不可达证明）。

目标：
- 让“重试”变成可观测、可评估、可切换策略的尝试循环；
- 避免同一失败在同一策略下无意义重复；
- 失败时输出结构化不可达证明，提升可调试性。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Optional

from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.common.task_error_codes import extract_task_error_code, is_source_failure_error_code
from backend.src.common.utils import coerce_int, now_iso
from backend.src.services.llm.llm_client import classify_llm_error_text

_STRATEGY_HISTORY_LIMIT = 30
_PROGRESS_HISTORY_LIMIT = 80
_RECENT_FAILURE_LIMIT = 5

_SOURCE_SELECTION_OPTIONS = ("auto", "mirror_preferred", "api_first", "search_first")
_TOOL_PATH_OPTIONS = ("default", "tool_call_first", "http_request_first", "shell_fallback")
_LLM_BUDGET_OPTIONS = ("balanced", "strict_json", "low_temp")
_EXECUTION_ORDER_OPTIONS = ("sequential", "prepare_then_execute")


def _clamp_score(value: float) -> int:
    try:
        number = int(round(float(value)))
    except Exception:
        number = 0
    if number < 0:
        return 0
    if number > 100:
        return 100
    return int(number)


def _pick_option(options: tuple[str, ...], variant: int, offset: int = 0) -> str:
    if not options:
        return "auto"
    idx = (int(variant) + int(offset)) % len(options)
    return str(options[idx])


def _count_plan_status(plan_struct: PlanStructure) -> dict[str, int]:
    counts: dict[str, int] = {}
    items = plan_struct.get_items_payload() if isinstance(plan_struct, PlanStructure) else []
    for raw in items or []:
        item = raw if isinstance(raw, dict) else {}
        status = str(item.get("status") or "pending").strip().lower() or "pending"
        if status in {"planned", "queued"}:
            status = "pending"
        counts[status] = int(counts.get(status, 0)) + 1
    return counts


def _strategy_vector(agent_state: Dict, *, plan_struct: PlanStructure) -> dict:
    variant = coerce_int(agent_state.get("strategy_variant"), default=0)
    source_selection = str(agent_state.get("source_selection") or "").strip() or _pick_option(
        _SOURCE_SELECTION_OPTIONS, variant, 0
    )
    tool_path = str(agent_state.get("tool_path") or "").strip() or _pick_option(
        _TOOL_PATH_OPTIONS, variant, 1
    )
    llm_budget = str(agent_state.get("llm_budget_profile") or "").strip() or _pick_option(
        _LLM_BUDGET_OPTIONS, variant, 2
    )
    execution_order = str(agent_state.get("execution_order") or "").strip() or _pick_option(
        _EXECUTION_ORDER_OPTIONS, variant, 3
    )
    return {
        "source_selection": source_selection,
        "tool_path": tool_path,
        "llm_budget_profile": llm_budget,
        "execution_order": execution_order,
        "plan_step_count": int(plan_struct.step_count),
        "variant": int(variant),
    }


def _strategy_fingerprint(vector: dict) -> str:
    encoded = json.dumps(vector, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha1(encoded.encode("utf-8")).hexdigest()[:16]
    return str(digest)


def _append_limited(history: list, row: dict, *, limit: int) -> list:
    items = list(history or [])
    items.append(dict(row or {}))
    if len(items) > int(limit):
        items = items[-int(limit) :]
    return items


def classify_failure_class(error_text: str) -> str:
    """
    将错误归类为编排层可消费的 failure_class。
    """
    text = str(error_text or "").strip()
    code = extract_task_error_code(text)
    if code and is_source_failure_error_code(code):
        return "source_unavailable"
    if code in {
        "invalid_action_payload",
        "invalid_tool_payload",
        "missing_tool_exec_spec",
        "tool_exec_contract_error",
    }:
        return "contract_error"

    kind = classify_llm_error_text(text)
    if kind == "rate_limit":
        return "llm_rate_limit"
    if kind == "transient":
        return "llm_transient"

    lowered = text.lower()
    if "action_invalid" in lowered or "action 不合法" in lowered:
        return "action_invalid"
    if "missing_expected_artifact" in lowered or "artifact" in lowered:
        return "artifact_missing"
    return "execution_error"


def ensure_strategy_state(
    *,
    agent_state: Dict,
    plan_struct: PlanStructure,
    reason: str = "run_start",
) -> dict:
    """
    初始化（或刷新）策略状态并返回 strategy_update 事件。
    """
    if not isinstance(agent_state, dict):
        agent_state = {}

    variant = coerce_int(agent_state.get("strategy_variant"), default=0)
    agent_state["strategy_variant"] = int(max(0, variant))

    vector = _strategy_vector(agent_state, plan_struct=plan_struct)
    fingerprint = _strategy_fingerprint(vector)

    previous = str(agent_state.get("strategy_fingerprint") or "").strip()
    changed = bool(previous != fingerprint)

    history = agent_state.get("strategy_history") if isinstance(agent_state.get("strategy_history"), list) else []
    attempt_index = coerce_int(agent_state.get("attempt_index"), default=0)
    if changed or attempt_index <= 0:
        attempt_index = max(1, int(attempt_index) + 1)
        row = {
            "attempt_index": int(attempt_index),
            "strategy_fingerprint": str(fingerprint),
            "strategy": dict(vector),
            "reason": str(reason or "").strip() or "update",
            "changed": bool(changed),
            "created_at": now_iso(),
        }
        history = _append_limited(history, row, limit=_STRATEGY_HISTORY_LIMIT)
        agent_state["strategy_history"] = history
        agent_state["attempt_index"] = int(attempt_index)

    agent_state["strategy"] = dict(vector)
    agent_state["strategy_fingerprint"] = str(fingerprint)

    return {
        "type": "strategy_update",
        "strategy": dict(vector),
        "strategy_fingerprint": str(fingerprint),
        "attempt_index": int(agent_state.get("attempt_index") or 1),
        "reason": str(reason or "").strip() or "update",
        "changed": bool(changed),
        "switched": False,
    }


def rotate_strategy(
    *,
    agent_state: Dict,
    plan_struct: PlanStructure,
    reason: str,
    failure_class: str = "",
) -> dict:
    """
    策略切换：增加 variant 并按固定序列轮换策略向量。
    """
    variant = coerce_int(agent_state.get("strategy_variant"), default=0) + 1
    agent_state["strategy_variant"] = int(max(0, variant))
    # 根据 variant 确定性轮换，避免同策略重复重试。
    agent_state["source_selection"] = _pick_option(_SOURCE_SELECTION_OPTIONS, variant, 0)
    agent_state["tool_path"] = _pick_option(_TOOL_PATH_OPTIONS, variant, 1)
    agent_state["llm_budget_profile"] = _pick_option(_LLM_BUDGET_OPTIONS, variant, 2)
    agent_state["execution_order"] = _pick_option(_EXECUTION_ORDER_OPTIONS, variant, 3)

    event = ensure_strategy_state(agent_state=agent_state, plan_struct=plan_struct, reason=reason)
    event["switched"] = True
    if str(failure_class or "").strip():
        event["failure_class"] = str(failure_class).strip()
    return event


def strategy_meta(agent_state: Dict) -> dict:
    return {
        "strategy_fingerprint": str(agent_state.get("strategy_fingerprint") or "").strip(),
        "attempt_index": int(coerce_int(agent_state.get("attempt_index"), default=0)),
    }


def update_progress_state(
    *,
    agent_state: Dict,
    plan_struct: PlanStructure,
    context: Optional[Dict] = None,
    reason: str = "",
    step_order: Optional[int] = None,
) -> dict:
    """
    基于运行态计算 progress_score，并返回 progress_update 事件。
    """
    context_obj = context if isinstance(context, dict) else {}
    counts = _count_plan_status(plan_struct)
    total = max(1, int(plan_struct.step_count))
    done = int(counts.get("done", 0))
    failed = int(counts.get("failed", 0))

    done_ratio = float(done) / float(total)
    validation_score = done_ratio * 100.0

    artifact_hits = 0
    if str(context_obj.get("latest_external_url") or "").strip():
        artifact_hits += 1
    if str(context_obj.get("latest_parse_input_text") or "").strip():
        artifact_hits += 1
    if context_obj.get("latest_script_json_output") is not None:
        artifact_hits += 1
    if isinstance(context_obj.get("latest_shell_artifacts"), list) and context_obj.get("latest_shell_artifacts"):
        artifact_hits += 1
    artifact_score = min(100.0, float(artifact_hits) * 25.0)

    goal_progress = agent_state.get("goal_progress") if isinstance(agent_state.get("goal_progress"), dict) else {}
    goal_progress_score = float(max(0, min(100, coerce_int(goal_progress.get("score"), default=0))))
    goal_progress_state = str(goal_progress.get("state") or "none").strip() or "none"

    failure_signatures = (
        agent_state.get("failure_signatures") if isinstance(agent_state.get("failure_signatures"), dict) else {}
    )
    source_failure_count = 0
    for key, info in (failure_signatures or {}).items():
        sign = str(key or "")
        if "|code:" not in sign:
            continue
        code = sign.split("|code:", 1)[1].strip().lower()
        if is_source_failure_error_code(code):
            source_failure_count += int(coerce_int((info or {}).get("count"), default=0))
    source_health_score = max(0.0, 100.0 - min(100.0, float(source_failure_count) * 20.0))

    streak_count = float(max(0, coerce_int(agent_state.get("failure_streak_count"), default=0)))
    error_pressure_score = max(0.0, 100.0 - min(100.0, streak_count * 25.0) - float(failed) * 5.0)

    score = _clamp_score(
        0.45 * goal_progress_score
        + 0.25 * validation_score
        + 0.15 * artifact_score
        + 0.10 * source_health_score
        + 0.05 * error_pressure_score
    )

    prev_score = coerce_int(agent_state.get("progress_score"), default=-1)
    improved = bool(prev_score < 0 or score > prev_score)
    no_progress_streak = 0 if improved else coerce_int(agent_state.get("no_progress_streak"), default=0) + 1
    agent_state["progress_score"] = int(score)
    agent_state["no_progress_streak"] = int(max(0, no_progress_streak))

    progress_row = {
        "score": int(score),
        "previous_score": int(prev_score),
        "improved": bool(improved),
        "no_progress_streak": int(max(0, no_progress_streak)),
        "step_order": int(step_order) if step_order is not None else None,
        "reason": str(reason or "").strip() or "update",
        "created_at": now_iso(),
    }
    history = agent_state.get("progress_history") if isinstance(agent_state.get("progress_history"), list) else []
    history = _append_limited(history, progress_row, limit=_PROGRESS_HISTORY_LIMIT)
    agent_state["progress_history"] = history

    meta = strategy_meta(agent_state)
    return {
        "type": "progress_update",
        "score": int(score),
        "previous_score": int(prev_score if prev_score >= 0 else score),
        "improved": bool(improved),
        "no_progress_streak": int(max(0, no_progress_streak)),
        "step_order": int(step_order) if step_order is not None else None,
        "reason": str(reason or "").strip() or "update",
        "attempt_index": int(meta.get("attempt_index") or 0),
        "strategy_fingerprint": str(meta.get("strategy_fingerprint") or ""),
        "goal_progress": goal_progress_state,
        "metrics": {
            "goal_progress_score": _clamp_score(goal_progress_score),
            "validation_score": _clamp_score(validation_score),
            "artifact_score": _clamp_score(artifact_score),
            "source_health_score": _clamp_score(source_health_score),
            "error_pressure_score": _clamp_score(error_pressure_score),
        },
    }


def build_unreachable_proof_event(
    *,
    agent_state: Dict,
    task_id: int,
    run_id: int,
    reason: str,
    failure_class: str,
    error_message: str,
) -> dict:
    """
    构建并写入不可达证明（用于 failed 终态的结构化可观测）。
    """
    strategy_hist = agent_state.get("strategy_history") if isinstance(agent_state.get("strategy_history"), list) else []
    failure_signatures = (
        agent_state.get("failure_signatures") if isinstance(agent_state.get("failure_signatures"), dict) else {}
    )
    ranked = []
    for signature, info in (failure_signatures or {}).items():
        count = coerce_int((info or {}).get("count"), default=0)
        ranked.append((int(count), str(signature or "")))
    ranked.sort(key=lambda item: item[0], reverse=True)

    seed = f"{run_id}:{reason}:{failure_class}:{now_iso()}"
    proof_id = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:20]
    proof = {
        "type": "unreachable_proof",
        "proof_id": str(proof_id),
        "task_id": int(task_id),
        "run_id": int(run_id),
        "reason": str(reason or "").strip() or "unreachable",
        "failure_class": str(failure_class or "").strip() or "execution_error",
        "error_message": str(error_message or "").strip(),
        "attempt_index": int(coerce_int(agent_state.get("attempt_index"), default=0)),
        "strategy_fingerprint": str(agent_state.get("strategy_fingerprint") or "").strip(),
        "strategy_attempts": int(len(strategy_hist or [])),
        "no_progress_streak": int(coerce_int(agent_state.get("no_progress_streak"), default=0)),
        "recent_failures": [
            {"signature": sign, "count": cnt} for cnt, sign in ranked[:_RECENT_FAILURE_LIMIT] if sign
        ],
        "created_at": now_iso(),
    }
    agent_state["unreachable_proof"] = dict(proof)
    return dict(proof)
