from __future__ import annotations

from typing import Any, Dict, Optional

from backend.src.agent.core.context_budget import (
    CONTEXT_BUDGET_PIPELINE_VERSION,
    resolve_context_budgets,
)
from backend.src.common.utils import now_iso
from backend.src.services.llm.llm_client import resolve_llm_runtime_config


RUN_CONFIG_SNAPSHOT_VERSION = 1
CONTEXT_BUDGET_PIPELINE_STAGES = ["load", "trim", "compress"]


def _normalize_parameters(parameters: object) -> Dict[str, Any]:
    if not isinstance(parameters, dict):
        return {}
    out: Dict[str, Any] = {}
    for key, value in parameters.items():
        name = str(key or "").strip()
        if not name:
            continue
        out[name] = value
    return out


def build_run_config_snapshot(
    *,
    mode: Optional[str],
    requested_model: Optional[str],
    parameters: Optional[dict],
    requested_provider: Optional[str] = None,
) -> Dict[str, Any]:
    llm_config = resolve_llm_runtime_config(
        provider=str(requested_provider or "").strip() or None,
        model=str(requested_model or "").strip() or None,
    )
    return {
        "version": int(RUN_CONFIG_SNAPSHOT_VERSION),
        "captured_at": now_iso(),
        "mode": str(mode or "").strip() or None,
        "llm": {
            "provider": str(llm_config.get("provider") or "").strip() or None,
            "model": str(llm_config.get("model") or "").strip() or None,
            "base_url": str(llm_config.get("base_url") or "").strip() or None,
            "fallback_base_urls": list(llm_config.get("fallback_base_urls") or []),
        },
        "parameters": _normalize_parameters(parameters),
        "context_budget": resolve_context_budgets(),
        "context_budget_pipeline": {
            "version": int(CONTEXT_BUDGET_PIPELINE_VERSION),
            "stages": list(CONTEXT_BUDGET_PIPELINE_STAGES),
        },
    }


def apply_run_config_snapshot_if_missing(
    *,
    agent_state: Optional[dict],
    mode: Optional[str],
    requested_model: Optional[str],
    parameters: Optional[dict],
    requested_provider: Optional[str] = None,
) -> dict:
    state = dict(agent_state or {})
    existing = state.get("config_snapshot")
    if isinstance(existing, dict) and existing:
        return state
    state["config_snapshot"] = build_run_config_snapshot(
        mode=mode,
        requested_model=requested_model,
        parameters=parameters,
        requested_provider=requested_provider,
    )
    return state
