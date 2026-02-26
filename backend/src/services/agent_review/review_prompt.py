from __future__ import annotations

import json
from typing import List, Optional

from backend.src.constants import (
    AGENT_REVIEW_DISTILL_SCORE_THRESHOLD,
    AGENT_REVIEW_PASS_SCORE_THRESHOLD,
    SKILL_CATEGORY_CHOICES,
)
from backend.src.prompt.system_prompts import load_system_prompt
from backend.src.services.llm.llm_client import resolve_default_model


def build_review_prompt_text(
    *,
    task_title: str,
    run_meta: dict,
    plan_compact: dict,
    steps_compact: List[dict],
    outputs_compact: List[dict],
    tools_compact: List[dict],
    user_note: str = "(auto)",
) -> str:
    prompt = load_system_prompt("agent_evaluate")
    if not prompt:
        # fallback：即使提示词文件缺失也要可用
        prompt = (
            "你是评估 Agent，只输出 JSON："
            "{{\"status\":\"pass|needs_changes|fail\",\"summary\":\"\",\"issues\":[],\"next_actions\":[],\"skills\":[]}}。\n"
            "输入：{task_title}\n{run_meta}\n{plan}\n{steps}\n{outputs}\n{tool_calls}\n"
        )

    skill_categories_text = "\n".join(f"- {c}" for c in SKILL_CATEGORY_CHOICES)
    return prompt.format(
        skill_categories=skill_categories_text,
        pass_threshold=int(AGENT_REVIEW_PASS_SCORE_THRESHOLD),
        distill_threshold=int(AGENT_REVIEW_DISTILL_SCORE_THRESHOLD),
        user_note=str(user_note or "(auto)"),
        task_title=task_title,
        run_meta=json.dumps(run_meta, ensure_ascii=False),
        plan=json.dumps(plan_compact, ensure_ascii=False),
        steps=json.dumps(steps_compact, ensure_ascii=False),
        outputs=json.dumps(outputs_compact, ensure_ascii=False),
        tool_calls=json.dumps(tools_compact, ensure_ascii=False),
    )


def resolve_review_model(
    *,
    mode: str,
    state_obj: Optional[dict],
    requested_model: str = "",
) -> str:
    requested = str(requested_model or "").strip()
    if requested:
        return requested

    model = resolve_default_model()

    # Think 模式：默认使用配置的 evaluator 模型（与 docs/agent 对齐）。
    if mode != "think":
        return model

    base_model = str(state_obj.get("model") or "").strip() if isinstance(state_obj, dict) else ""
    if not base_model:
        base_model = model

    raw_cfg = state_obj.get("think_config") if isinstance(state_obj, dict) else None
    try:
        from backend.src.agent.think import create_think_config_from_dict, get_default_think_config

        think_cfg = (
            create_think_config_from_dict(raw_cfg, base_model=base_model)
            if isinstance(raw_cfg, dict) and raw_cfg
            else get_default_think_config(base_model=base_model)
        )
        return str(getattr(think_cfg, "evaluator_model", "") or "").strip() or base_model
    except Exception:
        return base_model
