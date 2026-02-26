from __future__ import annotations

from typing import List, Optional

from backend.src.services.agent_review.review_prompt import (
    build_review_prompt_text as build_review_prompt_text_shared,
)
from backend.src.services.agent_review.review_prompt import (
    resolve_review_model as resolve_review_model_shared,
)


def build_review_prompt_text(
    *,
    task_title: str,
    run_meta: dict,
    plan_compact: dict,
    steps_compact: List[dict],
    outputs_compact: List[dict],
    tools_compact: List[dict],
) -> str:
    # 后处理评估固定使用自动来源标记，避免被 UI 消息污染。
    return build_review_prompt_text_shared(
        task_title=task_title,
        run_meta=run_meta,
        plan_compact=plan_compact,
        steps_compact=steps_compact,
        outputs_compact=outputs_compact,
        tools_compact=tools_compact,
        user_note="(auto)",
    )


def resolve_review_model(*, mode: str, state_obj: Optional[dict]) -> str:
    return resolve_review_model_shared(
        mode=mode,
        state_obj=state_obj,
        requested_model="",
    )
