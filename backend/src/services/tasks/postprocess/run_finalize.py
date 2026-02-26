from __future__ import annotations

from typing import Callable, List, Optional, Tuple

from backend.src.constants import RUN_STATUS_DONE
from backend.src.services.tasks.postprocess.run_distill_actions import (
    autogen_skills_response,
    autogen_solution_if_allowed,
    collect_graph_update_if_allowed,
    sync_draft_skill_status,
    sync_review_skills,
)
from backend.src.services.tasks.postprocess.run_eval import create_eval_response
from backend.src.services.tasks.postprocess.run_gate import resolve_distill_gate
from backend.src.services.tasks.postprocess.run_memory import write_task_result_memory_safe


def postprocess_task_run_core(
    *,
    task_row,
    task_id: int,
    run_id: int,
    run_status: str,
    ensure_agent_review_record_fn: Callable[..., Optional[int]],
    safe_write_debug_fn: Callable[..., None],
    extract_graph_updates_fn: Callable[[int, int, List[dict], List[dict]], Optional[dict]],
    write_task_result_memory_if_missing_fn: Callable[..., Optional[dict]],
    resolve_default_model_fn: Callable[[], str],
) -> Tuple[Optional[dict], Optional[dict], Optional[dict]]:
    """
    任务执行结束后的后置处理：
    - 自动评估（Expectation -> Eval）
    - 图谱抽取/更新（从 step.result / output.content 推断）
    - 自动抽象技能卡（skills_items），并落盘（backend/prompt/skills）

    返回：(eval_response, skill_response, graph_update)
    """
    if run_status != RUN_STATUS_DONE:
        return None, None, None

    eval_response = create_eval_response(
        task_row=task_row,
        task_id=int(task_id),
        run_id=int(run_id),
    )

    gate = resolve_distill_gate(
        task_id=int(task_id),
        run_id=int(run_id),
        ensure_agent_review_record_fn=ensure_agent_review_record_fn,
        safe_write_debug_fn=safe_write_debug_fn,
    )
    allow_distill = bool(gate.get("allow_distill"))
    latest_review_id = gate.get("latest_review_id")
    review_status = str(gate.get("review_status") or "")

    sync_draft_skill_status(
        allow_distill=allow_distill,
        review_status=review_status,
        task_id=int(task_id),
        run_id=int(run_id),
        latest_review_id=int(latest_review_id) if latest_review_id is not None else None,
        safe_write_debug_fn=safe_write_debug_fn,
    )

    graph_update = collect_graph_update_if_allowed(
        allow_distill=allow_distill,
        task_id=int(task_id),
        run_id=int(run_id),
        extract_graph_updates_fn=extract_graph_updates_fn,
    )

    autogen_solution_if_allowed(
        allow_distill=allow_distill,
        task_id=int(task_id),
        run_id=int(run_id),
        safe_write_debug_fn=safe_write_debug_fn,
    )

    skill_response = autogen_skills_response(
        allow_distill=allow_distill,
        task_id=int(task_id),
        run_id=int(run_id),
        resolve_default_model_fn=resolve_default_model_fn,
    )

    write_task_result_memory_safe(
        task_row=task_row,
        task_id=int(task_id),
        run_id=int(run_id),
        write_task_result_memory_if_missing_fn=write_task_result_memory_if_missing_fn,
        safe_write_debug_fn=safe_write_debug_fn,
    )

    sync_review_skills(
        latest_review_id=int(latest_review_id) if latest_review_id is not None else None,
        skill_response=skill_response,
        task_id=int(task_id),
        run_id=int(run_id),
        safe_write_debug_fn=safe_write_debug_fn,
    )

    return eval_response, skill_response, graph_update
