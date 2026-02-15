import logging
from dataclasses import dataclass
from typing import Callable, Generator, List, Optional

from backend.src.agent.core.plan_coordinator import PlanCoordinator
from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.planning_phase import PlanPhaseFailure, run_replan_phase
from backend.src.agent.runner.feedback import append_task_feedback_step, is_task_feedback_step_title
from backend.src.common.utils import now_iso
from backend.src.services.llm.llm_client import sse_json
from backend.src.repositories.task_runs_repo import update_task_run

logger = logging.getLogger(__name__)


@dataclass
class ReplanMergeResult:
    plan_struct: PlanStructure
    done_count: int


def run_replan_and_merge(
    *,
    task_id: int,
    run_id: int,
    message: str,
    workdir: str,
    model: str,
    react_params: dict,
    max_steps_value: int,
    tools_hint: str,
    skills_hint: str,
    solutions_hint: Optional[str] = None,
    memories_hint: str,
    graph_hint: str,
    plan_struct: PlanStructure,
    agent_state: dict,
    observations: List[str],
    done_count: int,
    error: str,
    sse_notice: str,
    replan_attempts: int,
    safe_write_debug: Callable[..., None],
    extra_observations: Optional[List[str]] = None,
) -> Generator[str, None, Optional[ReplanMergeResult]]:
    """
    统一 Replan + 合并逻辑：
    - 生成剩余 plan（包含 allow + artifacts）
    - 把历史步骤（0..done_count-1）合并到新计划前缀
    - 将历史 failed 标记为 skipped（避免阻塞最终输出）
    - 持久化 agent_plan/agent_state，并推送 plan SSE
    """
    if sse_notice:
        yield sse_json({"delta": f"{sse_notice}\n"})

    plan_titles = plan_struct.get_titles()
    plan_items = plan_struct.get_items_payload()

    done_steps = []
    for i in range(done_count):
        status_value = "done"
        if i < len(plan_items):
            status_value = str(plan_items[i].get("status") or "done")
        done_steps.append({"title": plan_titles[i] if i < len(plan_titles) else "", "status": status_value})

    replan_observations = list(observations)
    if extra_observations:
        replan_observations.extend(extra_observations)

    solutions_hint_value = str(solutions_hint or "").strip()
    if not solutions_hint_value and isinstance(agent_state, dict):
        solutions_hint_value = str(agent_state.get("solutions_hint") or "").strip()
    if not solutions_hint_value:
        solutions_hint_value = "(无)"

    try:
        replan_result = yield from run_replan_phase(
            task_id=int(task_id),
            run_id=int(run_id),
            message=message,
            workdir=workdir,
            model=model,
            parameters=react_params,
            max_steps=int(max_steps_value),
            tools_hint=tools_hint,
            skills_hint=skills_hint,
            solutions_hint=solutions_hint_value,
            memories_hint=memories_hint,
            graph_hint=graph_hint,
            plan_titles=plan_titles,
            plan_artifacts=list(plan_struct.artifacts or []),
            done_steps=done_steps,
            error=str(error or ""),
            observations=replan_observations,
        )
    except PlanPhaseFailure as exc:
        safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.replan.failed",
            data={"error": str(exc.reason)},
            level="warning",
        )
        return None

    if not replan_result:
        return None

    for idx, step in enumerate(plan_struct.steps[: max(0, int(done_count))], start=1):
        if str(step.status or "") == "failed":
            safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.replan.skip_failed",
                data={"step_order": idx, "title": str(step.title or "")},
                level="info",
            )

    merged_plan = PlanCoordinator.merge_replan_with_history(
        current_plan=plan_struct,
        done_count=int(done_count),
        replan_titles=replan_result.plan_titles,
        replan_allows=replan_result.plan_allows,
        replan_items=replan_result.plan_items,
        replan_artifacts=replan_result.plan_artifacts,
    )

    had_feedback_step = any(is_task_feedback_step_title(s.title) for s in plan_struct.steps)
    has_feedback_after_replan = any(is_task_feedback_step_title(s.title) for s in merged_plan.steps)
    feedback_asked = bool(agent_state.get("task_feedback_asked")) if isinstance(agent_state, dict) else False
    if had_feedback_step and (not feedback_asked) and (not has_feedback_after_replan):
        # append_task_feedback_step 仍使用 legacy 列表接口（后续可进一步收编）
        new_titles, new_items, new_allows, new_artifacts = merged_plan.to_legacy_lists()
        restored = append_task_feedback_step(
            plan_titles=new_titles,
            plan_items=new_items,
            plan_allows=new_allows,
            max_steps=None,
        )
        if restored:
            merged_plan = PlanStructure.from_legacy(
                plan_titles=new_titles,
                plan_items=new_items,
                plan_allows=new_allows,
                plan_artifacts=new_artifacts,
            )
            safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.replan.feedback_step_restored",
                data={"done_count": int(done_count), "plan_len": merged_plan.step_count},
                level="info",
            )

    agent_state["replan_attempts"] = int(replan_attempts) + 1
    agent_state["critical_failure"] = False
    agent_state["last_failed_step_id"] = None
    try:
        updated_at = now_iso()
        update_task_run(
            run_id=int(run_id),
            agent_plan=merged_plan.to_agent_plan_payload(),
            agent_state=agent_state,
            updated_at=updated_at,
        )
    except Exception as exc:
        safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.replan.persist_failed",
            data={"error": str(exc)},
            level="warning",
        )
    yield sse_json({"type": "plan", "task_id": task_id, "run_id": run_id, "items": merged_plan.get_items_payload()})
    return ReplanMergeResult(
        plan_struct=merged_plan,
        done_count=done_count,
    )
