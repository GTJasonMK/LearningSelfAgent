import json
from dataclasses import dataclass
from typing import Callable, Dict, Generator, List, Optional

from backend.src.agent.support import _truncate_observation, apply_next_step_patch
from backend.src.agent.runner.feedback import (
    append_task_feedback_step,
    build_task_feedback_question,
    is_positive_feedback,
    is_task_feedback_step_title,
    task_feedback_need_input_kind,
)
from backend.src.agent.runner.plan_events import sse_plan, sse_plan_delta
from backend.src.agent.runner.react_plan_state import build_agent_plan_payload
from backend.src.common.utils import now_iso
from backend.src.constants import (
    AGENT_REACT_REPLAN_MAX_ATTEMPTS,
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_WAITING,
    STATUS_WAITING,
    STEP_STATUS_RUNNING,
    STREAM_TAG_ASK,
    STREAM_TAG_EXEC,
    STREAM_TAG_FAIL,
    STREAM_TAG_OK,
    TASK_OUTPUT_TYPE_USER_PROMPT,
)
from backend.src.repositories.task_outputs_repo import create_task_output
from backend.src.repositories.task_steps_repo import (
    TaskStepCreateParams,
    create_task_step,
    mark_task_step_done,
)
from backend.src.repositories.task_runs_repo import update_task_run
from backend.src.repositories.tasks_repo import update_task
from backend.src.services.llm.llm_client import sse_json


@dataclass
class TaskFeedbackOutcome:
    """
    处理“确认满意度”步骤后的控制结果：
    - run_status != None：外层应 break 并以该状态结束本次生成器；
    - next_idx != None：外层应把 idx 设为 next_idx 并 continue；
    - plan_* 不为 None：外层应替换当前 plan 引用（通常发生在 replan 之后）。
    """

    run_status: Optional[str] = None
    next_idx: Optional[int] = None
    plan_titles: Optional[List[str]] = None
    plan_items: Optional[List[dict]] = None
    plan_allows: Optional[List[List[str]]] = None
    plan_artifacts: Optional[List[str]] = None


def maybe_apply_review_gate_before_feedback(
    *,
    task_id: int,
    run_id: int,
    idx: int,
    title: str,
    model: str,
    react_params: dict,
    variables_source: str,
    llm_call: Callable[[dict], dict],
    plan_titles: List[str],
    plan_items: List[dict],
    plan_allows: List[List[str]],
    plan_artifacts: List[str],
    max_steps_limit: Optional[int],
    safe_write_debug: Callable[..., None],
) -> Generator[str, None, bool]:
    """
    评估门闩（在“确认满意度”之前）：
    - 若评估未通过，则在“确认满意度”之前插入修复步骤并继续执行（不立刻进入 waiting）；
    - 直到评估通过，才允许进入 waiting 询问满意度。

    返回：是否已插入修复步骤（True 表示外层应 continue，开始执行插入的新步骤）。
    """
    if not is_task_feedback_step_title(title):
        return False
    if idx <= 0:
        return False

    review_id = None
    review_row = None
    review_status = ""
    review_summary = ""
    review_next_actions = ""
    try:
        from backend.src.repositories.agent_reviews_repo import get_agent_review as repo_get_agent_review
        from backend.src.services.tasks.task_postprocess import ensure_agent_review_record

        yield sse_json({"delta": f"{STREAM_TAG_EXEC} 评估任务完成度…\n"})
        review_id = ensure_agent_review_record(
            task_id=int(task_id),
            run_id=int(run_id),
            skills=[],
            force=True,
        )
        if review_id:
            review_row = repo_get_agent_review(review_id=int(review_id))
        if review_row:
            review_status = str(review_row["status"] or "").strip()
            review_summary = str(review_row["summary"] or "").strip()
            review_next_actions = str(review_row["next_actions"] or "").strip()
    except Exception as exc:
        safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.review_gate.ensure_failed",
            data={"error": str(exc)},
            level="warning",
        )
        review_id = None
        review_row = None
        review_status = ""
        review_summary = ""
        review_next_actions = ""

    normalized_review_status = str(review_status or "").strip().lower()
    if not (review_row and normalized_review_status and normalized_review_status != "pass"):
        return False

    try:
        from backend.src.agent.runner import review_repair

        repair_prompt = review_repair.build_review_repair_prompt(
            review_status=normalized_review_status,
            review_summary=review_summary,
            review_next_actions=review_next_actions,
        )
        repair_text, repair_err = review_repair.call_llm_for_text(
            llm_call,
            prompt=repair_prompt,
            task_id=int(task_id),
            run_id=int(run_id),
            model=model,
            parameters=react_params,
            variables={
                "source": f"{variables_source}_review_repair",
                "review_id": int(review_id) if review_id else None,
                "review_status": normalized_review_status,
            },
        )
        insert_steps = review_repair.parse_insert_steps_from_text(repair_text or "")
        if repair_err or not isinstance(insert_steps, list) or not insert_steps:
            raise ValueError(str(repair_err or "repair_output_invalid"))

        patch_err = apply_next_step_patch(
            current_step_index=int(idx),
            patch_obj={"step_index": int(idx) + 1, "insert_steps": insert_steps},
            plan_titles=plan_titles,
            plan_items=plan_items,
            plan_allows=plan_allows,
            plan_artifacts=plan_artifacts,
            max_steps=max_steps_limit,
        )
        if patch_err:
            raise ValueError(f"plan_patch_invalid:{patch_err}")

        safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.review_gate.repair_steps_inserted",
            data={
                "review_id": int(review_id) if review_id else None,
                "review_status": normalized_review_status,
                "insert_steps_count": len(insert_steps),
            },
            level="info",
        )
        yield sse_plan(task_id=task_id, plan_items=plan_items)
        try:
            updated_at = now_iso()
            update_task_run(
                run_id=int(run_id),
                agent_plan=build_agent_plan_payload(
                    plan_titles=plan_titles,
                    plan_items=plan_items,
                    plan_allows=plan_allows,
                    plan_artifacts=plan_artifacts,
                ),
                updated_at=updated_at,
            )
        except Exception as exc:
            safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.review_gate.plan.persist_failed",
                data={"error": str(exc)},
                level="warning",
            )
        return True
    except Exception as exc:
        safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.review_gate.repair_failed",
            data={"review_id": int(review_id) if review_id else None, "error": str(exc)},
            level="warning",
        )
        return False


def handle_task_feedback_step(
    *,
    task_id: int,
    run_id: int,
    idx: int,
    step_order: int,
    title: str,
    message: str,
    workdir: str,
    model: str,
    react_params: dict,
    variables_source: str,
    tools_hint: str,
    skills_hint: str,
    memories_hint: str,
    graph_hint: str,
    plan_titles: List[str],
    plan_items: List[dict],
    plan_allows: List[List[str]],
    plan_artifacts: List[str],
    agent_state: Dict,
    context: Dict,
    observations: List[str],
    max_steps_limit: Optional[int],
    run_replan_and_merge: Callable[..., Generator[dict, None, object]],
    safe_write_debug: Callable[..., None],
) -> Generator[str, None, TaskFeedbackOutcome]:
    """
    任务闭环：确认满意度（后端接管，不依赖模型猜测/前端硬编码）。
    """
    if not is_task_feedback_step_title(title):
        return TaskFeedbackOutcome()

    asked = bool(agent_state.get("task_feedback_asked"))

    # 1) 首次到达该步骤：提出满意度问题并暂停 run（waiting），等待用户回复
    if not asked:
        question = build_task_feedback_question()
        created_at = now_iso()

        if 0 <= idx < len(plan_items):
            plan_items[idx]["status"] = "waiting"
            yield sse_plan_delta(task_id=task_id, plan_items=plan_items, indices=[idx])

        try:
            create_task_output(
                task_id=int(task_id),
                run_id=int(run_id),
                output_type=TASK_OUTPUT_TYPE_USER_PROMPT,
                content=question,
                created_at=created_at,
            )
        except Exception as exc:
            safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.task_feedback.output_write_failed",
                data={"step_order": int(step_order), "error": str(exc)},
                level="warning",
            )

        safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.task_feedback.ask",
            data={"step_order": int(step_order)},
            level="info",
        )

        agent_state["paused"] = {
            "question": question,
            "step_order": step_order,
            "step_title": title,
            "created_at": created_at,
        }
        agent_state["step_order"] = step_order
        agent_state["task_feedback_asked"] = True

        updated_at = now_iso()
        try:
            update_task_run(
                run_id=int(run_id),
                status=RUN_STATUS_WAITING,
                agent_plan=build_agent_plan_payload(
                    plan_titles=plan_titles,
                    plan_items=plan_items,
                    plan_allows=plan_allows,
                    plan_artifacts=plan_artifacts,
                ),
                agent_state=agent_state,
                updated_at=updated_at,
            )
            update_task(task_id=int(task_id), status=STATUS_WAITING, updated_at=updated_at)
        except Exception as exc:
            safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.task_feedback.waiting_state.persist_failed",
                data={"step_order": int(step_order), "error": str(exc)},
                level="error",
            )

        yield sse_json(
            {
                "type": "need_input",
                "task_id": task_id,
                "run_id": run_id,
                "question": question,
                "kind": task_feedback_need_input_kind(),
            }
        )
        yield sse_json({"delta": f"{STREAM_TAG_ASK} {question}\n"})
        return TaskFeedbackOutcome(run_status=RUN_STATUS_WAITING)

    # 2) 用户已回复：根据满意度决定“结束 or 追加步骤继续”
    answer = str(agent_state.get("last_user_input") or "").strip()
    satisfied = is_positive_feedback(answer)

    # 当前步骤先标为 running，便于 UI 呈现“正在处理反馈”
    if 0 <= idx < len(plan_items):
        plan_items[idx]["status"] = "running"
        yield sse_plan_delta(task_id=task_id, plan_items=plan_items, indices=[idx])

    step_created_at = now_iso()
    detail = json.dumps(
        {"type": "task_feedback", "payload": {"answer": answer, "satisfied": satisfied}},
        ensure_ascii=False,
    )

    executor_value = None
    try:
        assignments = agent_state.get("executor_assignments") if isinstance(agent_state, dict) else None
        if isinstance(assignments, list):
            for a in assignments:
                if not isinstance(a, dict):
                    continue
                raw_order = a.get("step_order")
                try:
                    order_value = int(raw_order)
                except Exception:
                    continue
                if order_value != int(step_order):
                    continue
                ev = str(a.get("executor") or "").strip()
                executor_value = ev or None
                break
    except Exception:
        executor_value = None

    step_id, _created, _updated = create_task_step(
        TaskStepCreateParams(
            task_id=int(task_id),
            run_id=int(run_id),
            title=title,
            status=STEP_STATUS_RUNNING,
            executor=executor_value,
            detail=detail,
            result=None,
            error=None,
            attempts=1,
            started_at=step_created_at,
            finished_at=None,
            step_order=step_order,
            created_at=step_created_at,
            updated_at=step_created_at,
        )
    )

    finished_at = now_iso()
    try:
        result_value = json.dumps(
            {"satisfied": bool(satisfied), "answer": answer},
            ensure_ascii=False,
        )
    except Exception:
        result_value = json.dumps(
            {"satisfied": bool(satisfied), "answer": str(answer)},
            ensure_ascii=False,
        )
    mark_task_step_done(step_id=int(step_id), result=result_value, finished_at=finished_at)

    if 0 <= idx < len(plan_items):
        plan_items[idx]["status"] = "done"
        yield sse_plan_delta(task_id=task_id, plan_items=plan_items, indices=[idx])

    yield sse_json({"delta": f"{STREAM_TAG_OK} {title}\n"})

    observations.append(f"{title}: satisfied={bool(satisfied)} answer={_truncate_observation(answer)}")

    # 清理本次反馈输入（后续若继续会再次触发新的反馈步骤）
    agent_state["paused"] = None
    agent_state["task_feedback_asked"] = False
    agent_state["last_user_input"] = None
    agent_state["last_user_prompt"] = None
    agent_state["step_order"] = step_order + 1
    agent_state["observations"] = observations
    agent_state["context"] = context

    # 满意：run 结束（不再追加步骤）
    if satisfied:
        try:
            updated_at = now_iso()
            update_task_run(
                run_id=int(run_id),
                agent_plan=build_agent_plan_payload(
                    plan_titles=plan_titles,
                    plan_items=plan_items,
                    plan_allows=plan_allows,
                    plan_artifacts=plan_artifacts,
                ),
                agent_state=agent_state,
                updated_at=updated_at,
            )
        except Exception as exc:
            safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.task_feedback.state.persist_failed",
                data={"where": "satisfied", "step_order": int(step_order), "error": str(exc)},
                level="warning",
            )
        return TaskFeedbackOutcome(run_status=RUN_STATUS_DONE)

    # 不满意：进入 Replan（重新规划剩余步骤），避免反复 insert_steps 造成冗余
    try:
        replan_attempts = int(agent_state.get("replan_attempts") or 0)
    except Exception:
        replan_attempts = 0
    done_count = int(step_order)
    remaining_limit = None
    if isinstance(max_steps_limit, int) and max_steps_limit > 0:
        remaining_limit = max_steps_limit - done_count - 1  # 预留 1 步给下一轮确认满意度

    if remaining_limit is not None and remaining_limit <= 0:
        yield sse_json(
            {
                "delta": f"{STREAM_TAG_FAIL} 已收到反馈，但剩余步数不足以继续（max_steps={max_steps_limit}）。\n"
            }
        )
        return TaskFeedbackOutcome(run_status=RUN_STATUS_FAILED)

    if replan_attempts >= int(AGENT_REACT_REPLAN_MAX_ATTEMPTS or 0):
        yield sse_json({"delta": f"{STREAM_TAG_FAIL} 已收到反馈，但重新规划次数已达上限。\n"})
        return TaskFeedbackOutcome(run_status=RUN_STATUS_FAILED)

    max_steps_value = int(remaining_limit) if remaining_limit is not None else int(max_steps_limit or len(plan_titles))
    replan_result = yield from run_replan_and_merge(
        task_id=int(task_id),
        run_id=int(run_id),
        message=message,
        workdir=workdir,
        model=model,
        react_params=react_params,
        max_steps_value=max_steps_value,
        tools_hint=tools_hint,
        skills_hint=skills_hint,
        memories_hint=memories_hint,
        graph_hint=graph_hint,
        plan_titles=plan_titles,
        plan_items=plan_items,
        plan_allows=plan_allows,
        plan_artifacts=plan_artifacts,
        agent_state=agent_state,
        observations=observations,
        done_count=done_count,
        error=str(answer or "user_not_satisfied"),
        sse_notice=f"{STREAM_TAG_EXEC} 已收到反馈，重新规划剩余步骤…",
        replan_attempts=replan_attempts,
        safe_write_debug=safe_write_debug,
    )

    if not replan_result:
        yield sse_json({"delta": f"{STREAM_TAG_FAIL} 重新规划失败，无法继续。\\n"})
        return TaskFeedbackOutcome(run_status=RUN_STATUS_FAILED)

    # replan_result 结构由上层提供；这里按约定读取字段（避免强耦合具体类型）
    plan_titles = getattr(replan_result, "plan_titles", plan_titles)
    plan_allows = getattr(replan_result, "plan_allows", plan_allows)
    plan_items = getattr(replan_result, "plan_items", plan_items)
    plan_artifacts = getattr(replan_result, "plan_artifacts", plan_artifacts)

    append_task_feedback_step(
        plan_titles=plan_titles,
        plan_items=plan_items,
        plan_allows=plan_allows,
        max_steps=int(max_steps_limit) if isinstance(max_steps_limit, int) else None,
    )

    yield sse_plan(task_id=task_id, plan_items=plan_items)
    try:
        updated_at = now_iso()
        update_task_run(
            run_id=int(run_id),
            agent_plan=build_agent_plan_payload(
                plan_titles=plan_titles,
                plan_items=plan_items,
                plan_allows=plan_allows,
                plan_artifacts=plan_artifacts,
            ),
            agent_state=agent_state,
            updated_at=updated_at,
        )
    except Exception as exc:
        safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.task_feedback.plan.persist_failed",
            data={"where": "feedback_replan", "error": str(exc)},
            level="warning",
        )

    return TaskFeedbackOutcome(
        next_idx=done_count,
        plan_titles=plan_titles,
        plan_items=plan_items,
        plan_allows=plan_allows,
        plan_artifacts=plan_artifacts,
    )
