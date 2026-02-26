import json
from dataclasses import dataclass
from typing import Callable, Dict, Generator, List, Optional

from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.contracts.stream_events import build_need_input_payload, generate_prompt_token
from backend.src.agent.support import _truncate_observation, apply_next_step_patch
from backend.src.agent.runner.feedback import (
    append_task_feedback_step,
    build_task_feedback_question,
    is_positive_feedback,
    is_task_feedback_step_title,
    task_feedback_need_input_kind,
)
from backend.src.agent.runner.plan_events import sse_plan, sse_plan_delta
from backend.src.agent.runner.react_state_manager import resolve_executor
from backend.src.common.utils import coerce_int, now_iso, parse_optional_int
from backend.src.constants import (
    AGENT_MAX_STEPS_UNLIMITED,
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
from backend.src.services.agent_review.review_records import get_agent_review
from backend.src.services.llm.llm_client import sse_json
from backend.src.services.tasks.task_queries import (
    create_task_output,
    TaskStepCreateParams,
    create_task_step,
    mark_task_step_done,
    update_task,
    update_task_run,
)


@dataclass
class TaskFeedbackOutcome:
    """
    处理"确认满意度"步骤后的控制结果：
    - run_status != None：外层应 break 并以该状态结束本次生成器；
    - next_idx != None：外层应把 idx 设为 next_idx 并 continue；
    - plan_changed: 计划是否被修改（replan 后），外层通过 plan_struct 直接读取最新状态。
    """

    run_status: Optional[str] = None
    next_idx: Optional[int] = None
    plan_changed: bool = False


def _read_attempt_counter(agent_state: Dict, key: str) -> int:
    if not isinstance(agent_state, dict):
        return 0
    return coerce_int(agent_state.get(key) or 0, default=0)


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
    plan_struct: PlanStructure,
    max_steps_limit: Optional[int],
    agent_state: Dict,
    safe_write_debug: Callable[..., None],
) -> Generator[str, None, bool]:
    """
    评估门闩（在"确认满意度"之前）：
    - 若评估未通过，则在"确认满意度"之前插入修复步骤并继续执行（不立刻进入 waiting）；
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
        from backend.src.services.tasks.task_postprocess import ensure_agent_review_record

        yield sse_json({"delta": f"{STREAM_TAG_EXEC} 评估任务完成度…\n"})
        review_id = ensure_agent_review_record(
            task_id=int(task_id),
            run_id=int(run_id),
            skills=[],
            force=True,
        )
        if review_id:
            review_row = get_agent_review(review_id=int(review_id))
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
        if isinstance(agent_state, dict):
            agent_state["review_gate_attempts"] = 0
        return False

    review_gate_attempts = _read_attempt_counter(agent_state, "review_gate_attempts")

    repair_budget = coerce_int(AGENT_REACT_REPLAN_MAX_ATTEMPTS or 0, default=0)
    if repair_budget > 0 and review_gate_attempts >= repair_budget:
        safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.review_gate.repair_budget_exhausted",
            data={
                "review_id": int(review_id) if review_id else None,
                "review_status": normalized_review_status,
                "attempts": int(review_gate_attempts),
                "budget": int(repair_budget),
            },
            level="warning",
        )
        yield sse_json({"delta": f"{STREAM_TAG_EXEC} 评估修复已达预算，先进入反馈确认。\n"})
        return False

    try:
        from backend.src.agent.runner import review_repair

        repair_prompt = review_repair.build_review_repair_prompt(
            review_status=normalized_review_status,
            review_summary=review_summary,
            review_next_actions=review_next_actions,
        )
        deliberation_text, deliberation_err = review_repair.call_llm_for_text(
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
        decision = review_repair.parse_review_gate_decision_from_text(deliberation_text or "")

        safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.review_gate.deliberation_decision",
            data={
                "review_id": int(review_id) if review_id else None,
                "review_status": normalized_review_status,
                "decision": decision.decision,
                "reasons": decision.reasons,
                "evidence": decision.evidence,
                "parse_error": decision.parse_error,
            },
            level="info",
        )

        if deliberation_err and not str(deliberation_text or "").strip():
            safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.review_gate.deliberation_failed",
                data={
                    "review_id": int(review_id) if review_id else None,
                    "error": str(deliberation_err),
                },
                level="warning",
            )
            return False

        if decision.parse_error:
            safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.review_gate.deliberation_parse_warning",
                data={
                    "review_id": int(review_id) if review_id else None,
                    "decision": decision.decision,
                    "parse_error": decision.parse_error,
                },
                level="warning",
            )

        if decision.decision != review_repair.REVIEW_GATE_DECISION_REPAIR:
            if isinstance(agent_state, dict):
                agent_state["review_gate_attempts"] = 0
            if decision.decision == review_repair.REVIEW_GATE_DECISION_FINALIZE_WITH_RISK:
                yield sse_json({"delta": f"{STREAM_TAG_EXEC} 评估建议先保留风险并进入反馈。\n"})
            elif decision.decision == review_repair.REVIEW_GATE_DECISION_ASK_USER:
                yield sse_json({"delta": f"{STREAM_TAG_EXEC} 评估建议先向用户确认，再决定是否修复。\n"})
            else:
                yield sse_json({"delta": f"{STREAM_TAG_EXEC} 评估建议先进入反馈，暂不插入修复步骤。\n"})
            return False

        insert_steps = decision.insert_steps
        if not isinstance(insert_steps, list) or not insert_steps:
            safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.review_gate.repair_steps_missing",
                data={
                    "review_id": int(review_id) if review_id else None,
                    "decision": decision.decision,
                    "parse_error": decision.parse_error,
                },
                level="warning",
            )
            return False

        # apply_next_step_patch 仍使用 legacy 列表接口（后续可进一步收编到 PlanStructure）
        plan_titles, plan_items, plan_allows, plan_artifacts = plan_struct.to_legacy_lists()
        patch_obj = {"step_index": int(idx) + 1, "insert_steps": insert_steps}
        patch_err = apply_next_step_patch(
            current_step_index=int(idx),
            patch_obj=patch_obj,
            plan_titles=plan_titles,
            plan_items=plan_items,
            plan_allows=plan_allows,
            plan_artifacts=plan_artifacts,
            max_steps=max_steps_limit,
        )

        if patch_err and "超出 max_steps" in str(patch_err):
            # 修复器可能返回多条步骤；当超过 max_steps 时自动裁剪，避免整次修复直接失败。
            allowed_inserts = 0
            if isinstance(max_steps_limit, int) and max_steps_limit > 0:
                allowed_inserts = max(0, int(max_steps_limit) - len(plan_titles))
            if allowed_inserts > 0:
                original_count = len(insert_steps)
                trimmed_insert_steps = list(insert_steps)[:allowed_inserts]
                patch_obj = {"step_index": int(idx) + 1, "insert_steps": trimmed_insert_steps}
                patch_err = apply_next_step_patch(
                    current_step_index=int(idx),
                    patch_obj=patch_obj,
                    plan_titles=plan_titles,
                    plan_items=plan_items,
                    plan_allows=plan_allows,
                    plan_artifacts=plan_artifacts,
                    max_steps=max_steps_limit,
                )
                if not patch_err:
                    insert_steps = trimmed_insert_steps
                    safe_write_debug(
                        task_id=int(task_id),
                        run_id=int(run_id),
                        message="agent.review_gate.repair_steps_trimmed",
                        data={
                            "review_id": int(review_id) if review_id else None,
                            "original_count": int(original_count),
                            "trimmed_count": len(insert_steps),
                            "max_steps": int(max_steps_limit),
                        },
                        level="info",
                    )
                else:
                    # 裁剪后仍失败：容量不足，跳过修复步骤插入
                    safe_write_debug(
                        task_id=int(task_id),
                        run_id=int(run_id),
                        message="agent.review_gate.repair_trimmed_still_failed",
                        data={
                            "review_id": int(review_id) if review_id else None,
                            "allowed_inserts": int(allowed_inserts),
                            "patch_err": str(patch_err),
                        },
                        level="info",
                    )
                    return False

        if patch_err and "超出 max_steps" in str(patch_err):
            reached_limit = False
            if isinstance(max_steps_limit, int) and max_steps_limit > 0:
                reached_limit = len(plan_titles) >= int(max_steps_limit)
            if reached_limit:
                safe_write_debug(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    message="agent.review_gate.repair_skipped_max_steps",
                    data={
                        "review_id": int(review_id) if review_id else None,
                        "plan_len": len(plan_titles),
                        "max_steps": int(max_steps_limit),
                    },
                    level="info",
                )
                return False
        if patch_err:
            raise ValueError(f"plan_patch_invalid:{patch_err}")

        # 回写到 plan_struct
        patched_plan = PlanStructure.from_legacy(
            plan_titles=plan_titles,
            plan_items=plan_items,
            plan_allows=plan_allows,
            plan_artifacts=plan_artifacts,
        )
        plan_struct.replace_from(patched_plan)

        safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.review_gate.repair_steps_inserted",
            data={
                "review_id": int(review_id) if review_id else None,
                "review_status": normalized_review_status,
                "insert_steps_count": len(insert_steps),
                "decision_reasons": decision.reasons,
            },
            level="info",
        )
        if isinstance(agent_state, dict):
            agent_state["review_gate_attempts"] = review_gate_attempts + 1

        yield sse_plan(task_id=task_id, run_id=run_id, plan_items=plan_struct.get_items_payload())
        try:
            updated_at = now_iso()
            update_task_run(
                run_id=int(run_id),
                agent_plan=plan_struct.to_agent_plan_payload(),
                agent_state=agent_state,
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
    plan_struct: PlanStructure,
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
        choices = [{"label": "是", "value": "是"}, {"label": "否", "value": "否"}]
        created_at = now_iso()

        plan_struct.set_step_status(idx, "waiting")
        yield sse_plan_delta(task_id=task_id, run_id=run_id, plan_items=plan_struct.get_items_payload(), indices=[idx])

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
            "kind": task_feedback_need_input_kind(),
            "choices": choices,
        }
        agent_state["paused"]["prompt_token"] = generate_prompt_token(
            task_id=int(task_id),
            run_id=int(run_id),
            step_order=int(step_order),
            question=question,
            created_at=created_at,
        )
        agent_state["step_order"] = step_order
        agent_state["task_feedback_asked"] = True

        updated_at = now_iso()
        try:
            update_task_run(
                run_id=int(run_id),
                status=RUN_STATUS_WAITING,
                agent_plan=plan_struct.to_agent_plan_payload(),
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
            build_need_input_payload(
                task_id=int(task_id),
                run_id=int(run_id),
                question=question,
                kind=task_feedback_need_input_kind(),
                choices=choices,
                prompt_token=agent_state.get("paused", {}).get("prompt_token")
                if isinstance(agent_state.get("paused"), dict)
                else None,
                session_key=str(agent_state.get("session_key") or "") if isinstance(agent_state, dict) else "",
            )
        )
        yield sse_json({"delta": f"{STREAM_TAG_ASK} {question}\n"})
        return TaskFeedbackOutcome(run_status=RUN_STATUS_WAITING)

    # 2) 用户已回复：根据满意度决定"结束 or 追加步骤继续"
    answer = str(agent_state.get("last_user_input") or "").strip()
    satisfied = is_positive_feedback(answer)

    # 当前步骤先标为 running，便于 UI 呈现"正在处理反馈"
    plan_struct.set_step_status(idx, "running")
    yield sse_plan_delta(task_id=task_id, run_id=run_id, plan_items=plan_struct.get_items_payload(), indices=[idx])

    step_created_at = now_iso()
    detail = json.dumps(
        {"type": "task_feedback", "payload": {"answer": answer, "satisfied": satisfied}},
        ensure_ascii=False,
    )

    executor_value = resolve_executor(agent_state, step_order)

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
    result_value = json.dumps(
        {"satisfied": bool(satisfied), "answer": str(answer)},
        ensure_ascii=False,
    )
    mark_task_step_done(step_id=int(step_id), result=result_value, finished_at=finished_at)

    plan_struct.set_step_status(idx, "done")
    yield sse_plan_delta(task_id=task_id, run_id=run_id, plan_items=plan_struct.get_items_payload(), indices=[idx])

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
        persist_ok = True
        try:
            updated_at = now_iso()
            update_task_run(
                run_id=int(run_id),
                agent_plan=plan_struct.to_agent_plan_payload(),
                agent_state=agent_state,
                updated_at=updated_at,
            )
        except Exception as exc:
            persist_ok = False
            safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.task_feedback.state.persist_failed",
                data={"where": "satisfied", "step_order": int(step_order), "error": str(exc)},
                level="error",
            )
        if not persist_ok:
            # 持久化失败时不能返回 DONE，否则 DB 中 run 状态可能仍为 WAITING，导致前后端状态分裂。
            return TaskFeedbackOutcome(run_status=RUN_STATUS_FAILED)
        return TaskFeedbackOutcome(run_status=RUN_STATUS_DONE)

    # 不满意：进入 Replan（重新规划剩余步骤），避免反复 insert_steps 造成冗余
    replan_attempts = _read_attempt_counter(agent_state, "replan_attempts")
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

    replan_budget = coerce_int(AGENT_REACT_REPLAN_MAX_ATTEMPTS or 0, default=0)
    if replan_attempts >= replan_budget:
        yield sse_json({"delta": f"{STREAM_TAG_FAIL} 已收到反馈，但重新规划次数已达上限。\n"})
        return TaskFeedbackOutcome(run_status=RUN_STATUS_FAILED)

    if remaining_limit is not None:
        max_steps_value = int(remaining_limit)
    elif isinstance(max_steps_limit, int) and max_steps_limit > 0:
        max_steps_value = int(max_steps_limit)
    else:
        max_steps_value = int(AGENT_MAX_STEPS_UNLIMITED)
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
        plan_struct=plan_struct,
        agent_state=agent_state,
        observations=observations,
        done_count=done_count,
        error=str(answer or "user_not_satisfied"),
        sse_notice=f"{STREAM_TAG_EXEC} 已收到反馈，重新规划剩余步骤…",
        replan_attempts=replan_attempts,
        safe_write_debug=safe_write_debug,
    )

    if not replan_result:
        yield sse_json({"delta": f"{STREAM_TAG_FAIL} 重新规划失败，无法继续。\n"})
        return TaskFeedbackOutcome(run_status=RUN_STATUS_FAILED)

    plan_struct.replace_from(replan_result.plan_struct)

    # replan 后检查是否已含 feedback 步骤（run_replan_and_merge 内部可能已追加），避免重复插入。
    new_titles, new_items, new_allows, new_artifacts = plan_struct.to_legacy_lists()
    has_feedback_already = any(is_task_feedback_step_title(t) for t in new_titles)
    if not has_feedback_already:
        append_task_feedback_step(
            plan_titles=new_titles,
            plan_items=new_items,
            plan_allows=new_allows,
            max_steps=parse_optional_int(max_steps_limit, default=None),
        )
    feedback_plan = PlanStructure.from_legacy(
        plan_titles=new_titles,
        plan_items=new_items,
        plan_allows=new_allows,
        plan_artifacts=new_artifacts,
    )
    plan_struct.replace_from(feedback_plan)

    yield sse_plan(task_id=task_id, run_id=run_id, plan_items=plan_struct.get_items_payload())
    try:
        updated_at = now_iso()
        update_task_run(
            run_id=int(run_id),
            agent_plan=plan_struct.to_agent_plan_payload(),
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
        plan_changed=True,
    )
