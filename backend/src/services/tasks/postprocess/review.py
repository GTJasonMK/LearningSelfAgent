from __future__ import annotations

from threading import Lock
from typing import Callable, List, Optional, Tuple

from backend.src.common.utils import extract_json_object, truncate_text
from backend.src.repositories.agent_reviews_repo import (
    create_agent_review_record,
    get_latest_agent_review_id_for_run,
    update_agent_review_record,
)
from backend.src.repositories.task_runs_repo import get_task_run
from backend.src.storage import get_connection
from backend.src.services.tasks.postprocess.review_data import collect_review_data
from backend.src.services.tasks.postprocess.review_decision import evaluate_review_decision
from backend.src.services.tasks.postprocess.review_prompt import (
    build_review_prompt_text,
    resolve_review_model,
)
from backend.src.services.tasks.postprocess.review_tool_approval import (
    approve_tools_after_review,
    ensure_existing_review_tool_approval,
)


def ensure_agent_review_record_core(
    *,
    task_id: int,
    run_id: int,
    skills: Optional[list] = None,
    force: bool = False,
    review_record_lock: Lock,
    allow_tool_approval_on_waiting_feedback_fn: Callable[[Optional[dict]], bool],
    is_selftest_title_fn: Callable[[str], bool],
    extract_tool_name_from_tool_call_step_fn: Callable[[str, object], str],
    find_unverified_text_output_fn: Callable[[List[dict]], Optional[dict]],
    call_openai_fn: Callable[[str, str, dict], Tuple[str, object, Optional[str]]],
    safe_write_debug_fn: Callable[..., None],
) -> Optional[int]:
    _REVIEW_RECORD_LOCK = review_record_lock
    _allow_tool_approval_on_waiting_feedback = allow_tool_approval_on_waiting_feedback_fn
    _is_selftest_title = is_selftest_title_fn
    _extract_tool_name_from_tool_call_step = extract_tool_name_from_tool_call_step_fn
    _find_unverified_text_output = find_unverified_text_output_fn
    _safe_write_debug = safe_write_debug_fn
    call_openai = call_openai_fn

    rid = int(run_id)
    tid = int(task_id)
    if rid <= 0 or tid <= 0:
        return None

    run_row = None
    try:
        run_row = get_task_run(run_id=rid)
    except Exception:
        run_row = None
    if not run_row:
        return None

    run_summary = str(run_row["summary"] or "").strip()
    if not run_summary.startswith("agent_"):
        return None

    # 快速路径去重（无锁）：大部分情况下记录已存在，直接返回，避免进入耗时逻辑。
    with get_connection() as conn:
        existing_row = conn.execute(
            "SELECT id FROM agent_review_records WHERE run_id = ? ORDER BY id DESC LIMIT 1",
            (rid,),
        ).fetchone()
        existing = int(existing_row["id"]) if existing_row and existing_row["id"] is not None else None

    if existing and not bool(force):
        ensure_existing_review_tool_approval(
            task_id=tid,
            run_id=rid,
            existing_review_id=int(existing),
            run_row=run_row,
            allow_tool_approval_on_waiting_feedback_fn=_allow_tool_approval_on_waiting_feedback,
            safe_write_debug_fn=_safe_write_debug,
        )
        return int(existing)

    skill_items = skills if isinstance(skills, list) else []

    review_data = collect_review_data(
        task_id=tid,
        run_id=rid,
        run_row=run_row,
        is_selftest_title_fn=_is_selftest_title,
        extract_tool_name_from_tool_call_step_fn=_extract_tool_name_from_tool_call_step,
    )
    step_rows = review_data["step_rows"]
    output_rows = review_data["output_rows"]
    tool_rows = review_data["tool_rows"]
    plan_compact = review_data["plan_compact"]
    steps_compact = review_data["steps_compact"]
    outputs_compact = review_data["outputs_compact"]
    tools_compact = review_data["tools_compact"]
    plan_artifacts = review_data["plan_artifacts"]
    artifacts_check_items = review_data["artifacts_check_items"]
    auto_status = review_data["auto_status"]
    auto_summary = review_data["auto_summary"]
    auto_issues = review_data["auto_issues"]
    auto_next_actions = review_data["auto_next_actions"]
    task_title = review_data["task_title"]
    state_obj = review_data["state_obj"]
    mode = review_data["mode"]
    run_meta = review_data["run_meta"]

    # 原子性 check+insert：使用线程锁防止并发线程同时通过 check 后各自创建重复记录。
    with _REVIEW_RECORD_LOCK:
        if not bool(force):
            recheck = get_latest_agent_review_id_for_run(run_id=rid)
            if recheck:
                return int(recheck)

        review_id = create_agent_review_record(
            task_id=tid,
            run_id=rid,
            status="running",
            summary="评估中：读取记录…",
            issues=[],
            next_actions=[],
            skills=skill_items,
        )

    _safe_write_debug(
        tid,
        rid,
        message="agent.review.started",
        data={"review_id": int(review_id)},
        level="info",
    )

    if auto_status:
        update_agent_review_record(
            review_id=int(review_id),
            status=auto_status,
            summary=auto_summary,
            issues=auto_issues,
            next_actions=auto_next_actions,
            skills=skill_items,
        )
        _safe_write_debug(
            tid,
            rid,
            message="agent.review.auto_fail",
            data={"review_id": int(review_id), "status": auto_status},
            level="info",
        )
        return int(review_id)

    try:
        prompt_text = build_review_prompt_text(
            task_title=task_title,
            run_meta=run_meta,
            plan_compact=plan_compact,
            steps_compact=steps_compact,
            outputs_compact=outputs_compact,
            tools_compact=tools_compact,
        )

        # 进度：评估分析（LLM 审查）
        update_agent_review_record(
            review_id=int(review_id),
            status="running",
            summary="评估中：评估分析…",
        )

        model = resolve_review_model(mode=str(mode or ""), state_obj=state_obj if isinstance(state_obj, dict) else None)
        parameters = {"temperature": 0}
        text, _, err = call_openai(prompt_text, model, parameters)
        obj = extract_json_object(text or "") if not err else None

        # 进度：落库沉淀（生成评估记录/建议）
        update_agent_review_record(
            review_id=int(review_id),
            status="running",
            summary="评估中：落库沉淀…",
        )

        decision = evaluate_review_decision(
            obj=obj,
            err=err,
            raw_text=str(text or ""),
            step_rows=step_rows,
            output_rows=output_rows,
            tool_rows=tool_rows,
            plan_artifacts=plan_artifacts,
            artifacts_check_items=artifacts_check_items,
            find_unverified_text_output_fn=_find_unverified_text_output,
        )

        status = decision["status"]
        summary = decision["summary"]
        issues = decision["issues"]
        next_actions = decision["next_actions"]
        pass_score = decision["pass_score"]
        pass_threshold = decision["pass_threshold"]
        distill_status = decision["distill_status"]
        distill_score = decision["distill_score"]
        distill_threshold = decision["distill_threshold"]
        distill_notes = decision["distill_notes"]
        distill_evidence_refs = decision["distill_evidence_refs"]

        update_agent_review_record(
            review_id=int(review_id),
            status=status,
            pass_score=pass_score,
            pass_threshold=pass_threshold,
            distill_status=distill_status,
            distill_score=distill_score,
            distill_threshold=distill_threshold,
            distill_notes=distill_notes,
            distill_evidence_refs=distill_evidence_refs,
            summary=summary,
            issues=issues,
            next_actions=next_actions,
            skills=skill_items,
        )
        _safe_write_debug(
            tid,
            rid,
            message="agent.review.updated",
            data={
                "review_id": int(review_id),
                "status": status,
                "pass_score": pass_score,
                "pass_threshold": pass_threshold,
                "distill_status": distill_status,
                "distill_score": distill_score,
                "distill_threshold": distill_threshold,
            },
            level="info",
        )

        approve_tools_after_review(
            task_id=tid,
            run_id=rid,
            review_id=int(review_id),
            review_status=str(status or ""),
            distill_status=str(distill_status or ""),
            run_row=run_row,
            allow_tool_approval_on_waiting_feedback_fn=_allow_tool_approval_on_waiting_feedback,
            safe_write_debug_fn=_safe_write_debug,
        )
    except Exception as exc:
        update_agent_review_record(
            review_id=int(review_id),
            status="fail",
            summary="评估异常：请查看 debug 输出",
            issues=[
                {
                    "title": "评估异常",
                    "severity": "high",
                    "details": "后处理阶段触发 Eval Agent 时发生异常。",
                    "evidence": truncate_text(str(exc), 260),
                    "suggestion": "查看 records/debug（task_outputs）中的 agent.review.* 相关日志。",
                }
            ],
            next_actions=[{"title": "修复评估链路", "details": "检查后端日志与 LLM 配置。"}],
            skills=skill_items,
        )
        _safe_write_debug(
            tid,
            rid,
            message="agent.review.failed",
            data={"review_id": int(review_id), "error": str(exc)},
            level="warning",
        )

    return int(review_id)
