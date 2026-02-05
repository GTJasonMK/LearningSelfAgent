import logging
import json
import os
from typing import List, Optional, Tuple

from backend.src.common.utils import extract_json_object, now_iso, parse_json_list, truncate_text
from backend.src.constants import (
    ACTION_TYPE_FILE_WRITE,
    ACTION_TYPE_SHELL_COMMAND,
    ACTION_TYPE_TOOL_CALL,
    DEFAULT_MEMORY_TYPE,
    EVAL_PASS_RATE_THRESHOLD,
    MEMORY_AUTO_TASK_RESULT_MAX_CHARS,
    MEMORY_TAG_AUTO,
    MEMORY_TAG_TASK_RESULT,
    RUN_STATUS_WAITING,
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_STOPPED,
    STREAM_TAG_RESULT,
    TASK_OUTPUT_TYPE_DEBUG,
    SKILL_CATEGORY_CHOICES,
    AGENT_TASK_FEEDBACK_STEP_TITLE,
    AGENT_REVIEW_PASS_SCORE_THRESHOLD,
    AGENT_REVIEW_DISTILL_SCORE_THRESHOLD,
    AGENT_REVIEW_DISTILL_STATUS_ALLOW,
    AGENT_REVIEW_DISTILL_STATUS_DENY,
    AGENT_REVIEW_DISTILL_STATUS_MANUAL,
)
from backend.src.prompt.system_prompts import load_system_prompt
from backend.src.repositories.agent_reviews_repo import (
    create_agent_review_record,
    get_latest_agent_review_id_for_run,
    update_agent_review_record,
)
from backend.src.repositories.eval_repo import create_eval_criteria_bulk, create_eval_record
from backend.src.repositories.expectations_repo import get_expectation as get_expectation_repo
from backend.src.repositories.memory_repo import (
    find_memory_item_id_by_task_and_tag_like,
)
from backend.src.repositories.task_outputs_repo import list_task_outputs_for_run
from backend.src.repositories.task_runs_repo import get_task_run, list_agent_runs_missing_reviews
from backend.src.repositories.task_steps_repo import list_task_steps_for_run
from backend.src.repositories.tasks_repo import get_task
from backend.src.repositories.tool_call_records_repo import list_tool_calls_with_tool_name_by_run
from backend.src.storage import get_connection
from backend.src.services.debug.debug_output import write_task_debug_output
from backend.src.services.graph.graph_extract import extract_graph_updates
from backend.src.services.llm.llm_client import call_openai, resolve_default_model
from backend.src.services.memory.memory_items import create_memory_item as create_memory_item_service

logger = logging.getLogger(__name__)


def _json_preview(value, max_chars: int) -> str:
    """
    评估 prompt 的体积控制：把复杂对象压缩为可读片段，避免塞爆上下文。
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return truncate_text(value, max_chars)
    try:
        return truncate_text(json.dumps(value, ensure_ascii=False), max_chars)
    except Exception:
        return truncate_text(str(value), max_chars)


def _safe_write_debug(
    task_id: int,
    run_id: int,
    *,
    message: str,
    data: Optional[dict] = None,
    level: str = "debug",
) -> None:
    """
    后处理链路的调试输出不应影响主链路：失败时降级为 logger.exception。
    """
    try:
        write_task_debug_output(
            task_id=int(task_id),
            run_id=int(run_id),
            message=message,
            data=data if isinstance(data, dict) else None,
            level=level,
        )
    except Exception:
        logger.exception("write_task_debug_output failed: %s", message)


def ensure_agent_review_record(
    *,
    task_id: int,
    run_id: int,
    skills: Optional[list] = None,
    force: bool = False,
) -> Optional[int]:
    """
    确保某个 Agent run 至少有一条评估记录（agent_review_records）。

    设计目标：
    - “可观察性”优先：即使 LLM 配置缺失/调用失败，也要落库一条 fail 记录，避免前端误以为“评估没触发”。；
    - 先插入 running 占位，再更新最终结果：即使进程重启/线程被 kill，也能看到评估已启动；
    - 默认每个 run 只自动评估一次（存在记录则直接返回）；force=True 时会强制追加一条新评估记录。
    """
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

    existing = get_latest_agent_review_id_for_run(run_id=rid)
    if existing and not bool(force):
        # 已存在评估记录：默认不重复调用 Eval LLM（避免评估风暴）。
        # 但“工具批准/技能沉淀”需要 run 进入 done/waiting（确认满意度）后才能安全执行，
        # 因此这里做一次幂等的闭环补齐：若 review 已 pass 且 run 状态满足，则尝试批准 draft 工具。
        try:
            from backend.src.repositories.agent_reviews_repo import get_agent_review as repo_get_agent_review
            from backend.src.services.tools.tool_approval import approve_draft_tools_from_run

            review_row = repo_get_agent_review(review_id=int(existing))
            review_status = str(review_row["status"] or "").strip() if review_row else ""
            review_distill_status = str(review_row["distill_status"] or "").strip().lower() if review_row else ""
            if not review_distill_status:
                review_distill_status = ""

            allow_waiting_feedback = False
            try:
                run_status_value = str(run_row["status"] or "").strip() if run_row else ""
                if run_status_value == RUN_STATUS_WAITING:
                    state_obj = extract_json_object(run_row["agent_state"] or "") if run_row else None
                    paused = state_obj.get("paused") if isinstance(state_obj, dict) else None
                    step_title = str(paused.get("step_title") or "").strip() if isinstance(paused, dict) else ""
                    allow_waiting_feedback = step_title == AGENT_TASK_FEEDBACK_STEP_TITLE
            except Exception:
                allow_waiting_feedback = False

            approve_draft_tools_from_run(
                task_id=int(tid),
                run_id=int(rid),
                run_status=str(run_row["status"] or "") if run_row else "",
                review_id=int(existing),
                review_status=str(review_status or ""),
                distill_status=str(review_distill_status or "") if review_distill_status else None,
                allow_waiting_feedback=bool(allow_waiting_feedback),
                model=None,
            )
        except Exception as exc:
            _safe_write_debug(
                tid,
                rid,
                message="tool.approval.ensure_failed",
                data={"review_id": int(existing), "error": str(exc)},
                level="warning",
            )

        return int(existing)

    skill_items = skills if isinstance(skills, list) else []

    # 采样：复用已有的 steps/outputs/tool_calls 作为证据输入（体积受控）
    step_rows = list_task_steps_for_run(task_id=tid, run_id=rid)
    output_rows = list_task_outputs_for_run(task_id=tid, run_id=rid, order="ASC")
    tool_rows = list_tool_calls_with_tool_name_by_run(run_id=rid, limit=50)

    plan_obj = extract_json_object(run_row["agent_plan"] or "") if run_row else None
    if not isinstance(plan_obj, dict):
        plan_obj = {}
    plan_compact = {
        "titles": plan_obj.get("titles"),
        "allows": plan_obj.get("allows"),
        "artifacts": plan_obj.get("artifacts"),
    }

    steps_compact: List[dict] = []
    failed_steps: List[dict] = []
    failed_selftest = False
    failed_exec_step = False
    for row in step_rows:
        action_type = None
        payload_preview = None
        try:
            detail_obj = json.loads(row["detail"]) if row["detail"] else None
            if isinstance(detail_obj, dict):
                action_type = detail_obj.get("type")
                payload_preview = detail_obj.get("payload")
        except Exception:
            action_type = None
            payload_preview = None
        status_value = str(row["status"] or "").strip()
        title_value = str(row["title"] or "").strip()
        if status_value == "failed":
            failed_steps.append(
                {
                    "step_order": row["step_order"],
                    "title": title_value,
                    "action_type": action_type,
                    "error": truncate_text(str(row["error"] or ""), 260),
                }
            )
            if "自测" in title_value or "selftest" in title_value.lower() or "self-test" in title_value.lower():
                failed_selftest = True
            if action_type in {ACTION_TYPE_TOOL_CALL, ACTION_TYPE_SHELL_COMMAND, ACTION_TYPE_FILE_WRITE}:
                failed_exec_step = True
        steps_compact.append(
            {
                "step_id": row["id"],
                "step_order": row["step_order"],
                "title": title_value,
                "status": status_value,
                "action_type": action_type,
                "payload_preview": _json_preview(payload_preview, 360),
                "result_preview": _json_preview(row["result"], 520),
                "error_preview": truncate_text(str(row["error"] or ""), 260),
            }
        )
        if len(steps_compact) >= 80:
            break

    outputs_compact: List[dict] = []
    for row in output_rows:
        outputs_compact.append(
            {
                "output_id": row["id"],
                "type": row["output_type"],
                "content_preview": truncate_text(str(row["content"] or ""), 620),
                "created_at": row["created_at"],
            }
        )
        if len(outputs_compact) >= 40:
            break

    tools_compact: List[dict] = []
    for row in tool_rows:
        tools_compact.append(
            {
                "tool_call_record_id": row["id"],
                "tool_id": row["tool_id"],
                "tool_name": row["tool_name"],
                "reuse": bool(row["reuse"]),
                "reuse_status": row["reuse_status"],
                "input": truncate_text(str(row["input"] or ""), 360),
                "output": truncate_text(str(row["output"] or ""), 520),
                "created_at": row["created_at"],
            }
        )

    plan_artifacts = plan_obj.get("artifacts") if isinstance(plan_obj.get("artifacts"), list) else []
    artifacts_check_workdir = ""
    artifacts_check_items: List[dict] = []
    missing_artifacts: List[str] = []

    # 自动失败判定（避免“评估通过但事实未完成”）
    auto_status = None
    auto_summary = ""
    auto_issues: List[dict] = []
    auto_next_actions: List[dict] = []

    if failed_selftest:
        auto_status = "needs_changes"
        auto_summary = "存在工具自测失败步骤，需修复后再完成任务。"
        auto_issues.append(
            {
                "title": "工具自测失败",
                "severity": "high",
                "details": "新工具未通过最小输入自测，但任务仍继续执行，可能导致结果不可信。",
                "evidence": truncate_text(json.dumps(failed_steps, ensure_ascii=False), 320),
                "suggestion": "补齐/修复工具执行命令并重新自测，确认输出非空且与目标相关。",
            }
        )

    if failed_exec_step and not auto_status:
        auto_status = "needs_changes"
        auto_summary = "存在关键执行步骤失败记录，需修复后再继续。"
        auto_issues.append(
            {
                "title": "关键执行步骤失败",
                "severity": "high",
                "details": "tool_call/shell_command/file_write 等关键步骤失败，最终输出缺乏可信执行证据。",
                "evidence": truncate_text(json.dumps(failed_steps, ensure_ascii=False), 320),
                "suggestion": "根据失败原因修复执行步骤（路径/依赖/命令），必要时插入重试步骤。",
            }
        )

    if plan_artifacts:
        state_obj = extract_json_object(run_row["agent_state"] or "") if run_row else None
        workdir = ""
        if isinstance(state_obj, dict):
            workdir = str(state_obj.get("workdir") or "").strip()
        if not workdir:
            workdir = os.getcwd()
        artifacts_check_workdir = workdir

        for item in plan_artifacts:
            rel = str(item or "").strip()
            if not rel:
                continue
            target = rel
            if not os.path.isabs(target):
                target = os.path.abspath(os.path.join(workdir, target))
            exists = bool(os.path.exists(target))
            artifacts_check_items.append({"path": rel, "exists": exists})
            if not exists:
                missing_artifacts.append(rel)

        if missing_artifacts and not auto_status:
            auto_status = "needs_changes"
            auto_summary = "计划声明的产物未落盘，任务不应判定完成。"
            auto_issues.append(
                {
                    "title": "产物缺失",
                    "severity": "high",
                    "details": "计划声明的 artifacts 未生成或路径错误。",
                    "evidence": truncate_text(json.dumps(missing_artifacts, ensure_ascii=False), 260),
                    "suggestion": "检查 file_write 路径与 workdir，补齐缺失文件后再输出结果。",
                }
            )

    task_row = None
    try:
        task_row = get_task(task_id=tid)
    except Exception:
        task_row = None
    task_title = str(task_row["title"]) if task_row else ""
    state_obj = extract_json_object(run_row["agent_state"] or "") if run_row else None
    mode = str(state_obj.get("mode") or "").strip().lower() if isinstance(state_obj, dict) else ""
    if mode not in {"think", "do"}:
        mode = "do"
    run_meta = {
        "run_id": rid,
        "status": run_row["status"] if run_row else "",
        "started_at": run_row["started_at"] if run_row else None,
        "finished_at": run_row["finished_at"] if run_row else None,
        "summary": run_summary,
        "mode": mode,
        "artifacts_check": {
            "workdir": artifacts_check_workdir,
            "items": artifacts_check_items,
            "missing": missing_artifacts,
        },
    }
    if mode == "think" and isinstance(state_obj, dict):
        vote_records = state_obj.get("vote_records")
        if vote_records is None:
            vote_records = state_obj.get("plan_votes")
        alternative_plans = state_obj.get("alternative_plans")
        if alternative_plans is None:
            alternative_plans = state_obj.get("plan_alternatives")
        run_meta["think"] = {
            "think_config": state_obj.get("think_config"),
            "winning_planner_id": state_obj.get("winning_planner_id"),
            "vote_records": vote_records,
            "alternative_plans": alternative_plans,
            "reflection_count": state_obj.get("reflection_count"),
            "reflection_records": state_obj.get("reflection_records"),
            "executor_assignments": state_obj.get("executor_assignments"),
        }

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
        prompt = load_system_prompt("agent_evaluate")
        if not prompt:
            # fallback：即使提示词文件缺失也要可用
            prompt = (
                "你是评估 Agent，只输出 JSON："
                "{{\"status\":\"pass|needs_changes|fail\",\"summary\":\"\",\"issues\":[],\"next_actions\":[],\"skills\":[]}}。\n"
                "输入：{task_title}\n{run_meta}\n{plan}\n{steps}\n{outputs}\n{tool_calls}\n"
            )

        skill_categories_text = "\n".join(f"- {c}" for c in SKILL_CATEGORY_CHOICES)
        prompt_text = prompt.format(
            skill_categories=skill_categories_text,
            pass_threshold=int(AGENT_REVIEW_PASS_SCORE_THRESHOLD),
            distill_threshold=int(AGENT_REVIEW_DISTILL_SCORE_THRESHOLD),
            user_note="(auto)",
            task_title=task_title,
            run_meta=json.dumps(run_meta, ensure_ascii=False),
            plan=json.dumps(plan_compact, ensure_ascii=False),
            steps=json.dumps(steps_compact, ensure_ascii=False),
            outputs=json.dumps(outputs_compact, ensure_ascii=False),
            tool_calls=json.dumps(tools_compact, ensure_ascii=False),
        )

        # 进度：评估分析（LLM 审查）
        update_agent_review_record(
            review_id=int(review_id),
            status="running",
            summary="评估中：评估分析…",
        )

        model = resolve_default_model()
        # Think 模式：默认使用配置的 evaluator 模型（与 docs/agent 对齐）。
        if mode == "think":
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
                model = str(getattr(think_cfg, "evaluator_model", "") or "").strip() or base_model
            except Exception:
                model = base_model
        parameters = {"temperature": 0}
        text, _, err = call_openai(prompt_text, model, parameters)
        obj = extract_json_object(text or "") if not err else None

        # 进度：落库沉淀（生成评估记录/建议）
        update_agent_review_record(
            review_id=int(review_id),
            status="running",
            summary="评估中：落库沉淀…",
        )

        def _coerce_score(value: object) -> Optional[float]:
            try:
                fv = float(value)  # type: ignore[arg-type]
            except Exception:
                return None
            if fv < 0:
                fv = 0.0
            if fv > 100:
                fv = 100.0
            return float(fv)

        def _normalize_distill_status(value: object) -> str:
            v = str(value or "").strip().lower()
            if v in {
                AGENT_REVIEW_DISTILL_STATUS_ALLOW,
                AGENT_REVIEW_DISTILL_STATUS_DENY,
                AGENT_REVIEW_DISTILL_STATUS_MANUAL,
            }:
                return v
            return ""

        pass_threshold = float(AGENT_REVIEW_PASS_SCORE_THRESHOLD)
        distill_threshold = float(AGENT_REVIEW_DISTILL_SCORE_THRESHOLD)
        pass_score: Optional[float] = None
        distill_score: Optional[float] = None
        distill_status = ""
        distill_notes = ""

        if not isinstance(obj, dict):
            status = "fail"
            summary = f"评估失败：{err or 'invalid_json'}"
            issues = [
                {
                    "title": "评估失败",
                    "severity": "high",
                    "details": "Eval Agent 未能生成有效 JSON（可能是 LLM 配置/网络/提示词/返回格式问题）。",
                    "evidence_quote": truncate_text(str(err or text or ""), 260),
                    "evidence_refs": [],
                    "suggestion": "检查设置页的 LLM 配置（API Key/Base URL/Model），并在桌宠中用 /eval run_id 复现错误。",
                }
            ]
            next_actions = [{"title": "修复评估链路", "details": "确认 /agent/evaluate/stream 可用并能写入评估记录。"}]
            pass_score = 0.0
            distill_status = AGENT_REVIEW_DISTILL_STATUS_DENY
            distill_score = 0.0
            distill_notes = "评估 JSON 无效：禁止自动沉淀"
        else:
            status = str(obj.get("status") or "").strip() or "needs_changes"
            normalized_status = str(status or "").strip().lower()
            if normalized_status not in {"pass", "needs_changes", "fail"}:
                normalized_status = "needs_changes"
            status = normalized_status

            summary = str(obj.get("summary") or "").strip()
            issues = obj.get("issues") if isinstance(obj.get("issues"), list) else []
            next_actions = obj.get("next_actions") if isinstance(obj.get("next_actions"), list) else []

            pass_score = _coerce_score(obj.get("pass_score"))
            pass_threshold_value = _coerce_score(obj.get("pass_threshold"))
            if pass_threshold_value is not None:
                pass_threshold = float(pass_threshold_value)

            distill_payload = obj.get("distill")
            if isinstance(distill_payload, dict):
                distill_status = _normalize_distill_status(distill_payload.get("status"))
                distill_score = _coerce_score(distill_payload.get("score"))
                distill_threshold_value = _coerce_score(distill_payload.get("threshold"))
                if distill_threshold_value is not None:
                    distill_threshold = float(distill_threshold_value)
                distill_notes = str(
                    distill_payload.get("reason") or distill_payload.get("notes") or ""
                ).strip()

            if not distill_status:
                distill_status = _normalize_distill_status(obj.get("distill_status"))
            if distill_score is None:
                distill_score = _coerce_score(obj.get("distill_score"))
            if not distill_notes:
                distill_notes = str(obj.get("distill_notes") or "").strip()

            if pass_score is None:
                if status == "pass":
                    pass_score = 100.0
                elif status == "needs_changes":
                    pass_score = 70.0
                else:
                    pass_score = 0.0

            # 兜底一致性：status=pass 但 score 未达门槛时，降级为 needs_changes
            if status == "pass" and pass_score is not None and pass_score < pass_threshold:
                status = "needs_changes"
                if not summary:
                    summary = "评分未达标：需补齐验证与修复后再交付。"

            # 知识沉淀前置条件：未通过任务评估(pass)则不允许沉淀（与 docs/agent 对齐）
            if status != "pass":
                distill_status = AGENT_REVIEW_DISTILL_STATUS_DENY
                distill_score = 0.0

            # backward compatible：旧评估没有 distill 字段时，默认按历史语义（pass=允许沉淀）
            if not distill_status:
                distill_status = (
                    AGENT_REVIEW_DISTILL_STATUS_ALLOW if status == "pass" else AGENT_REVIEW_DISTILL_STATUS_DENY
                )
            if distill_score is None:
                distill_score = float(pass_score or 0.0) if status == "pass" else 0.0

            # distill 门槛：allow 但未达阈值时，默认不自动沉淀（manual）
            if (
                distill_status == AGENT_REVIEW_DISTILL_STATUS_ALLOW
                and distill_score is not None
                and distill_score < distill_threshold
            ):
                distill_status = AGENT_REVIEW_DISTILL_STATUS_MANUAL
                if not distill_notes:
                    distill_notes = "distill_score 未达门槛：默认不自动沉淀"

            # 证据引用清洗：防止 LLM 胡编不存在的 id
            valid_step_ids = set()
            valid_output_ids = set()
            valid_tool_call_ids = set()
            try:
                valid_step_ids = {int(r["id"]) for r in (step_rows or []) if r and r["id"] is not None}
            except Exception:
                valid_step_ids = set()
            try:
                valid_output_ids = {int(r["id"]) for r in (output_rows or []) if r and r["id"] is not None}
            except Exception:
                valid_output_ids = set()
            try:
                valid_tool_call_ids = {int(r["id"]) for r in (tool_rows or []) if r and r["id"] is not None}
            except Exception:
                valid_tool_call_ids = set()
            artifact_paths = set()
            try:
                artifact_paths = {str(x).strip() for x in (plan_artifacts or []) if str(x).strip()}
            except Exception:
                artifact_paths = set()

            def _filter_evidence_refs(raw: object) -> List[dict]:
                if not isinstance(raw, list):
                    return []
                out: List[dict] = []
                for it in raw:
                    if not isinstance(it, dict):
                        continue
                    kind = str(it.get("kind") or "").strip().lower()
                    if kind == "step":
                        try:
                            sid = int(it.get("step_id"))
                        except Exception:
                            continue
                        if valid_step_ids and sid not in valid_step_ids:
                            continue
                        ref = {"kind": "step", "step_id": sid}
                        if it.get("step_order") is not None:
                            try:
                                ref["step_order"] = int(it.get("step_order"))
                            except Exception:
                                pass
                        out.append(ref)
                    elif kind == "output":
                        try:
                            oid = int(it.get("output_id"))
                        except Exception:
                            continue
                        if valid_output_ids and oid not in valid_output_ids:
                            continue
                        out.append({"kind": "output", "output_id": oid})
                    elif kind == "tool_call":
                        try:
                            cid = int(it.get("tool_call_record_id"))
                        except Exception:
                            continue
                        if valid_tool_call_ids and cid not in valid_tool_call_ids:
                            continue
                        out.append({"kind": "tool_call", "tool_call_record_id": cid})
                    elif kind == "artifact":
                        path = str(it.get("path") or "").strip()
                        if not path:
                            continue
                        if artifact_paths and path not in artifact_paths:
                            continue
                        ref = {"kind": "artifact", "path": path}
                        exists_value = it.get("exists")
                        if isinstance(exists_value, bool):
                            ref["exists"] = bool(exists_value)
                        out.append(ref)
                    if len(out) >= 8:
                        break
                return out

            normalized_issues: List[dict] = []
            for issue in issues or []:
                if not isinstance(issue, dict):
                    continue
                refs = _filter_evidence_refs(issue.get("evidence_refs"))
                issue["evidence_refs"] = refs
                quote = str(issue.get("evidence_quote") or issue.get("evidence") or "").strip()
                if quote:
                    issue["evidence_quote"] = truncate_text(quote, 120)
                normalized_issues.append(issue)
                if len(normalized_issues) >= 50:
                    break
            issues = normalized_issues

        update_agent_review_record(
            review_id=int(review_id),
            status=status,
            pass_score=pass_score,
            pass_threshold=pass_threshold,
            distill_status=distill_status,
            distill_score=distill_score,
            distill_threshold=distill_threshold,
            distill_notes=distill_notes,
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

        # 新工具注册闭环：仅当 run 成功结束且评估通过时，把本次 run 创建的 draft 工具升级为 approved，
        # 使其进入后续任务的“可复用工具清单”（tools_hint）。
        try:
            from backend.src.services.tools.tool_approval import approve_draft_tools_from_run

            allow_waiting_feedback = False
            try:
                run_status_value = str(run_row["status"] or "").strip() if run_row else ""
                if run_status_value == RUN_STATUS_WAITING:
                    state_obj = extract_json_object(run_row["agent_state"] or "") if run_row else None
                    paused = state_obj.get("paused") if isinstance(state_obj, dict) else None
                    step_title = str(paused.get("step_title") or "").strip() if isinstance(paused, dict) else ""
                    allow_waiting_feedback = step_title == AGENT_TASK_FEEDBACK_STEP_TITLE
            except Exception:
                allow_waiting_feedback = False

            approve_draft_tools_from_run(
                task_id=int(tid),
                run_id=int(rid),
                run_status=str(run_row["status"] or "") if run_row else "",
                review_id=int(review_id),
                review_status=str(status or ""),
                distill_status=str(distill_status or "") if distill_status else None,
                allow_waiting_feedback=bool(allow_waiting_feedback),
                model=None,
            )
        except Exception as exc:
            _safe_write_debug(
                int(tid),
                int(rid),
                message="tool.approval.failed",
                data={"review_id": int(review_id), "error": str(exc)},
                level="warning",
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


def backfill_missing_agent_reviews(*, limit: int = 10) -> dict:
    """
    启动兜底：补齐最近 N 条“已结束(done/failed/stopped)但缺评估”的 Agent runs。
    """
    try:
        rows = list_agent_runs_missing_reviews(
            statuses=[RUN_STATUS_DONE, RUN_STATUS_FAILED, RUN_STATUS_STOPPED],
            limit=int(limit),
        )
    except Exception as exc:
        return {"ok": False, "error": str(exc), "count": 0, "items": []}

    created = []
    for row in rows:
        try:
            run_id = int(row["id"])
            task_id = int(row["task_id"])
        except Exception:
            continue
        review_id = ensure_agent_review_record(task_id=task_id, run_id=run_id, skills=[])
        if review_id:
            created.append({"run_id": run_id, "review_id": int(review_id)})
    return {"ok": True, "count": len(created), "items": created}


def backfill_waiting_feedback_agent_reviews(*, limit: int = 10) -> dict:
    """
    启动兜底：补齐最近 N 条“waiting(确认满意度) 但缺评估”的 Agent runs。

    背景：
    - 任务闭环引入“确认满意度”后，run 会停在 waiting；
    - waiting run 不会触发 postprocess_thread，因此如果不额外处理，
      世界页会长期显示“评估未触发”，用户也无法基于评估建议继续改进。
    """
    try:
        rows = list_agent_runs_missing_reviews(statuses=[RUN_STATUS_WAITING], limit=int(limit))
    except Exception as exc:
        return {"ok": False, "error": str(exc), "count": 0, "items": []}

    created = []
    for row in rows:
        try:
            run_id = int(row["id"])
            task_id = int(row["task_id"])
        except Exception:
            continue
        # 只处理“确认满意度等待”，避免把“用户补充信息等待(user_prompt)”误当作已完成任务。
        try:
            state_obj = extract_json_object(row["agent_state"] or "") if row and row["agent_state"] else None
            paused = state_obj.get("paused") if isinstance(state_obj, dict) else None
            step_title = str(paused.get("step_title") or "").strip() if isinstance(paused, dict) else ""
            if step_title != AGENT_TASK_FEEDBACK_STEP_TITLE:
                continue
        except Exception:
            continue

        review_id = ensure_agent_review_record(task_id=task_id, run_id=run_id, skills=[], force=False)
        if review_id:
            created.append({"run_id": run_id, "review_id": int(review_id)})
    return {"ok": True, "count": len(created), "items": created}


def write_task_result_memory_if_missing(
    *,
    task_id: int,
    run_id: int,
    title: str,
    output_rows: Optional[List[dict]] = None,
) -> Optional[dict]:
    """
    为一次成功 run 写入“任务结果摘要”到 memory_items（短期记忆）。

    设计目标：
    - 记忆面板不能长期为空（MVP 可用性）
    - 去重：同一个 run 只写一次（通过 tags 中的 run:{run_id} 判断）
    - 不依赖 LLM：直接从 task_outputs 里挑选可用输出并截断
    """
    title_value = str(title or "").strip()
    existed_id = find_memory_item_id_by_task_and_tag_like(
        task_id=int(task_id),
        tag_like=f"%run:{run_id}%",
    )
    if existed_id:
        return None

    rows = output_rows
    if rows is None:
        fetched = list_task_outputs_for_run(
            task_id=int(task_id),
            run_id=int(run_id),
            limit=20,
            order="DESC",
        )
        rows = [dict(row) for row in fetched] if fetched else []

    picked = ""
    for row in rows or []:
        out_type = str((row or {}).get("output_type") or "")
        content = str((row or {}).get("content") or "").strip()
        if not content:
            continue
        if out_type == "user_prompt":
            continue
        if out_type == TASK_OUTPUT_TYPE_DEBUG:
            continue
        picked = content
        break

    if not picked:
        picked = title_value

    picked = str(picked or "").strip()
    if picked.startswith(STREAM_TAG_RESULT):
        picked = picked[len(STREAM_TAG_RESULT) :].strip()

    if not picked:
        return None

    if len(picked) > MEMORY_AUTO_TASK_RESULT_MAX_CHARS:
        picked = picked[:MEMORY_AUTO_TASK_RESULT_MAX_CHARS].rstrip()

    memory_text = picked
    if title_value and picked != title_value:
        memory_text = f"任务：{title_value}\n结果：{picked}"

    # 增加 mode 标签（docs/agent 约定：便于后续按 do/think 过滤与溯源）
    mode_tag = "mode:do"
    try:
        run_row = get_task_run(run_id=int(run_id))
        state_obj = extract_json_object(run_row["agent_state"] or "") if run_row else None
        mode = str(state_obj.get("mode") or "").strip().lower() if isinstance(state_obj, dict) else ""
        if mode == "think":
            mode_tag = "mode:think"
    except Exception:
        mode_tag = "mode:do"

    # 说明：统一走 services 层，确保 DB 与 backend/prompt/memory 强一致落盘。
    result = create_memory_item_service(
        {
            "content": memory_text,
            "memory_type": DEFAULT_MEMORY_TYPE,
            "tags": [
                MEMORY_TAG_AUTO,
                MEMORY_TAG_TASK_RESULT,
                f"task:{task_id}",
                f"run:{run_id}",
                mode_tag,
            ],
            "task_id": int(task_id),
        }
    )
    item = result.get("item") if isinstance(result, dict) else None
    return item if isinstance(item, dict) else None


def postprocess_task_run(
    task_row,
    task_id: int,
    run_id: int,
    run_status: str,
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

    eval_response = None
    skill_response = None
    graph_update = None
    criteria_list: List[str] = []

    # 1) 自动评估：简单关键词命中（MVP）
    expectation_id = task_row["expectation_id"] if task_row else None
    expectation_row = None
    if expectation_id is not None:
        expectation_row = get_expectation_repo(expectation_id=int(expectation_id))
    if expectation_row:
        criteria_list = parse_json_list(expectation_row["criteria"])
        eval_created_at = now_iso()
        evidence_texts: List[str] = [task_row["title"] or ""]
        output_rows = list_task_outputs_for_run(task_id=int(task_id), run_id=int(run_id), order="ASC")
        step_rows = list_task_steps_for_run(task_id=int(task_id), run_id=int(run_id))
        for row in output_rows:
            if row["content"]:
                evidence_texts.append(str(row["content"]))
        for row in step_rows:
            if row["result"]:
                evidence_texts.append(str(row["result"]))
        evidence_text = " ".join(evidence_texts).lower()

        pass_count = 0
        eval_criteria_payload = []
        for criterion in criteria_list:
            normalized = str(criterion).strip()
            if not normalized:
                continue
            matched = normalized.lower() in evidence_text
            status = "pass" if matched else "fail"
            if matched:
                pass_count += 1
            eval_criteria_payload.append(
                {
                    "criterion": normalized,
                    "status": status,
                    "notes": "命中关键词" if matched else "未匹配关键词",
                }
            )

        score = pass_count / len(criteria_list) if criteria_list else None
        if criteria_list:
            eval_status = "pass" if score is not None and score >= EVAL_PASS_RATE_THRESHOLD else "fail"
            eval_notes = f"自动评估：命中 {pass_count}/{len(criteria_list)}"
        else:
            eval_status = "unknown"
            eval_notes = "自动评估：未提供 criteria"

        with get_connection() as conn:
            eval_id, _ = create_eval_record(
                status=eval_status,
                score=score,
                notes=eval_notes,
                task_id=task_id,
                expectation_id=expectation_row["id"],
                created_at=eval_created_at,
                conn=conn,
            )
            create_eval_criteria_bulk(
                eval_id=int(eval_id),
                items=eval_criteria_payload,
                created_at=eval_created_at,
                conn=conn,
            )
        eval_response = {"eval_id": eval_id, "status": eval_status}

    # 2) 评估门控（Agent Review）：仅评估通过(pass)才允许知识沉淀（solutions/skills/graph）。
    # 说明：
    # - 与 docs/agent 对齐：needs_changes/fail 不进行沉淀，避免污染知识库；
    # - 先确保评估记录存在，再决定是否允许沉淀，避免“先沉淀后评估”的时序问题。
    allow_distill = False
    latest_review_id: Optional[int] = None
    review_status = ""
    try:
        latest_review_id = get_latest_agent_review_id_for_run(run_id=int(run_id))
        if not latest_review_id:
            latest_review_id = ensure_agent_review_record(
                task_id=int(task_id),
                run_id=int(run_id),
                skills=[],
                force=False,
            )
        if latest_review_id:
            from backend.src.repositories.agent_reviews_repo import get_agent_review as repo_get_agent_review

            review_row = repo_get_agent_review(review_id=int(latest_review_id))
            review_status = str(review_row["status"] or "").strip().lower() if review_row else ""
            review_distill_status = str(review_row["distill_status"] or "").strip().lower() if review_row else ""
            distill_score = review_row["distill_score"] if review_row else None
            distill_threshold = review_row["distill_threshold"] if review_row else None

            if review_distill_status:
                score_ok = True
                try:
                    if distill_score is not None and distill_threshold is not None:
                        score_ok = float(distill_score) >= float(distill_threshold)
                except Exception:
                    score_ok = True
                allow_distill = (
                    review_status == "pass"
                    and review_distill_status == AGENT_REVIEW_DISTILL_STATUS_ALLOW
                    and bool(score_ok)
                )
            else:
                # backward compatible：旧评估记录没有 distill_status 时，保持历史语义（pass=允许沉淀）
                allow_distill = review_status == "pass"
    except Exception as exc:
        allow_distill = False
        latest_review_id = None
        review_status = ""
        _safe_write_debug(
            int(task_id),
            int(run_id),
            message="postprocess.review_gate.failed",
            data={"error": str(exc)},
            level="warning",
        )

    # 3) 草稿技能升级：评估通过(pass)时，把本次 run 创建的 draft 技能升级为 approved 并落盘文件。
    # 与 docs/agent 对齐：规划阶段创建的 draft 知识，只有在评估通过后才进入知识库。
    # 注意：Solution（skill_type='solution'）有独立的后处理生成逻辑（run_solution_autogen），
    # 需要先用“实际执行记录”覆盖草稿再升级；因此这里仅升级非 solution 的 draft 技能。
    if allow_distill:
        try:
            from backend.src.repositories.skills_repo import update_skill_status
            from backend.src.services.skills.skills_publish import publish_skill_file

            with get_connection() as conn:
                rows = conn.execute(
                    "SELECT id FROM skills_items WHERE status = 'draft' AND source_run_id = ? AND (skill_type IS NULL OR skill_type != 'solution') ORDER BY id ASC",
                    (int(run_id),),
                ).fetchall()

            approved_skill_ids: List[int] = []
            for row in rows or []:
                try:
                    sid = int(row["id"])
                except Exception:
                    continue
                if sid <= 0:
                    continue
                try:
                    _ = update_skill_status(skill_id=int(sid), status="approved")
                    _source_path, _publish_err = publish_skill_file(int(sid))
                    if _publish_err:
                        continue
                    approved_skill_ids.append(int(sid))
                except Exception:
                    continue

            if approved_skill_ids:
                _safe_write_debug(
                    int(task_id),
                    int(run_id),
                    message="skill.draft_approved",
                    data={
                        "review_id": int(latest_review_id) if latest_review_id else None,
                        "review_status": str(review_status or ""),
                        "skill_ids": approved_skill_ids,
                    },
                    level="info",
                )
        except Exception as exc:
            _safe_write_debug(
                int(task_id),
                int(run_id),
                message="skill.draft_approve_failed",
                data={"error": str(exc)},
                level="warning",
            )
    elif str(review_status or "").strip().lower() == "fail":
        # docs/agent：Draft 生命周期
        # - fail → draft 标记为 abandoned（不参与后续检索，保留供溯源）
        try:
            from backend.src.repositories.skills_repo import update_skill_status

            with get_connection() as conn:
                rows = conn.execute(
                    "SELECT id FROM skills_items WHERE status = 'draft' AND source_run_id = ? ORDER BY id ASC",
                    (int(run_id),),
                ).fetchall()

            abandoned_skill_ids: List[int] = []
            for row in rows or []:
                try:
                    sid = int(row["id"])
                except Exception:
                    continue
                if sid <= 0:
                    continue
                try:
                    updated = update_skill_status(skill_id=int(sid), status="abandoned")
                    if updated:
                        abandoned_skill_ids.append(int(sid))
                except Exception:
                    continue

            if abandoned_skill_ids:
                _safe_write_debug(
                    int(task_id),
                    int(run_id),
                    message="skill.draft_abandoned",
                    data={
                        "review_id": int(latest_review_id) if latest_review_id else None,
                        "review_status": str(review_status or ""),
                        "skill_ids": abandoned_skill_ids,
                    },
                    level="info",
                )
        except Exception as exc:
            _safe_write_debug(
                int(task_id),
                int(run_id),
                message="skill.draft_abandon_failed",
                data={"error": str(exc)},
                level="warning",
            )

    # 4) 图谱更新：仅在评估通过后执行（失败不阻塞主流程）
    if allow_distill:
        try:
            step_rows = list_task_steps_for_run(task_id=int(task_id), run_id=int(run_id))
            output_rows = list_task_outputs_for_run(task_id=int(task_id), run_id=int(run_id), order="ASC")
            graph_update = extract_graph_updates(task_id, run_id, step_rows, output_rows)
        except Exception:
            graph_update = None

    try:
        if allow_distill:
            from backend.src.services.skills.run_solution_autogen import autogen_solution_from_run

            _ = autogen_solution_from_run(task_id=int(task_id), run_id=int(run_id), force=False)
    except Exception as exc:
        _safe_write_debug(
            int(task_id),
            int(run_id),
            message="solution.autogen_failed",
            data={"error": str(exc)},
            level="warning",
        )

    # 4) 自动技能抽象：只沉淀“可迁移 patterns”，避免把每次执行的具体步骤都变成技能卡导致技能库膨胀
    try:
        if allow_distill:
            from backend.src.services.llm.llm_client import resolve_default_model
            from backend.src.services.skills.run_skill_autogen import autogen_skills_from_run

            skill_response = autogen_skills_from_run(
                task_id=int(task_id),
                run_id=int(run_id),
                model=resolve_default_model(),
            )
        else:
            skill_response = {"ok": True, "status": "skipped_review_not_pass"}
    except Exception as exc:
        skill_response = {"ok": False, "error": f"{exc}"}

    # 5) 自动记忆：把本次 run 的“最终结果摘要”写入 memory_items，避免记忆面板长期为空。
    # 说明：这是轻量兜底（短期记忆），不替代图谱/skills；失败不阻塞主流程。
    try:
        title = str(task_row["title"] or "").strip() if task_row else ""
        write_task_result_memory_if_missing(task_id=task_id, run_id=run_id, title=title)
    except Exception as exc:
        _safe_write_debug(
            int(task_id),
            int(run_id),
            message="memory.auto_task_result_failed",
            data={"error": str(exc)},
            level="warning",
        )

    # 6) 自动复盘补齐：把后置抽象产物（skills）写回到最新评估记录（用于 UI 可观察性）
    try:
        skills = []
        if isinstance(skill_response, dict) and isinstance(skill_response.get("skills"), list):
            skills = skill_response.get("skills") or []
        if latest_review_id:
            update_agent_review_record(review_id=int(latest_review_id), skills=skills)
    except Exception as exc:
        _safe_write_debug(
            int(task_id),
            int(run_id),
            message="agent.review.ensure_failed",
            data={"error": str(exc)},
            level="warning",
        )

    return eval_response, skill_response, graph_update
