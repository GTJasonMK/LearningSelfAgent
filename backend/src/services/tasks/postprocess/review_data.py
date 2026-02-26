from __future__ import annotations

import json
from typing import Callable, Dict, List, Optional, Tuple

from backend.src.common.utils import (
    extract_json_object,
    json_preview,
    parse_json_value,
    truncate_text,
)
from backend.src.constants import ACTION_TYPE_TOOL_CALL
from backend.src.repositories.task_outputs_repo import list_task_outputs_for_run
from backend.src.repositories.task_steps_repo import list_task_steps_for_run
from backend.src.repositories.tasks_repo import get_task
from backend.src.repositories.tool_call_records_repo import list_tool_calls_with_tool_name_by_run
from backend.src.services.agent_review.review_snapshot import (
    build_artifacts_check,
    build_run_meta,
    compact_outputs_for_review,
    compact_tools_for_review,
)


def collect_review_data(
    *,
    task_id: int,
    run_id: int,
    run_row: Optional[dict],
    is_selftest_title_fn: Callable[[str], bool],
    extract_tool_name_from_tool_call_step_fn: Callable[[str, object], str],
) -> dict:
    """
    收集评估输入证据，并生成自动失败判定与 run_meta。
    """
    tid = int(task_id)
    rid = int(run_id)

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
    # 自测/验证步骤失败仅在“后续无成功 tool_call”时才作为硬性失败依据。
    last_tool_success_pos: Dict[str, Tuple[int, int]] = {}
    last_failed_selftest_pos: Dict[str, Tuple[int, int]] = {}
    last_failed_selftest_step: Dict[str, dict] = {}

    for row in step_rows:
        action_type = None
        payload_preview = None
        try:
            detail_obj = parse_json_value(row["detail"]) if row["detail"] else None
            if isinstance(detail_obj, dict):
                action_type = detail_obj.get("type")
                payload_preview = detail_obj.get("payload")
        except Exception:
            action_type = None
            payload_preview = None

        status_value = str(row["status"] or "").strip()
        title_value = str(row["title"] or "").strip()

        step_id_value = None
        try:
            if row["id"] is not None:
                step_id_value = int(row["id"])
        except Exception:
            step_id_value = None

        step_order_value = None
        try:
            if row["step_order"] is not None:
                step_order_value = int(row["step_order"])
        except Exception:
            step_order_value = None

        pos = (
            int(step_order_value) if step_order_value is not None else 10**9,
            int(step_id_value) if step_id_value is not None else 10**9,
        )

        tool_name = ""
        if action_type == ACTION_TYPE_TOOL_CALL:
            tool_name = extract_tool_name_from_tool_call_step_fn(title_value, payload_preview)
            if status_value == "done" and tool_name:
                last_tool_success_pos[tool_name] = pos

        if status_value == "failed":
            if action_type == ACTION_TYPE_TOOL_CALL and tool_name and is_selftest_title_fn(title_value):
                last_failed_selftest_pos[tool_name] = pos
                last_failed_selftest_step[tool_name] = {
                    "step_order": row["step_order"],
                    "title": title_value,
                    "action_type": action_type,
                    "tool_name": tool_name,
                    "error": truncate_text(str(row["error"] or ""), 260),
                }

        steps_compact.append(
            {
                "step_id": row["id"],
                "step_order": row["step_order"],
                "title": title_value,
                "status": status_value,
                "action_type": action_type,
                "payload_preview": json_preview(payload_preview, 360),
                "result_preview": json_preview(row["result"], 520),
                "error_preview": truncate_text(str(row["error"] or ""), 260),
            }
        )
        if len(steps_compact) >= 80:
            break

    outputs_compact = compact_outputs_for_review(
        output_rows,
        content_key="content_preview",
        max_items=40,
        content_max_chars=620,
    )
    tools_compact = compact_tools_for_review(tool_rows)

    plan_artifacts = plan_obj.get("artifacts") if isinstance(plan_obj.get("artifacts"), list) else []
    state_obj = extract_json_object(run_row["agent_state"] or "") if run_row else None
    mode = str(state_obj.get("mode") or "").strip().lower() if isinstance(state_obj, dict) else ""
    if mode not in {"think", "do"}:
        mode = "do"
    artifacts_check_workdir, artifacts_check_items, missing_artifacts = build_artifacts_check(
        plan_artifacts=plan_artifacts,
        state_obj=state_obj if isinstance(state_obj, dict) else None,
    )

    auto_status = None
    auto_summary = ""
    auto_issues: List[dict] = []
    auto_next_actions: List[dict] = []

    unresolved_selftest_steps: List[dict] = []
    for tool_name, failed_pos in (last_failed_selftest_pos or {}).items():
        success_pos = last_tool_success_pos.get(tool_name)
        if not success_pos or success_pos <= failed_pos:
            step_obj = last_failed_selftest_step.get(tool_name)
            if isinstance(step_obj, dict):
                unresolved_selftest_steps.append(step_obj)

    if unresolved_selftest_steps:
        auto_status = "needs_changes"
        auto_summary = "存在未修复的工具自测失败步骤，需修复后再完成任务。"
        auto_issues.append(
            {
                "title": "工具自测失败",
                "severity": "high",
                "details": "新工具未通过最小输入自测，但任务仍继续执行，可能导致结果不可信。",
                "evidence": truncate_text(json.dumps(unresolved_selftest_steps, ensure_ascii=False), 320),
                "suggestion": "补齐/修复工具执行命令并重新自测，确认输出非空且与目标相关。",
            }
        )

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

    run_meta = build_run_meta(
        run_id=rid,
        run_row=run_row,
        mode=mode,
        state_obj=state_obj if isinstance(state_obj, dict) else None,
        workdir=artifacts_check_workdir,
        artifacts_check_items=artifacts_check_items,
        missing_artifacts=missing_artifacts,
    )

    return {
        "step_rows": step_rows,
        "output_rows": output_rows,
        "tool_rows": tool_rows,
        "plan_obj": plan_obj,
        "plan_compact": plan_compact,
        "steps_compact": steps_compact,
        "outputs_compact": outputs_compact,
        "tools_compact": tools_compact,
        "plan_artifacts": plan_artifacts,
        "artifacts_check_items": artifacts_check_items,
        "missing_artifacts": missing_artifacts,
        "auto_status": auto_status,
        "auto_summary": auto_summary,
        "auto_issues": auto_issues,
        "auto_next_actions": auto_next_actions,
        "task_title": task_title,
        "state_obj": state_obj,
        "mode": mode,
        "run_meta": run_meta,
    }
