from dataclasses import dataclass
from typing import Callable, Generator, List, Optional

from backend.src.common.utils import now_iso
from backend.src.constants import (
    ACTION_TYPE_FILE_WRITE,
    ACTION_TYPE_SHELL_COMMAND,
    ACTION_TYPE_TASK_OUTPUT,
    ACTION_TYPE_TOOL_CALL,
    AGENT_REACT_ARTIFACT_AUTOFIX_MAX_ATTEMPTS,
    AGENT_REACT_REPLAN_MAX_ATTEMPTS,
    RUN_STATUS_FAILED,
    STREAM_TAG_EXEC,
    STREAM_TAG_FAIL,
)
from backend.src.agent.runner.react_plan_state import build_agent_plan_payload
from backend.src.agent.runner.plan_events import sse_plan, sse_plan_delta
from backend.src.repositories.task_runs_repo import update_task_run
from backend.src.services.llm.llm_client import sse_json
from backend.src.services.tasks.task_run_lifecycle import check_missing_artifacts


@dataclass
class ArtifactsGateOutcome:
    """
    artifacts 门闩的控制结果：
    - run_status != None：外层应 break 并结束本次 run（通常为 failed）
    - next_idx != None：外层应把 idx 设为 next_idx 并 continue
    - plan_* 不为 None：外层应替换当前 plan 引用（通常发生在 replan 之后）
    """

    run_status: Optional[str] = None
    next_idx: Optional[int] = None
    plan_titles: Optional[List[str]] = None
    plan_items: Optional[List[dict]] = None
    plan_allows: Optional[List[List[str]]] = None
    plan_artifacts: Optional[List[str]] = None


def _has_success_validation_step(
    *,
    plan_titles: List[str],
    plan_items: List[dict],
    plan_allows: List[List[str]],
) -> bool:
    """
    判断是否存在“成功的验证步骤”（用于 artifacts 任务的最终输出门闩）。
    - 允许类型：shell_command / tool_call
    - 标题包含“验证/校验/检查/自测”等关键词
    - 状态必须是 done
    """
    keywords = ("验证", "校验", "检查", "自测", "verify", "validate", "check", "test")
    for idx, title in enumerate(plan_titles):
        allow = plan_allows[idx] if idx < len(plan_allows) else []
        allow_set = set(allow or [])
        if ACTION_TYPE_SHELL_COMMAND not in allow_set and ACTION_TYPE_TOOL_CALL not in allow_set:
            continue
        item = plan_items[idx] if idx < len(plan_items) else {}
        status = str(item.get("status") or "pending") if isinstance(item, dict) else "pending"
        if status != "done":
            continue
        title_text = str(title or "")
        if any(key in title_text for key in keywords):
            return True
    return False


def apply_artifacts_gates(
    *,
    task_id: int,
    run_id: int,
    idx: int,
    step_order: int,
    title: str,
    workdir: str,
    message: str,
    model: str,
    react_params: dict,
    tools_hint: str,
    skills_hint: str,
    memories_hint: str,
    graph_hint: str,
    allowed: List[str],
    plan_titles: List[str],
    plan_items: List[dict],
    plan_allows: List[List[str]],
    plan_artifacts: List[str],
    agent_state: dict,
    observations: List[str],
    max_steps_limit: Optional[int],
    run_replan_and_merge: Callable[..., Generator[str, None, object]],
    safe_write_debug: Callable[..., None],
) -> Generator[str, None, ArtifactsGateOutcome]:
    """
    artifacts 门闩集合：
    1) 声明了 artifacts 时，task_output 前必须先检查文件是否落盘；缺失则尝试自动插入 file_write 补救步骤；
    2) 存在 failed 步骤时，禁止直接输出最终结果；优先 replan 补救；
    3) 声明了 artifacts 时，要求至少出现一次“验证成功”的步骤（done 且标题含关键字且 allow 包含 shell/tool）。
    """
    allowed_set = set(allowed or [])

    # --- 1) task_output + artifacts：缺文件则补救或失败 ---
    if plan_artifacts and ACTION_TYPE_TASK_OUTPUT in allowed_set:
        missing = check_missing_artifacts(artifacts=plan_artifacts, workdir=workdir)
        if missing:
            autofix_attempts = 0
            try:
                autofix_attempts = int(agent_state.get("artifact_autofix_attempts") or 0)
            except Exception:
                autofix_attempts = 0

            can_autofix = (
                max_steps_limit is not None
                and len(plan_titles) < int(max_steps_limit)
                and autofix_attempts < int(AGENT_REACT_ARTIFACT_AUTOFIX_MAX_ATTEMPTS or 0)
            )
            if can_autofix:
                remaining = int(max_steps_limit) - len(plan_titles)
                to_insert = list(missing[: max(0, int(remaining))])
                inserted = 0
                for rel in to_insert:
                    if max_steps_limit is not None and len(plan_titles) >= int(max_steps_limit):
                        break
                    plan_titles.insert(idx + inserted, f"file_write:{rel} 写入文件")
                    plan_allows.insert(idx + inserted, [ACTION_TYPE_FILE_WRITE])
                    plan_items.insert(idx + inserted, {"id": 0, "brief": "写文件", "status": "pending"})
                    inserted += 1

                # 重编号：保持 plan_items.id 与顺序一致
                for i, item in enumerate(plan_items, start=1):
                    if isinstance(item, dict):
                        item["id"] = i

                agent_state["artifact_autofix_attempts"] = autofix_attempts + 1
                safe_write_debug(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    message="agent.artifacts.autofix.insert_steps",
                    data={"missing": missing, "inserted": inserted},
                    level="warning",
                )
                observations.append(f"artifacts_missing_autofix: {', '.join(missing)}")

                # 推送并持久化更新后的计划：下一轮会从插入的 file_write 步骤继续执行
                yield sse_json({"delta": f"{STREAM_TAG_FAIL} 检测到缺失文件，已补齐写文件步骤，继续执行…\n"})
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
                        message="agent.plan.persist_failed",
                        data={"where": "artifact_autofix", "error": str(exc)},
                        level="warning",
                    )
                return ArtifactsGateOutcome(next_idx=int(idx))

            if 0 <= idx < len(plan_items):
                plan_items[idx]["status"] = "failed"
                yield sse_plan_delta(task_id=task_id, plan_items=plan_items, indices=[idx])
            safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.artifacts.missing",
                data={"missing": missing},
                level="error",
            )
            yield sse_json({"delta": f"{STREAM_TAG_FAIL} 未生成文件：{', '.join(missing)}\n"})
            return ArtifactsGateOutcome(run_status=RUN_STATUS_FAILED)

    # --- 2) 存在失败步骤：禁止直接输出最终结果，优先 replan 补救 ---
    if ACTION_TYPE_TASK_OUTPUT in allowed_set:
        failed_steps: List[int] = []
        for idx_item, item in enumerate(plan_items):
            if isinstance(item, dict) and str(item.get("status") or "").strip() == "failed":
                failed_steps.append(idx_item + 1)
        if failed_steps:
            try:
                replan_attempts = int(agent_state.get("replan_attempts") or 0)
            except Exception:
                replan_attempts = 0
            done_count = max(0, int(step_order) - 1)
            remaining_limit = None
            if isinstance(max_steps_limit, int) and max_steps_limit > 0:
                remaining_limit = int(max_steps_limit) - done_count
            can_replan = (remaining_limit is None or remaining_limit > 0) and replan_attempts < int(
                AGENT_REACT_REPLAN_MAX_ATTEMPTS or 0
            )
            if can_replan:
                max_steps_value = int(remaining_limit) if remaining_limit is not None else int(
                    max_steps_limit or len(plan_titles)
                )
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
                    error=f"prior_failed_steps:{failed_steps}",
                    sse_notice=f"{STREAM_TAG_EXEC} 检测到失败步骤，重新规划剩余步骤…",
                    replan_attempts=replan_attempts,
                    safe_write_debug=safe_write_debug,
                )
                if replan_result:
                    return ArtifactsGateOutcome(
                        next_idx=done_count,
                        plan_titles=getattr(replan_result, "plan_titles", plan_titles),
                        plan_items=getattr(replan_result, "plan_items", plan_items),
                        plan_allows=getattr(replan_result, "plan_allows", plan_allows),
                        plan_artifacts=getattr(replan_result, "plan_artifacts", plan_artifacts),
                    )

            yield sse_json({"delta": f"{STREAM_TAG_FAIL} 存在失败步骤，无法直接输出结果\n"})
            if 0 <= idx < len(plan_items):
                plan_items[idx]["status"] = "failed"
                yield sse_plan_delta(task_id=task_id, plan_items=plan_items, indices=[idx])
            return ArtifactsGateOutcome(run_status=RUN_STATUS_FAILED)

    # --- 3) artifacts：必须至少有一次“验证成功”的步骤 ---
    if plan_artifacts and ACTION_TYPE_TASK_OUTPUT in allowed_set:
        if not _has_success_validation_step(
            plan_titles=plan_titles,
            plan_items=plan_items,
            plan_allows=plan_allows,
        ):
            try:
                replan_attempts = int(agent_state.get("replan_attempts") or 0)
            except Exception:
                replan_attempts = 0
            done_count = max(0, int(step_order) - 1)
            remaining_limit = None
            if isinstance(max_steps_limit, int) and max_steps_limit > 0:
                remaining_limit = int(max_steps_limit) - done_count
            can_replan = (remaining_limit is None or remaining_limit > 0) and replan_attempts < int(
                AGENT_REACT_REPLAN_MAX_ATTEMPTS or 0
            )
            if can_replan:
                max_steps_value = int(remaining_limit) if remaining_limit is not None else int(
                    max_steps_limit or len(plan_titles)
                )
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
                    error="artifact_validation_missing",
                    sse_notice=f"{STREAM_TAG_EXEC} 缺少验证步骤，重新规划剩余步骤…",
                    replan_attempts=replan_attempts,
                    safe_write_debug=safe_write_debug,
                )
                if replan_result:
                    return ArtifactsGateOutcome(
                        next_idx=done_count,
                        plan_titles=getattr(replan_result, "plan_titles", plan_titles),
                        plan_items=getattr(replan_result, "plan_items", plan_items),
                        plan_allows=getattr(replan_result, "plan_allows", plan_allows),
                        plan_artifacts=getattr(replan_result, "plan_artifacts", plan_artifacts),
                    )

            yield sse_json({"delta": f"{STREAM_TAG_FAIL} 缺少验证步骤，无法直接输出结果\n"})
            if 0 <= idx < len(plan_items):
                plan_items[idx]["status"] = "failed"
                yield sse_plan_delta(task_id=task_id, plan_items=plan_items, indices=[idx])
            return ArtifactsGateOutcome(run_status=RUN_STATUS_FAILED)

    return ArtifactsGateOutcome()
