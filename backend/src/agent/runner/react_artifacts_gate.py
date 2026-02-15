from dataclasses import dataclass
from typing import Callable, Generator, List, Optional

from backend.src.agent.core.plan_structure import PlanStep, PlanStructure
from backend.src.common.utils import now_iso
from backend.src.constants import (
    ACTION_TYPE_FILE_WRITE,
    ACTION_TYPE_HTTP_REQUEST,
    ACTION_TYPE_SHELL_COMMAND,
    ACTION_TYPE_TASK_OUTPUT,
    ACTION_TYPE_TOOL_CALL,
    AGENT_MAX_STEPS_UNLIMITED,
    AGENT_REACT_ARTIFACT_AUTOFIX_MAX_ATTEMPTS,
    AGENT_REACT_REPLAN_MAX_ATTEMPTS,
    STREAM_TAG_EXEC,
)
from backend.src.agent.runner.plan_events import sse_plan
from backend.src.repositories.task_runs_repo import update_task_run
from backend.src.services.llm.llm_client import sse_json
from backend.src.services.tasks.task_run_lifecycle import check_missing_artifacts


@dataclass
class ArtifactsGateOutcome:
    """
    artifacts 门闩的控制结果：
    - run_status != None：外层应 break 并结束本次 run（通常为 failed）
    - next_idx != None：外层应把 idx 设为 next_idx 并 continue
    - plan_changed: 计划是否被修改（replan/autofix），外层通过 plan_struct 直接读取最新状态
    """

    run_status: Optional[str] = None
    next_idx: Optional[int] = None
    plan_changed: bool = False


def _has_success_validation_step(plan_struct: PlanStructure) -> bool:
    """
    判断是否存在"成功的验证步骤"（用于 artifacts 任务的最终输出门闩）。
    - 允许类型：shell_command / tool_call
    - 标题包含"验证/校验/检查/自测"等关键词
    - 状态必须是 done
    """
    keywords = ("验证", "校验", "检查", "自测", "verify", "validate", "check", "test")
    for step in plan_struct.steps:
        allow_set = set(step.allow or [])
        if ACTION_TYPE_SHELL_COMMAND not in allow_set and ACTION_TYPE_TOOL_CALL not in allow_set:
            continue
        if step.status != "done":
            continue
        if any(key in str(step.title or "") for key in keywords):
            return True
    return False


def _has_prior_http_success_step(
    *,
    current_idx: int,
    plan_struct: PlanStructure,
) -> tuple[bool, bool]:
    """
    判断当前 task_output 前是否存在 http_request 依赖，且该依赖至少有一次成功执行。

    返回：
    - has_http_requirement: 前置步骤里是否包含 http_request
    - has_http_success: 前置 http_request 步骤里是否有 status=done
    """
    has_http_requirement = False
    has_http_success = False
    upper = max(0, int(current_idx))
    for idx in range(upper):
        step = plan_struct.get_step(idx)
        if step is None:
            continue
        allow_set = set(step.allow or [])
        is_http_step = ACTION_TYPE_HTTP_REQUEST in allow_set or str(step.title or "").startswith("http_request:")
        if not is_http_step:
            continue
        has_http_requirement = True
        if step.status == "done":
            has_http_success = True
            break
    return has_http_requirement, has_http_success


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
    plan_struct: PlanStructure,
    agent_state: dict,
    observations: List[str],
    max_steps_limit: Optional[int],
    run_replan_and_merge: Callable[..., Generator[str, None, object]],
    safe_write_debug: Callable[..., None],
) -> Generator[str, None, ArtifactsGateOutcome]:
    """
    artifacts 门闩集合：
    1) 声明了 artifacts 时，task_output 前必须先检查文件是否落盘；缺失则尝试自动插入 file_write 补救步骤；
    1.5) 存在 http_request 前置依赖时，优先确保至少有一次成功抓取证据（避免无证据 task_output）；
    2) 存在 failed 步骤时，优先 replan 补救；若仍无法补救，则继续执行并在结果中提示风险；
    3) 声明了 artifacts 时，优先要求至少出现一次"验证成功"的步骤（done 且标题含关键字且 allow 包含 shell/tool）。
    """
    allowed_set = set(allowed or [])
    plan_artifacts = plan_struct.artifacts

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
                and plan_struct.step_count < int(max_steps_limit)
                and autofix_attempts < int(AGENT_REACT_ARTIFACT_AUTOFIX_MAX_ATTEMPTS or 0)
            )
            if can_autofix:
                remaining = int(max_steps_limit) - plan_struct.step_count
                to_insert = list(missing[: max(0, int(remaining))])
                new_steps = []
                for rel in to_insert:
                    if max_steps_limit is not None and (plan_struct.step_count + len(new_steps)) >= int(max_steps_limit):
                        break
                    new_steps.append(
                        PlanStep(id=0, title=f"file_write:{rel} 写入文件", brief="写文件", allow=[ACTION_TYPE_FILE_WRITE], status="pending")
                    )

                if new_steps:
                    plan_struct.insert_steps(idx, new_steps)

                agent_state["artifact_autofix_attempts"] = autofix_attempts + 1
                safe_write_debug(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    message="agent.artifacts.autofix.insert_steps",
                    data={"missing": missing, "inserted": len(new_steps)},
                    level="warning",
                )
                observations.append(f"artifacts_missing_autofix: {', '.join(missing)}")

                # 推送并持久化更新后的计划：下一轮会从插入的 file_write 步骤继续执行
                yield sse_json({"delta": f"{STREAM_TAG_EXEC} 警告：检测到缺失文件，已补齐写文件步骤，继续执行…\n"})
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
                        message="agent.plan.persist_failed",
                        data={"where": "artifact_autofix", "error": str(exc)},
                        level="warning",
                    )
                return ArtifactsGateOutcome(next_idx=int(idx), plan_changed=True)

            safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.artifacts.missing",
                data={"missing": missing},
                level="warning",
            )
            observations.append(f"artifacts_missing_unfixed: {', '.join(missing)}")
            agent_state["missing_artifacts"] = list(missing)
            yield sse_json({"delta": f"{STREAM_TAG_EXEC} 警告：未生成文件：{', '.join(missing)}（继续执行，结果可能需要补救）\n"})
            return ArtifactsGateOutcome()

    # --- 1.5) task_output + http_request：缺少成功的抓取证据时禁止输出 ---
    if ACTION_TYPE_TASK_OUTPUT in allowed_set:
        has_http_requirement, has_http_success = _has_prior_http_success_step(
            current_idx=int(idx),
            plan_struct=plan_struct,
        )
        if has_http_requirement and not has_http_success:
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
                    error="http_evidence_missing",
                    sse_notice=f"{STREAM_TAG_EXEC} 缺少可验证的抓取证据，重新规划剩余步骤…",
                    replan_attempts=replan_attempts,
                    safe_write_debug=safe_write_debug,
                )
                if replan_result:
                    plan_struct.replace_from(replan_result.plan_struct)
                    return ArtifactsGateOutcome(next_idx=done_count, plan_changed=True)

            safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.http_evidence.missing",
                data={"step_order": int(step_order)},
                level="warning",
            )
            observations.append("http_evidence_missing: no successful http_request step")
            agent_state["http_evidence_missing"] = True
            yield sse_json({"delta": f"{STREAM_TAG_EXEC} 警告：缺少可验证的抓取证据（继续执行，结果可能不可靠）\n"})
            return ArtifactsGateOutcome()

    # --- 2) 存在失败步骤：禁止直接输出最终结果，优先 replan 补救 ---
    if ACTION_TYPE_TASK_OUTPUT in allowed_set:
        failed_steps: List[int] = []
        for step_idx, step in enumerate(plan_struct.steps):
            if step.status == "failed":
                failed_steps.append(step_idx + 1)
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
                    error=f"prior_failed_steps:{failed_steps}",
                    sse_notice=f"{STREAM_TAG_EXEC} 检测到失败步骤，重新规划剩余步骤…",
                    replan_attempts=replan_attempts,
                    safe_write_debug=safe_write_debug,
                )
                if replan_result:
                    plan_struct.replace_from(replan_result.plan_struct)
                    return ArtifactsGateOutcome(next_idx=done_count, plan_changed=True)

            safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.prior_failed_steps.before_output",
                data={"failed_steps": failed_steps},
                level="warning",
            )
            observations.append(f"prior_failed_steps_before_output: {failed_steps}")
            agent_state["prior_failed_steps_before_output"] = list(failed_steps)
            yield sse_json({"delta": f"{STREAM_TAG_EXEC} 警告：存在失败步骤（继续执行，但结果可能不完整）\n"})
            return ArtifactsGateOutcome()

    # --- 3) artifacts：必须至少有一次"验证成功"的步骤 ---
    if plan_artifacts and ACTION_TYPE_TASK_OUTPUT in allowed_set:
        if not _has_success_validation_step(plan_struct):
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
                    error="artifact_validation_missing",
                    sse_notice=f"{STREAM_TAG_EXEC} 缺少验证步骤，重新规划剩余步骤…",
                    replan_attempts=replan_attempts,
                    safe_write_debug=safe_write_debug,
                )
                if replan_result:
                    plan_struct.replace_from(replan_result.plan_struct)
                    return ArtifactsGateOutcome(next_idx=done_count, plan_changed=True)

            safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.artifacts.validation_missing",
                data={"artifacts": plan_artifacts},
                level="warning",
            )
            observations.append("artifact_validation_missing: no successful validation step")
            agent_state["artifact_validation_missing"] = True
            yield sse_json({"delta": f"{STREAM_TAG_EXEC} 警告：缺少验证步骤（继续执行，结果可能不可靠）\n"})
            return ArtifactsGateOutcome()

    return ArtifactsGateOutcome()
