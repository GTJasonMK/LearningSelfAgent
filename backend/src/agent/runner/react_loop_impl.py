# -*- coding: utf-8 -*-
"""
ReAct 执行循环核心实现。

本模块为主循环骨架，具体逻辑委托给子模块：
- react_state_manager: 状态管理和持久化
- react_step_executor: 步骤执行和观测生成
- react_error_handler: 错误处理和重试
"""

import json
from dataclasses import dataclass
from typing import Callable, Dict, Generator, List, Optional, Tuple

from backend.src.agent.support import (
    _truncate_observation,
    apply_next_step_patch,
)
from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.runner.react_helpers import (
    build_react_step_prompt,
    call_llm_for_text,
    validate_and_normalize_action_text,
)
from backend.src.agent.runner.capability_router import build_capability_hint, resolve_step_capability
from backend.src.agent.runner.feedback import is_task_feedback_step_title
from backend.src.agent.runner.react_feedback_flow import (
    handle_task_feedback_step,
    maybe_apply_review_gate_before_feedback,
)
from backend.src.agent.runner.plan_events import sse_plan, sse_plan_delta
from backend.src.agent.runner.react_artifacts_gate import apply_artifacts_gates
from backend.src.agent.runner.react_replan import run_replan_and_merge
from backend.src.agent.runner.react_state_manager import (
    persist_loop_state,
    persist_plan_only,
    resolve_executor,
)
from backend.src.agent.runner.react_step_executor import (
    generate_action_with_retry,
    build_observation_line,
    handle_user_prompt_action,
    handle_task_output_fallback,
    yield_memory_write_event,
    yield_visible_result,
)
from backend.src.agent.runner.react_error_handler import (
    handle_action_invalid,
    handle_allow_failure,
    handle_step_failure,
)
from backend.src.agent.source_failure_summary import summarize_recent_source_failures_for_prompt
from backend.src.common.utils import now_iso
from backend.src.constants import (
    ACTION_TYPE_MEMORY_WRITE,
    ACTION_TYPE_TASK_OUTPUT,
    ACTION_TYPE_USER_PROMPT,
    ACTION_TYPE_LLM_CALL,
    RUN_STATUS_DONE,
    RUN_STATUS_FAILED,
    RUN_STATUS_STOPPED,
    STEP_STATUS_RUNNING,
    STREAM_TAG_FAIL,
    STREAM_TAG_OK,
    STREAM_TAG_STEP,
)
from backend.src.services.debug.safe_debug import safe_write_debug as _safe_write_debug
from backend.src.services.llm.llm_client import sse_json
from backend.src.services.tasks.task_queries import (
    TaskStepCreateParams,
    create_task_step,
    get_task_run,
    mark_task_step_done,
    mark_task_step_failed,
)

@dataclass
class ReactLoopResult:
    """ReAct 循环执行结果。"""
    run_status: str
    last_step_order: int
    plan_struct: Optional[PlanStructure] = None


def _enforce_allow_constraints(
    *,
    task_id: int,
    run_id: int,
    step_order: int,
    step_title: str,
    workdir: str,
    allowed: List[str],
    allowed_text: str,
    action_obj: dict,
    action_type: str,
    payload_obj: dict,
    react_prompt: str,
    model: str,
    react_params: dict,
    variables_source: str,
    llm_call: Callable[[dict], dict],
) -> Tuple[Optional[dict], Optional[str], Optional[dict], Optional[str]]:
    """
    强制执行计划阶段给出的 allow 约束。
    """
    allowed_set = set(allowed or [])
    if not allowed_set or action_type in allowed_set:
        return action_obj, action_type, payload_obj, None

    _safe_write_debug(
        task_id=int(task_id),
        run_id=int(run_id),
        message="agent.allow_mismatch",
        data={"step_order": int(step_order), "got": action_type, "allow": allowed_text},
        level="warning",
    )

    forced_prompt = react_prompt + f"\n补充约束：本步骤允许的 action.type 只能是：{allowed_text}。请重新输出 JSON。\n"
    forced_text, forced_err = call_llm_for_text(
        llm_call,
        prompt=forced_prompt,
        task_id=int(task_id),
        run_id=int(run_id),
        model=model,
        parameters=react_params,
        variables={
            "source": f"{variables_source}_forced",
            "step_order": int(step_order),
            "allow": allowed_text,
        },
    )

    forced_obj, forced_type, forced_payload, forced_validate_error = validate_and_normalize_action_text(
        action_text=forced_text or "",
        step_title=step_title,
        workdir=workdir,
    )

    if forced_err or forced_validate_error or not forced_obj:
        return None, None, None, f"action.type 不在 allow 内（allow={allowed_text}）"
    if not forced_type or forced_type not in allowed_set:
        return None, None, None, f"action.type 不在 allow 内（allow={allowed_text} got={forced_type}）"

    return forced_obj, forced_type, forced_payload or {}, None


def _is_run_stopped(*, run_id: int) -> bool:
    """
    读取 run 实时状态，检测是否已被外部取消收敛到 stopped。

    说明：
    - SSE 断连后，生命周期线程会把 run 标记为 stopped；
    - 执行循环需要尽快感知并停止，避免“取消后继续落步骤”。
    """
    try:
        row = get_task_run(run_id=int(run_id))
    except Exception:
        return False
    if not row:
        return False
    status = str(row["status"] or "").strip().lower()
    return status == str(RUN_STATUS_STOPPED)


def run_react_loop_impl(
    *,
    task_id: int,
    run_id: int,
    message: str,
    workdir: str,
    model: str,
    parameters: dict,
    plan_struct: PlanStructure,
    tools_hint: str,
    skills_hint: str,
    memories_hint: str,
    graph_hint: str,
    agent_state: Dict,
    context: Dict,
    observations: List[str],
    start_step_order: int,
    variables_source: str,
    llm_call: Callable[[dict], dict],
    execute_step_action: Callable[..., tuple[Optional[dict], Optional[str]]],
    step_llm_config_resolver: Optional[
        Callable[[int, str, List[str]], Tuple[Optional[str], Optional[dict]]]
    ] = None,
) -> Generator[str, None, ReactLoopResult]:
    """
    ReAct 执行循环（新 run 与 resume 共用）。

    约定：
    - 每个 step 只执行一条 action，并落库到 task_steps / task_outputs / llm_records 等表
    - 支持 plan_patch（仅允许改下一步 k+1），并立即推送计划栏更新
    - 支持 user_prompt：进入 waiting，等待前端用 /agent/command/resume/stream 继续执行
    """
    run_status = RUN_STATUS_DONE
    last_step_order = max(0, int(start_step_order) - 1)

    # 保险丝：plan 为空时直接失败，避免 idx=-1 等越界错误（常见于旧 run/损坏数据）
    if plan_struct.step_count == 0:
        _safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.react.empty_plan",
            data={"start_step_order": int(start_step_order)},
            level="warning",
        )
        yield sse_json({"delta": f"{STREAM_TAG_FAIL} 计划为空，无法执行\n"})
        return ReactLoopResult(run_status=RUN_STATUS_FAILED, last_step_order=0, plan_struct=plan_struct)

    react_params = dict(parameters or {})
    react_params.setdefault("temperature", 0.2)

    start = int(start_step_order)
    if start < 1:
        start = 1
    if start > plan_struct.step_count:
        start = plan_struct.step_count

    # 开发阶段：执行链路不限制 max_steps，避免评估修复/重规划被上限提前截断。
    max_steps_limit = None

    idx = start - 1

    # 主循环
    while idx < plan_struct.step_count:
        if _is_run_stopped(run_id=int(run_id)):
            _safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.react.run_stopped_detected",
                data={"idx": int(idx)},
                level="warning",
            )
            run_status = RUN_STATUS_STOPPED
            break

        step_order = idx + 1
        last_step_order = step_order
        step = plan_struct.get_step(idx)
        title = step.title if step else ""

        # 结算上一步状态
        plan_struct.mark_running_as_done()

        allowed: List[str] = step.allow if step else []
        allowed_text = " / ".join(allowed) if allowed else "(未限制)"

        step_model = model
        step_react_params = react_params
        step_llm_overrides: Dict = {}
        if step_llm_config_resolver:
            try:
                resolved_model, resolved_params = step_llm_config_resolver(step_order, title, allowed)
            except Exception as exc:
                _safe_write_debug(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    message="agent.step_llm_config_resolver.failed",
                    data={"step_order": int(step_order), "title": str(title), "error": str(exc)},
                    level="warning",
                )
                resolved_model, resolved_params = None, None
            if isinstance(resolved_model, str) and resolved_model.strip():
                step_model = resolved_model.strip()
            if isinstance(resolved_params, dict):
                step_llm_overrides = dict(resolved_params)
                step_react_params = dict(react_params)
                step_react_params.update(step_llm_overrides)

        # 评估门闩
        if is_task_feedback_step_title(title) and (not bool(agent_state.get("task_feedback_asked"))) and idx > 0:
            inserted = yield from maybe_apply_review_gate_before_feedback(
                task_id=int(task_id),
                run_id=int(run_id),
                idx=int(idx),
                title=title,
                model=step_model,
                react_params=step_react_params,
                variables_source=variables_source,
                llm_call=llm_call,
                plan_struct=plan_struct,
                max_steps_limit=max_steps_limit,
                agent_state=agent_state,
                safe_write_debug=_safe_write_debug,
            )
            if inserted:
                continue

        # 任务闭环：确认满意度
        if is_task_feedback_step_title(title):
            outcome = yield from handle_task_feedback_step(
                task_id=int(task_id),
                run_id=int(run_id),
                idx=int(idx),
                step_order=int(step_order),
                title=title,
                message=message,
                workdir=workdir,
                model=step_model,
                react_params=step_react_params,
                variables_source=variables_source,
                tools_hint=tools_hint,
                skills_hint=skills_hint,
                memories_hint=memories_hint,
                graph_hint=graph_hint,
                plan_struct=plan_struct,
                agent_state=agent_state,
                context=context,
                observations=observations,
                max_steps_limit=max_steps_limit,
                run_replan_and_merge=run_replan_and_merge,
                safe_write_debug=_safe_write_debug,
            )
            if outcome.run_status:
                run_status = outcome.run_status
                break
            if outcome.next_idx is not None:
                idx = outcome.next_idx
                continue

        # artifacts 门闩
        artifacts_outcome = yield from apply_artifacts_gates(
            task_id=int(task_id),
            run_id=int(run_id),
            idx=int(idx),
            step_order=int(step_order),
            title=title,
            workdir=workdir,
            message=message,
            model=step_model,
            react_params=step_react_params,
            tools_hint=tools_hint,
            skills_hint=skills_hint,
            memories_hint=memories_hint,
            graph_hint=graph_hint,
            allowed=allowed,
            plan_struct=plan_struct,
            agent_state=agent_state,
            observations=observations,
            max_steps_limit=max_steps_limit,
            run_replan_and_merge=run_replan_and_merge,
            safe_write_debug=_safe_write_debug,
        )
        if artifacts_outcome.run_status:
            run_status = artifacts_outcome.run_status
            break
        if artifacts_outcome.next_idx is not None:
            idx = artifacts_outcome.next_idx
            continue

        # 当前步 -> running
        plan_struct.set_step_status(idx, "running")
        yield sse_plan_delta(task_id=task_id, run_id=run_id, plan_items=plan_struct.get_items_payload(), indices=[idx])

        # 尽早落库 running
        persist_loop_state(
            run_id=run_id,
            plan_struct=plan_struct,
            agent_state=agent_state,
            step_order=step_order,
            observations=observations,
            context=context,
            safe_write_debug=_safe_write_debug,
            task_id=task_id,
            where="before_step",
            # running 状态变更必须落盘：若此处被节流丢弃，崩溃后 resume 将从错误的步骤启动。
            force=True,
        )

        yield sse_json({"delta": f"{STREAM_TAG_STEP} {title}\n"})

        obs_text = "\n".join(f"- {_truncate_observation(o)}" for o in observations[-3:]) or "(无)"
        source_failure_summary = summarize_recent_source_failures_for_prompt(
            observations=list(observations or []),
            failure_signatures=(
                agent_state.get("failure_signatures")
                if isinstance(agent_state, dict) and isinstance(agent_state.get("failure_signatures"), dict)
                else None
            ),
        )

        budget_meta: dict = {}
        react_prompt = build_react_step_prompt(
            now_utc=now_iso(),
            workdir=workdir,
            message=message,
            plan=plan_struct.get_titles_json(),
            step_index=step_order,
            step_title=title,
            allowed_actions=allowed_text,
            observations=obs_text,
            recent_source_failures=source_failure_summary,
            graph=graph_hint,
            tools=tools_hint,
            skills=skills_hint,
            memories=memories_hint,
            capability_hint=build_capability_hint(
                capability=resolve_step_capability(
                    allowed_actions=list(allowed or []),
                    step_title=title,
                )
            ),
            budget_meta_sink=budget_meta,
        )
        if isinstance(agent_state, dict):
            agent_state["context_budget_last_meta"] = dict(budget_meta or {})

        # 生成 action
        action_obj, action_type, payload_obj, action_validate_error, last_action_text = generate_action_with_retry(
            llm_call=llm_call,
            react_prompt=react_prompt,
            task_id=task_id,
            run_id=run_id,
            step_order=step_order,
            step_title=title,
            workdir=workdir,
            model=step_model,
            react_params=step_react_params,
            variables_source=variables_source,
        )

        # 处理 action 验证失败
        if action_validate_error or not action_obj:
            status, next_idx = yield from handle_action_invalid(
                task_id=task_id,
                run_id=run_id,
                step_order=step_order,
                idx=idx,
                title=title,
                message=message,
                workdir=workdir,
                model=step_model,
                react_params=step_react_params,
                tools_hint=tools_hint,
                skills_hint=skills_hint,
                memories_hint=memories_hint,
                graph_hint=graph_hint,
                action_validate_error=action_validate_error or "invalid_action",
                last_action_text=last_action_text,
                plan_struct=plan_struct,
                agent_state=agent_state,
                context=context,
                observations=observations,
                max_steps_limit=max_steps_limit,
                run_replan_and_merge=run_replan_and_merge,
                safe_write_debug=_safe_write_debug,
            )
            if status:
                run_status = status
                break
            if next_idx is not None:
                idx = next_idx
                continue
            idx += 1
            continue

        # allow 约束检查
        action_obj, action_type, payload_obj, allow_err = _enforce_allow_constraints(
            task_id=int(task_id),
            run_id=int(run_id),
            step_order=int(step_order),
            step_title=title,
            workdir=workdir,
            allowed=allowed,
            allowed_text=allowed_text,
            action_obj=action_obj,
            action_type=action_type,
            payload_obj=payload_obj or {},
            react_prompt=react_prompt,
            model=step_model,
            react_params=step_react_params,
            variables_source=variables_source,
            llm_call=llm_call,
        )

        if allow_err or not action_obj or not action_type:
            status, next_idx = yield from handle_allow_failure(
                task_id=task_id,
                run_id=run_id,
                step_order=step_order,
                idx=idx,
                title=title,
                message=message,
                workdir=workdir,
                model=step_model,
                react_params=step_react_params,
                tools_hint=tools_hint,
                skills_hint=skills_hint,
                memories_hint=memories_hint,
                graph_hint=graph_hint,
                allow_err=allow_err or "action.type 不在 allow 内",
                plan_struct=plan_struct,
                agent_state=agent_state,
                context=context,
                observations=observations,
                max_steps_limit=max_steps_limit,
                run_replan_and_merge=run_replan_and_merge,
                safe_write_debug=_safe_write_debug,
            )
            if status:
                run_status = status
                break
            if next_idx is not None:
                idx = next_idx
                continue
            idx += 1
            continue

        # stop 保护：LLM 生成 action 后，可能在“计划补丁落库前”收到 stop。
        # 这里必须先检查一次，避免出现 stopped 后仍写入 plan_patch 的竞态。
        if _is_run_stopped(run_id=int(run_id)):
            _safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.react.run_stopped_before_plan_patch",
                data={"step_order": int(step_order)},
                level="warning",
            )
            run_status = RUN_STATUS_STOPPED
            break

        # plan_patch 处理
        patch_obj = action_obj.get("plan_patch")
        if isinstance(patch_obj, dict):
            # apply_next_step_patch 仍使用 legacy 列表接口（后续可进一步收编到 PlanStructure）
            patch_titles, patch_items, patch_allows, patch_artifacts = plan_struct.to_legacy_lists()
            patch_err = apply_next_step_patch(
                current_step_index=step_order,
                patch_obj=patch_obj,
                plan_titles=patch_titles,
                plan_items=patch_items,
                plan_allows=patch_allows,
                plan_artifacts=patch_artifacts,
                max_steps=max_steps_limit,
            )
            if patch_err:
                yield sse_json({"delta": f"{STREAM_TAG_FAIL} plan_patch 不合法（{patch_err}），已忽略\n"})
            else:
                patched_plan = PlanStructure.from_legacy(
                    plan_titles=patch_titles,
                    plan_items=patch_items,
                    plan_allows=patch_allows,
                    plan_artifacts=patch_artifacts,
                )
                plan_struct.replace_from(patched_plan)
                _safe_write_debug(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    message="agent.plan_patch.applied",
                    data={"current_step_order": int(step_order), "patch": patch_obj},
                    level="info",
                )
                yield sse_plan(task_id=task_id, run_id=run_id, plan_items=plan_struct.get_items_payload())
                persist_plan_only(
                    run_id=run_id,
                    plan_struct=plan_struct,
                    safe_write_debug=_safe_write_debug,
                    task_id=task_id,
                    step_order=step_order,
                    where="after_plan_patch",
                )

        # task_output.content 兜底
        if action_type == ACTION_TYPE_TASK_OUTPUT:
            forced_obj, forced_type, forced_payload, fallback_err = handle_task_output_fallback(
                llm_call=llm_call,
                react_prompt=react_prompt,
                task_id=task_id,
                run_id=run_id,
                step_order=step_order,
                title=title,
                workdir=workdir,
                model=step_model,
                react_params=step_react_params,
                variables_source=variables_source,
                payload_obj=payload_obj,
                context=context,
                safe_write_debug=_safe_write_debug,
            )
            if fallback_err:
                run_status = RUN_STATUS_FAILED
                plan_struct.set_step_status(idx, "failed")
                yield sse_plan_delta(
                    task_id=task_id, run_id=run_id, plan_items=plan_struct.get_items_payload(), indices=[idx]
                )
                yield sse_json({"delta": f"{STREAM_TAG_FAIL} {fallback_err}\n"})
                break
            if forced_obj:
                action_obj = forced_obj
                action_type = forced_type
                payload_obj = forced_payload or {}

        # user_prompt 处理
        if action_type == ACTION_TYPE_USER_PROMPT:
            status, should_break = yield from handle_user_prompt_action(
                task_id=task_id,
                run_id=run_id,
                step_order=step_order,
                title=title,
                payload_obj=payload_obj,
                plan_struct=plan_struct,
                agent_state=agent_state,
                safe_write_debug=_safe_write_debug,
            )
            if should_break:
                run_status = status
                break

        # Think 模式：llm_call 执行也要按 executor 选择的模型/参数运行。
        # 说明：上游会移除 LLM 自己写的 model/provider，避免“模型写错不可用”；这里仅注入服务端解析出的配置。
        if action_type == ACTION_TYPE_LLM_CALL and step_llm_config_resolver:
            payload_obj["model"] = step_model
            if step_llm_overrides:
                params = payload_obj.get("parameters")
                if not isinstance(params, dict):
                    params = {}
                merged = dict(params)
                merged.update(step_llm_overrides)
                payload_obj["parameters"] = merged

        # docs/agent：Think 模式需要把 executor 角色落到 task_steps.executor 便于审计/复盘。
        # 约定：executor 由上游（think runner）写入 agent_state.executor_assignments，再由执行阶段按 step_order 查表。
        executor_value = resolve_executor(agent_state, step_order)

        if _is_run_stopped(run_id=int(run_id)):
            _safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.react.run_stopped_before_step_persist",
                data={"step_order": int(step_order)},
                level="warning",
            )
            run_status = RUN_STATUS_STOPPED
            break

        # 执行步骤
        detail = json.dumps({"type": action_type, "payload": payload_obj}, ensure_ascii=False)
        step_created_at = now_iso()
        try:
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
        except Exception as create_step_exc:
            _safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.react.create_task_step_failed",
                data={"step_order": int(step_order), "error": str(create_step_exc)},
                level="error",
            )
            run_status = RUN_STATUS_FAILED
            break

        step_row = {"id": step_id, "title": title, "detail": detail}
        result, step_error = execute_step_action(int(task_id), int(run_id), step_row, context=context)
        finished_at = now_iso()

        if _is_run_stopped(run_id=int(run_id)):
            _safe_write_debug(
                task_id=int(task_id),
                run_id=int(run_id),
                message="agent.react.run_stopped_after_step_execution",
                data={"step_order": int(step_order), "step_id": int(step_id)},
                level="warning",
            )
            run_status = RUN_STATUS_STOPPED
            break

        # 处理步骤失败
        if step_error:
            status, next_idx = yield from handle_step_failure(
                task_id=task_id,
                run_id=run_id,
                step_id=step_id,
                step_order=step_order,
                idx=idx,
                title=title,
                message=message,
                workdir=workdir,
                model=step_model,
                react_params=step_react_params,
                tools_hint=tools_hint,
                skills_hint=skills_hint,
                memories_hint=memories_hint,
                graph_hint=graph_hint,
                action_type=action_type,
                step_error=step_error,
                plan_struct=plan_struct,
                agent_state=agent_state,
                context=context,
                observations=observations,
                max_steps_limit=max_steps_limit,
                run_replan_and_merge=run_replan_and_merge,
                safe_write_debug=_safe_write_debug,
                mark_task_step_failed=mark_task_step_failed,
                finished_at=finished_at,
            )
            if status:
                run_status = status
                break
            if next_idx is not None:
                idx = next_idx
                continue
            idx += 1
            continue

        # 步骤成功
        result_value = None
        if result is not None:
            try:
                result_value = json.dumps(result, ensure_ascii=False)
            except Exception:
                result_value = json.dumps({"text": str(result)}, ensure_ascii=False)

        mark_task_step_done(
            step_id=int(step_id),
            result=result_value,
            finished_at=finished_at,
        )

        yield sse_json({"delta": f"{STREAM_TAG_OK} {title}\n"})

        # 构建观测
        obs_line, visible_content = build_observation_line(
            action_type=action_type,
            title=title,
            result=result,
            context=context,
        )
        observations.append(obs_line)

        # 特殊输出处理
        if visible_content:
            yield yield_visible_result(visible_content)

        if action_type == ACTION_TYPE_MEMORY_WRITE and isinstance(result, dict):
            yield yield_memory_write_event(task_id=task_id, run_id=run_id, result=result)

        # 结算计划栏状态：步骤成功 -> done
        plan_struct.set_step_status(idx, "done")
        yield sse_plan_delta(task_id=task_id, run_id=run_id, plan_items=plan_struct.get_items_payload(), indices=[idx])

        # 持久化状态
        persist_loop_state(
            run_id=run_id,
            plan_struct=plan_struct,
            agent_state=agent_state,
            step_order=step_order + 1,
            observations=observations,
            context=context,
            safe_write_debug=_safe_write_debug,
            task_id=task_id,
            where="after_step",
            # 步骤结算必须落盘：避免节流吞掉 done/failed 状态，影响可恢复性。
            force=True,
        )

        idx += 1

    return ReactLoopResult(run_status=run_status, last_step_order=last_step_order, plan_struct=plan_struct)
