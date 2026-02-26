# -*- coding: utf-8 -*-
"""
ReAct 循环错误处理模块。

提供动作验证失败、allow 约束失败、步骤执行失败的统一处理逻辑。
"""

import json
import logging
import re
from typing import Callable, Dict, Generator, List, Optional, Tuple

from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.support import _truncate_observation
from backend.src.agent.runner.react_error_policy import should_force_replan_on_action_error
from backend.src.agent.runner.react_state_manager import (
    ReplanContext,
    prepare_replan_context,
    persist_loop_state,
    resolve_executor,
)
from backend.src.agent.runner.plan_events import sse_plan, sse_plan_delta
from backend.src.agent.runner.react_step_executor import record_invalid_action_step
from backend.src.constants import (
    AGENT_REACT_REPEAT_FAILURE_MAX,
    RUN_STATUS_FAILED,
    STREAM_TAG_EXEC,
    STREAM_TAG_FAIL,
)
from backend.src.common.utils import coerce_int
from backend.src.common.task_error_codes import extract_task_error_code
from backend.src.services.llm.llm_client import sse_json

logger = logging.getLogger(__name__)


def _normalize_failure_signature(*, action_type: str, step_error: str) -> str:
    action = str(action_type or "").strip().lower() or "unknown_action"
    error_text = str(step_error or "").strip()
    error_code = extract_task_error_code(error_text)
    if error_code:
        return f"{action}|code:{error_code}"

    head = error_text.splitlines()[0].strip() if error_text else "unknown_error"
    lowered = head.lower()
    # 归一化路径/日期/数字，减少“同类错误因动态文本不同”导致的签名漂移。
    lowered = re.sub(r"[a-z]:\\\\[^\\s]+", "<path>", lowered)
    lowered = re.sub(r"\b\d{4}-\d{2}-\d{2}\b", "<date>", lowered)
    lowered = re.sub(r"\b\d+\b", "<n>", lowered)
    lowered = " ".join(lowered.split())
    if not lowered:
        lowered = "unknown_error"
    return f"{action}|msg:{lowered[:180]}"


def _record_failure_signature(*, agent_state: Dict, action_type: str, step_error: str) -> tuple[str, int]:
    signature = _normalize_failure_signature(action_type=action_type, step_error=step_error)
    stats = agent_state.get("failure_signatures") if isinstance(agent_state, dict) else None
    if not isinstance(stats, dict):
        stats = {}

    existing = stats.get(signature)
    if isinstance(existing, dict):
        count = coerce_int(existing.get("count"), default=0) + 1
    else:
        count = 1

    stats[signature] = {"count": coerce_int(count, default=1)}

    # 防止状态无限膨胀：仅保留最近 20 个错误签名。
    if len(stats) > 20:
        removable = [k for k in stats.keys() if k != signature]
        for key in removable[: max(0, len(stats) - 20)]:
            stats.pop(key, None)

    agent_state["failure_signatures"] = stats
    return signature, coerce_int(count, default=1)



def handle_action_invalid(
    *,
    task_id: int,
    run_id: int,
    step_order: int,
    idx: int,
    title: str,
    message: str,
    workdir: str,
    model: str,
    react_params: dict,
    tools_hint: str,
    skills_hint: str,
    memories_hint: str,
    graph_hint: str,
    action_validate_error: str,
    last_action_text: Optional[str],
    plan_struct: PlanStructure,
    agent_state: Dict,
    context: Dict,
    observations: List[str],
    max_steps_limit: Optional[int],
    run_replan_and_merge: Callable,
    safe_write_debug: Callable,
) -> Generator[str, None, Tuple[str, Optional[int]]]:
    """
    处理 action 验证失败的情况。

    Yields:
        SSE 事件

    Returns:
        (run_status, next_idx)
        run_status 为空字符串表示继续执行，非空表示终止
        next_idx 为 None 表示正常递增，否则跳转到指定索引
    """
    will_continue = step_order < plan_struct.step_count

    # 更新计划栏状态
    plan_struct.set_step_status(idx, "failed")
    yield sse_plan_delta(task_id=task_id, run_id=run_id, plan_items=plan_struct.get_items_payload(), indices=[idx])

    # 输出错误信息
    if action_validate_error in {"empty_response", "action 输出不是有效 JSON"}:
        yield sse_json({"delta": f"{STREAM_TAG_FAIL} action 输出不是有效 JSON\n"})
    else:
        yield sse_json({"delta": f"{STREAM_TAG_FAIL} action 不合法（{action_validate_error}）\n"})

    # 调试输出
    safe_write_debug(
        task_id=int(task_id),
        run_id=int(run_id),
        message="agent.react.action_invalid",
        data={
            "step_order": int(step_order),
            "error": str(action_validate_error),
            "last_action_text": _truncate_observation(str(last_action_text or "")),
            "will_continue": bool(will_continue),
        },
        level="warning",
    )

    # 记录失败步骤
    executor_value = resolve_executor(agent_state, step_order)

    record_invalid_action_step(
        task_id=task_id,
        run_id=run_id,
        step_order=step_order,
        title=title,
        executor=executor_value,
        error=action_validate_error,
        last_action_text=last_action_text,
        safe_write_debug=safe_write_debug,
    )

    # 添加观测
    observations.append(f"{title}: FAIL action_invalid {action_validate_error}")

    # 判断是否需要强制 replan
    force_replan = should_force_replan_on_action_error(str(action_validate_error or ""))

    # 尝试 replan
    if force_replan or not will_continue:
        replan_ctx = prepare_replan_context(
            step_order=step_order,
            agent_state=agent_state,
            max_steps_limit=max_steps_limit,
            plan_titles=plan_struct.get_titles(),
        )

        if replan_ctx.can_replan:
            sse_notice = f"{STREAM_TAG_EXEC} action 不合法，重新规划剩余步骤…" if force_replan else f"{STREAM_TAG_EXEC} 动作解析失败，重新规划剩余步骤…"

            replan_result = yield from run_replan_and_merge(
                task_id=int(task_id),
                run_id=int(run_id),
                message=message,
                workdir=workdir,
                model=model,
                react_params=react_params,
                max_steps_value=replan_ctx.max_steps_value,
                tools_hint=tools_hint,
                skills_hint=skills_hint,
                memories_hint=memories_hint,
                graph_hint=graph_hint,
                plan_struct=plan_struct,
                agent_state=agent_state,
                observations=observations,
                done_count=replan_ctx.done_count,
                error=str(action_validate_error or "action_invalid"),
                sse_notice=sse_notice,
                replan_attempts=replan_ctx.replan_attempts,
                safe_write_debug=safe_write_debug,
            )

            if replan_result:
                # replan 成功，替换计划
                plan_struct.replace_from(replan_result.plan_struct)

                yield sse_plan(task_id=task_id, run_id=run_id, plan_items=plan_struct.get_items_payload())

                persist_loop_state(
                    run_id=run_id,
                    plan_struct=plan_struct,
                    agent_state=agent_state,
                    step_order=step_order,
                    observations=observations,
                    context=context,
                    safe_write_debug=safe_write_debug,
                    task_id=task_id,
                    where="after_action_invalid_replan",
                    # replan 合并属于关键状态：必须落盘，保证 resume 与审计一致。
                    force=True,
                )

                # 跳转到已完成步骤的下一步
                return "", replan_ctx.done_count

    # replan 未执行或失败，继续下一步或终止
    persist_loop_state(
        run_id=run_id,
        plan_struct=plan_struct,
        agent_state=agent_state,
        step_order=step_order + 1,
        observations=observations,
        context=context,
        safe_write_debug=safe_write_debug,
        task_id=task_id,
        where="after_action_invalid",
        # action 无效/失败后的状态变更属于关键节点：必须落盘。
        force=True,
    )

    if will_continue:
        if force_replan:
            yield sse_json({"delta": f"{STREAM_TAG_FAIL} 动作生成失败且无法恢复，终止本轮执行。\n"})
            return RUN_STATUS_FAILED, None
        # 继续下一步
        return "", idx + 1

    # 计划耗尽，终止
    return RUN_STATUS_FAILED, None


def handle_allow_failure(
    *,
    task_id: int,
    run_id: int,
    step_order: int,
    idx: int,
    title: str,
    message: str,
    workdir: str,
    model: str,
    react_params: dict,
    tools_hint: str,
    skills_hint: str,
    memories_hint: str,
    graph_hint: str,
    allow_err: str,
    plan_struct: PlanStructure,
    agent_state: Dict,
    context: Dict,
    observations: List[str],
    max_steps_limit: Optional[int],
    run_replan_and_merge: Callable,
    safe_write_debug: Callable,
) -> Generator[str, None, Tuple[str, Optional[int]]]:
    """
    处理 allow 约束验证失败的情况。

    Returns:
        (run_status, next_idx)
    """
    will_continue = step_order < plan_struct.step_count

    # 更新计划栏状态（与 handle_action_invalid / handle_step_failure 保持一致：使用 idx 而非 step_order-1）
    plan_struct.set_step_status(idx, "failed")
    yield sse_plan_delta(task_id=task_id, run_id=run_id, plan_items=plan_struct.get_items_payload(), indices=[idx])

    yield sse_json({"delta": f"{STREAM_TAG_FAIL} {allow_err or 'action.type 不在 allow 内'}\n"})

    safe_write_debug(
        task_id=int(task_id),
        run_id=int(run_id),
        message="agent.react.allow_failed",
        data={"step_order": int(step_order), "error": str(allow_err or ""), "will_continue": bool(will_continue)},
        level="warning",
    )

    observations.append(f"{title}: FAIL allow {allow_err or 'action.type_not_allowed'}")

    # 持久化状态
    persist_loop_state(
        run_id=run_id,
        plan_struct=plan_struct,
        agent_state=agent_state,
        step_order=step_order + 1,
        observations=observations,
        context=context,
        safe_write_debug=safe_write_debug,
        task_id=task_id,
        where="after_allow_failed",
        # allow 失败会影响后续 resume/诊断：必须落盘。
        force=True,
    )

    if will_continue:
        # 统一策略：优先尝试 replan 修复（与 handle_step_failure 保持一致），
        # 避免 allow 失败后盲目跳下一步导致连锁失败。
        replan_ctx = prepare_replan_context(
            step_order=step_order,
            agent_state=agent_state,
            max_steps_limit=max_steps_limit,
            plan_titles=plan_struct.get_titles(),
        )

        if replan_ctx.can_replan:
            replan_result = yield from run_replan_and_merge(
                task_id=int(task_id),
                run_id=int(run_id),
                message=message,
                workdir=workdir,
                model=model,
                react_params=react_params,
                max_steps_value=replan_ctx.max_steps_value,
                tools_hint=tools_hint,
                skills_hint=skills_hint,
                memories_hint=memories_hint,
                graph_hint=graph_hint,
                plan_struct=plan_struct,
                agent_state=agent_state,
                observations=observations,
                done_count=replan_ctx.done_count,
                error=str(allow_err or "allow_failed"),
                sse_notice=f"{STREAM_TAG_EXEC} allow 约束未满足，重新规划剩余步骤…",
                replan_attempts=replan_ctx.replan_attempts,
                safe_write_debug=safe_write_debug,
            )

            if replan_result:
                plan_struct.replace_from(replan_result.plan_struct)
                return "", replan_ctx.done_count

        # replan 不可用或失败，降级跳下一步
        return "", idx + 1

    # 计划耗尽，尝试 replan
    replan_ctx = prepare_replan_context(
        step_order=step_order,
        agent_state=agent_state,
        max_steps_limit=max_steps_limit,
        plan_titles=plan_struct.get_titles(),
    )

    if replan_ctx.can_replan:
        replan_result = yield from run_replan_and_merge(
            task_id=int(task_id),
            run_id=int(run_id),
            message=message,
            workdir=workdir,
            model=model,
            react_params=react_params,
            max_steps_value=replan_ctx.max_steps_value,
            tools_hint=tools_hint,
            skills_hint=skills_hint,
            memories_hint=memories_hint,
            graph_hint=graph_hint,
            plan_struct=plan_struct,
            agent_state=agent_state,
            observations=observations,
            done_count=replan_ctx.done_count,
            error=str(allow_err or "allow_failed"),
            sse_notice=f"{STREAM_TAG_EXEC} allow 约束未满足，重新规划剩余步骤…",
            replan_attempts=replan_ctx.replan_attempts,
            safe_write_debug=safe_write_debug,
        )

        if replan_result:
            plan_struct.replace_from(replan_result.plan_struct)
            return "", replan_ctx.done_count

    return RUN_STATUS_FAILED, None


def handle_step_failure(
    *,
    task_id: int,
    run_id: int,
    step_id: int,
    step_order: int,
    idx: int,
    title: str,
    message: str,
    workdir: str,
    model: str,
    react_params: dict,
    tools_hint: str,
    skills_hint: str,
    memories_hint: str,
    graph_hint: str,
    action_type: str,
    step_error: str,
    plan_struct: PlanStructure,
    agent_state: Dict,
    context: Dict,
    observations: List[str],
    max_steps_limit: Optional[int],
    run_replan_and_merge: Callable,
    safe_write_debug: Callable,
    mark_task_step_failed: Callable,
    finished_at: str,
) -> Generator[str, None, Tuple[str, Optional[int]]]:
    """
    处理步骤执行失败的情况。

    Returns:
        (run_status, next_idx)
    """
    will_continue = step_order < plan_struct.step_count
    failure_signature, failure_hit_count = _record_failure_signature(
        agent_state=agent_state,
        action_type=str(action_type or ""),
        step_error=str(step_error or ""),
    )
    repeat_failure_limit = coerce_int(AGENT_REACT_REPEAT_FAILURE_MAX, default=0)
    if repeat_failure_limit < 0:
        repeat_failure_limit = 0
    repeat_failure_exceeded = repeat_failure_limit > 0 and coerce_int(
        failure_hit_count, default=0
    ) >= repeat_failure_limit
    if repeat_failure_exceeded:
        agent_state["critical_failure"] = True
        agent_state["critical_failure_reason"] = "repeat_failure_budget_exceeded"

    safe_write_debug(
        task_id=int(task_id),
        run_id=int(run_id),
        message="agent.step.failed",
        data={
            "step_id": int(step_id),
            "step_order": int(step_order),
            "title": title,
            "action_type": action_type,
            "error": step_error,
            "will_continue": bool(will_continue),
            "failure_signature": failure_signature,
            "failure_hit_count": coerce_int(failure_hit_count, default=0),
            "failure_budget": coerce_int(repeat_failure_limit, default=0),
            "failure_budget_exceeded": bool(repeat_failure_exceeded),
        },
        level="warning",
    )

    mark_task_step_failed(
        step_id=int(step_id),
        error=str(step_error),
        finished_at=finished_at,
    )

    yield sse_json({"delta": f"{STREAM_TAG_FAIL} {title}（{step_error}）\n"})

    plan_struct.set_step_status(idx, "failed")
    yield sse_plan_delta(task_id=task_id, run_id=run_id, plan_items=plan_struct.get_items_payload(), indices=[idx])

    observations.append(f"{title}: FAIL {step_error}")
    # 失败后清空"最近可解析源"，避免后续 json_parse 继续消费陈旧结果。
    if isinstance(context, dict):
        context.pop("latest_parse_input_text", None)

    # 持久化失败状态
    persist_loop_state(
        run_id=run_id,
        plan_struct=plan_struct,
        agent_state=agent_state,
        step_order=step_order + 1,
        observations=observations,
        context=context,
        safe_write_debug=safe_write_debug,
        task_id=task_id,
        where="after_step_failed",
        # 步骤失败结算必须落盘：避免节流吞掉 failed 状态，影响可恢复性。
        force=True,
    )

    if repeat_failure_exceeded:
        safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.failure_budget.exceeded",
            data={
                "signature": failure_signature,
                "count": int(failure_hit_count),
                "budget": int(repeat_failure_limit),
                "step_order": int(step_order),
                "action_type": str(action_type or ""),
            },
            level="warning",
        )
        yield sse_json(
            {
                "delta": (
                    f"{STREAM_TAG_FAIL} 同类失败已连续出现 {int(failure_hit_count)} 次，"
                    "停止自动重试并终止本轮执行。\n"
                )
            }
        )
        return RUN_STATUS_FAILED, None

    # 失败后优先尝试 replan
    if will_continue:
        replan_ctx = prepare_replan_context(
            step_order=step_order,
            agent_state=agent_state,
            max_steps_limit=max_steps_limit,
            plan_titles=plan_struct.get_titles(),
        )

        if replan_ctx.can_replan:
            replan_result = yield from run_replan_and_merge(
                task_id=int(task_id),
                run_id=int(run_id),
                message=message,
                workdir=workdir,
                model=model,
                react_params=react_params,
                max_steps_value=replan_ctx.max_steps_value,
                tools_hint=tools_hint,
                skills_hint=skills_hint,
                memories_hint=memories_hint,
                graph_hint=graph_hint,
                plan_struct=plan_struct,
                agent_state=agent_state,
                observations=observations,
                done_count=replan_ctx.done_count,
                error=str(step_error),
                sse_notice="",  # 静默 replan
                replan_attempts=replan_ctx.replan_attempts,
                safe_write_debug=safe_write_debug,
            )

            if replan_result:
                plan_struct.replace_from(replan_result.plan_struct)
                return "", replan_ctx.done_count

        # replan 失败或不可用，继续下一步
        return "", idx + 1

    # 计划耗尽，尝试最后的 replan
    replan_ctx = prepare_replan_context(
        step_order=step_order,
        agent_state=agent_state,
        max_steps_limit=max_steps_limit,
        plan_titles=plan_struct.get_titles(),
    )

    if replan_ctx.remaining_limit is not None and replan_ctx.remaining_limit <= 0:
        yield sse_json({"delta": f"{STREAM_TAG_FAIL} 计划已耗尽且剩余步数不足以继续（max_steps={max_steps_limit}）。\n"})
        return RUN_STATUS_FAILED, None

    if not replan_ctx.can_replan:
        yield sse_json({"delta": f"{STREAM_TAG_FAIL} 计划已耗尽且重新规划次数已达上限。\n"})
        return RUN_STATUS_FAILED, None

    replan_result = yield from run_replan_and_merge(
        task_id=int(task_id),
        run_id=int(run_id),
        message=message,
        workdir=workdir,
        model=model,
        react_params=react_params,
        max_steps_value=replan_ctx.max_steps_value,
        tools_hint=tools_hint,
        skills_hint=skills_hint,
        memories_hint=memories_hint,
        graph_hint=graph_hint,
        plan_struct=plan_struct,
        agent_state=agent_state,
        observations=observations,
        done_count=replan_ctx.done_count,
        error=str(step_error),
        sse_notice=f"{STREAM_TAG_EXEC} 计划已耗尽，重新规划剩余步骤…",
        replan_attempts=replan_ctx.replan_attempts,
        safe_write_debug=safe_write_debug,
    )

    if replan_result:
        plan_struct.replace_from(replan_result.plan_struct)
        return "", replan_ctx.done_count

    return RUN_STATUS_FAILED, None
