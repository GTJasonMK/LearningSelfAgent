# -*- coding: utf-8 -*-
"""
ReAct 循环错误处理模块。

提供动作验证失败、allow 约束失败、步骤执行失败的统一处理逻辑。
"""

import json
import logging
from typing import Callable, Dict, Generator, List, Optional, Tuple

from backend.src.agent.support import _truncate_observation
from backend.src.agent.runner.react_state_manager import (
    ReplanContext,
    prepare_replan_context,
    persist_loop_state,
)
from backend.src.agent.runner.plan_events import sse_plan, sse_plan_delta
from backend.src.agent.runner.react_step_executor import record_invalid_action_step
from backend.src.constants import (
    RUN_STATUS_FAILED,
    STREAM_TAG_EXEC,
    STREAM_TAG_FAIL,
)
from backend.src.services.llm.llm_client import sse_json

logger = logging.getLogger(__name__)


def should_force_replan_on_action_error(error_text: str) -> bool:
    """
    判断是否应该因 action 错误强制触发 replan。

    Args:
        error_text: 错误文本

    Returns:
        是否应该强制 replan
    """
    try:
        return any(
            key in error_text
            for key in (
                "tool_call.input 不能为空",
                "action 输出不是有效 JSON",
                "action.payload 不是对象",
                "action.type 不能为空",
            )
        )
    except Exception:
        return False


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
    plan_titles: List[str],
    plan_items: List[dict],
    plan_allows: List[List[str]],
    plan_artifacts: List[str],
    agent_state: Dict,
    context: Dict,
    observations: List[str],
    max_steps_limit: Optional[int],
    run_replan_and_merge: Callable,
    safe_write_debug: Callable,
) -> Generator[str, None, Tuple[str, Optional[int], List[str], List[dict], List[List[str]], List[str]]]:
    """
    处理 action 验证失败的情况。

    Args:
        task_id: 任务 ID
        run_id: 执行尝试 ID
        step_order: 步骤序号（1-based）
        idx: 步骤索引（0-based）
        title: 步骤标题
        message: 原始用户消息
        workdir: 工作目录
        model: 模型名称
        react_params: LLM 参数
        tools_hint: 工具提示
        skills_hint: 技能提示
        memories_hint: 记忆提示
        graph_hint: 图谱提示
        action_validate_error: 验证错误信息
        last_action_text: 最后的 action 文本
        plan_titles: 计划标题列表
        plan_items: 计划项列表
        plan_allows: 允许的动作类型列表
        plan_artifacts: 产物列表
        agent_state: Agent 状态字典
        context: 上下文字典
        observations: 观测列表
        max_steps_limit: 最大步骤数限制
        run_replan_and_merge: replan 函数
        safe_write_debug: 调试输出函数

    Yields:
        SSE 事件

    Returns:
        (run_status, next_idx, plan_titles, plan_items, plan_allows, plan_artifacts)
        run_status 为空字符串表示继续执行，非空表示终止
        next_idx 为 None 表示正常递增，否则跳转到指定索引
    """
    will_continue = step_order < len(plan_titles)

    # 更新计划栏状态
    if 0 <= idx < len(plan_items):
        plan_items[idx]["status"] = "failed"
        yield sse_plan_delta(task_id=task_id, plan_items=plan_items, indices=[idx])

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
            plan_titles=plan_titles,
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
                plan_titles=plan_titles,
                plan_items=plan_items,
                plan_allows=plan_allows,
                plan_artifacts=plan_artifacts,
                agent_state=agent_state,
                observations=observations,
                done_count=replan_ctx.done_count,
                error=str(action_validate_error or "action_invalid"),
                sse_notice=sse_notice,
                replan_attempts=replan_ctx.replan_attempts,
                safe_write_debug=safe_write_debug,
            )

            if replan_result:
                # replan 成功，更新计划并持久化
                plan_titles = replan_result.plan_titles
                plan_allows = replan_result.plan_allows
                plan_items = replan_result.plan_items
                plan_artifacts = replan_result.plan_artifacts

                yield sse_plan(task_id=task_id, plan_items=plan_items)

                persist_loop_state(
                    run_id=run_id,
                    plan_titles=plan_titles,
                    plan_items=plan_items,
                    plan_allows=plan_allows,
                    plan_artifacts=plan_artifacts,
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
                return "", replan_ctx.done_count, plan_titles, plan_items, plan_allows, plan_artifacts

    # replan 未执行或失败，继续下一步或终止
    persist_loop_state(
        run_id=run_id,
        plan_titles=plan_titles,
        plan_items=plan_items,
        plan_allows=plan_allows,
        plan_artifacts=plan_artifacts,
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
        # 继续下一步
        return "", idx + 1, plan_titles, plan_items, plan_allows, plan_artifacts

    # 计划耗尽，终止
    return RUN_STATUS_FAILED, None, plan_titles, plan_items, plan_allows, plan_artifacts


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
    plan_titles: List[str],
    plan_items: List[dict],
    plan_allows: List[List[str]],
    plan_artifacts: List[str],
    agent_state: Dict,
    context: Dict,
    observations: List[str],
    max_steps_limit: Optional[int],
    run_replan_and_merge: Callable,
    safe_write_debug: Callable,
) -> Generator[str, None, Tuple[str, Optional[int], List[str], List[dict], List[List[str]], List[str]]]:
    """
    处理 allow 约束验证失败的情况。

    Returns:
        (run_status, next_idx, plan_titles, plan_items, plan_allows, plan_artifacts)
    """
    will_continue = step_order < len(plan_titles)

    # 更新计划栏状态
    if 0 <= step_order - 1 < len(plan_items):
        plan_items[step_order - 1]["status"] = "failed"
        yield sse_plan_delta(task_id=task_id, plan_items=plan_items, indices=[step_order - 1])

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
        plan_titles=plan_titles,
        plan_items=plan_items,
        plan_allows=plan_allows,
        plan_artifacts=plan_artifacts,
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
        return "", idx + 1, plan_titles, plan_items, plan_allows, plan_artifacts

    # 尝试 replan
    replan_ctx = prepare_replan_context(
        step_order=step_order,
        agent_state=agent_state,
        max_steps_limit=max_steps_limit,
        plan_titles=plan_titles,
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
            plan_titles=plan_titles,
            plan_items=plan_items,
            plan_allows=plan_allows,
            plan_artifacts=plan_artifacts,
            agent_state=agent_state,
            observations=observations,
            done_count=replan_ctx.done_count,
            error=str(allow_err or "allow_failed"),
            sse_notice=f"{STREAM_TAG_EXEC} allow 约束未满足，重新规划剩余步骤…",
            replan_attempts=replan_ctx.replan_attempts,
            safe_write_debug=safe_write_debug,
        )

        if replan_result:
            return "", replan_ctx.done_count, replan_result.plan_titles, replan_result.plan_items, replan_result.plan_allows, replan_result.plan_artifacts

    return RUN_STATUS_FAILED, None, plan_titles, plan_items, plan_allows, plan_artifacts


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
    plan_titles: List[str],
    plan_items: List[dict],
    plan_allows: List[List[str]],
    plan_artifacts: List[str],
    agent_state: Dict,
    context: Dict,
    observations: List[str],
    max_steps_limit: Optional[int],
    run_replan_and_merge: Callable,
    safe_write_debug: Callable,
    mark_task_step_failed: Callable,
    finished_at: str,
) -> Generator[str, None, Tuple[str, Optional[int], List[str], List[dict], List[List[str]], List[str]]]:
    """
    处理步骤执行失败的情况。

    Returns:
        (run_status, next_idx, plan_titles, plan_items, plan_allows, plan_artifacts)
    """
    will_continue = step_order < len(plan_titles)

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
        },
        level="warning",
    )

    mark_task_step_failed(
        step_id=int(step_id),
        error=str(step_error),
        finished_at=finished_at,
    )

    yield sse_json({"delta": f"{STREAM_TAG_FAIL} {title}（{step_error}）\n"})

    if 0 <= idx < len(plan_items):
        plan_items[idx]["status"] = "failed"
        yield sse_plan_delta(task_id=task_id, plan_items=plan_items, indices=[idx])

    observations.append(f"{title}: FAIL {step_error}")

    # 持久化失败状态
    persist_loop_state(
        run_id=run_id,
        plan_titles=plan_titles,
        plan_items=plan_items,
        plan_allows=plan_allows,
        plan_artifacts=plan_artifacts,
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

    # 失败后优先尝试 replan
    if will_continue:
        replan_ctx = prepare_replan_context(
            step_order=step_order,
            agent_state=agent_state,
            max_steps_limit=max_steps_limit,
            plan_titles=plan_titles,
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
                plan_titles=plan_titles,
                plan_items=plan_items,
                plan_allows=plan_allows,
                plan_artifacts=plan_artifacts,
                agent_state=agent_state,
                observations=observations,
                done_count=replan_ctx.done_count,
                error=str(step_error),
                sse_notice="",  # 静默 replan
                replan_attempts=replan_ctx.replan_attempts,
                safe_write_debug=safe_write_debug,
            )

            if replan_result:
                return "", replan_ctx.done_count, replan_result.plan_titles, replan_result.plan_items, replan_result.plan_allows, replan_result.plan_artifacts

        # replan 失败或不可用，继续下一步
        return "", idx + 1, plan_titles, plan_items, plan_allows, plan_artifacts

    # 计划耗尽，尝试最后的 replan
    replan_ctx = prepare_replan_context(
        step_order=step_order,
        agent_state=agent_state,
        max_steps_limit=max_steps_limit,
        plan_titles=plan_titles,
    )

    if replan_ctx.remaining_limit is not None and replan_ctx.remaining_limit <= 0:
        yield sse_json({"delta": f"{STREAM_TAG_FAIL} 计划已耗尽且剩余步数不足以继续（max_steps={max_steps_limit}）。\n"})
        return RUN_STATUS_FAILED, None, plan_titles, plan_items, plan_allows, plan_artifacts

    if not replan_ctx.can_replan:
        yield sse_json({"delta": f"{STREAM_TAG_FAIL} 计划已耗尽且重新规划次数已达上限。\n"})
        return RUN_STATUS_FAILED, None, plan_titles, plan_items, plan_allows, plan_artifacts

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
        plan_titles=plan_titles,
        plan_items=plan_items,
        plan_allows=plan_allows,
        plan_artifacts=plan_artifacts,
        agent_state=agent_state,
        observations=observations,
        done_count=replan_ctx.done_count,
        error=str(step_error),
        sse_notice=f"{STREAM_TAG_EXEC} 计划已耗尽，重新规划剩余步骤…",
        replan_attempts=replan_ctx.replan_attempts,
        safe_write_debug=safe_write_debug,
    )

    if replan_result:
        return "", replan_ctx.done_count, replan_result.plan_titles, replan_result.plan_items, replan_result.plan_allows, replan_result.plan_artifacts

    return RUN_STATUS_FAILED, None, plan_titles, plan_items, plan_allows, plan_artifacts
