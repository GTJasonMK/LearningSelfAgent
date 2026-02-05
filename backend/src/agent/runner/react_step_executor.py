# -*- coding: utf-8 -*-
"""
ReAct 循环步骤执行模块。

提供动作生成、步骤执行、观测生成等核心逻辑。
"""

import json
import logging
from typing import Callable, Dict, Generator, List, Optional, Tuple

from backend.src.agent.support import _truncate_observation
from backend.src.agent.runner.react_helpers import (
    call_llm_for_text,
    needs_nonempty_task_output_content,
    validate_and_normalize_action_text,
)
from backend.src.agent.runner.plan_events import sse_plan_delta
from backend.src.common.utils import now_iso
from backend.src.constants import (
    ACTION_TYPE_FILE_WRITE,
    ACTION_TYPE_FILE_READ,
    ACTION_TYPE_FILE_APPEND,
    ACTION_TYPE_FILE_LIST,
    ACTION_TYPE_FILE_DELETE,
    ACTION_TYPE_HTTP_REQUEST,
    ACTION_TYPE_LLM_CALL,
    ACTION_TYPE_JSON_PARSE,
    ACTION_TYPE_MEMORY_WRITE,
    ACTION_TYPE_SHELL_COMMAND,
    ACTION_TYPE_TASK_OUTPUT,
    ACTION_TYPE_TOOL_CALL,
    ACTION_TYPE_USER_PROMPT,
    AGENT_REACT_ACTION_RETRY_MAX_ATTEMPTS,
    RUN_STATUS_FAILED,
    RUN_STATUS_WAITING,
    STATUS_WAITING,
    STEP_STATUS_RUNNING,
    STEP_STATUS_WAITING,
    STREAM_TAG_ASK,
    STREAM_TAG_FAIL,
    SSE_TYPE_MEMORY_ITEM,
    TASK_OUTPUT_TYPE_USER_PROMPT,
)
from backend.src.services.llm.llm_client import sse_json
from backend.src.services.output.output_format import format_visible_result
from backend.src.repositories.task_outputs_repo import create_task_output
from backend.src.repositories.task_steps_repo import (
    TaskStepCreateParams,
    create_task_step,
    mark_task_step_failed,
)
from backend.src.repositories.task_runs_repo import update_task_run
from backend.src.repositories.tasks_repo import update_task

logger = logging.getLogger(__name__)


def generate_action_with_retry(
    *,
    llm_call: Callable[[dict], dict],
    react_prompt: str,
    task_id: int,
    run_id: int,
    step_order: int,
    step_title: str,
    workdir: str,
    model: str,
    react_params: dict,
    variables_source: str,
) -> Tuple[Optional[dict], Optional[str], Optional[dict], Optional[str], Optional[str]]:
    """
    生成 Action（支持自动重试）。

    Args:
        llm_call: LLM 调用函数
        react_prompt: ReAct 提示词
        task_id: 任务 ID
        run_id: 执行尝试 ID
        step_order: 步骤序号
        step_title: 步骤标题
        workdir: 工作目录
        model: 模型名称
        react_params: LLM 参数
        variables_source: 变量来源标识

    Returns:
        (action_obj, action_type, payload_obj, validate_error, last_action_text)
    """
    action_obj = None
    action_type = None
    payload_obj = None
    action_validate_error = None
    last_action_text = None
    prompt_for_attempt = react_prompt

    retries = int(AGENT_REACT_ACTION_RETRY_MAX_ATTEMPTS or 0)
    for attempt in range(0, 1 + max(0, retries)):
        attempt_params = dict(react_params)
        if attempt > 0:
            # 重试时强制更稳定：降低温度，减少格式漂移
            attempt_params["temperature"] = 0

        action_text, action_error = call_llm_for_text(
            llm_call,
            prompt=prompt_for_attempt,
            task_id=int(task_id),
            run_id=int(run_id),
            model=model,
            parameters=attempt_params,
            variables={
                "source": variables_source if attempt == 0 else f"{variables_source}_retry{attempt}",
                "step_order": int(step_order),
                "attempt": int(attempt),
            },
        )
        last_action_text = action_text

        if action_error or not action_text:
            action_validate_error = action_error or "empty_response"
        else:
            action_obj, action_type, payload_obj, action_validate_error = validate_and_normalize_action_text(
                action_text=action_text,
                step_title=step_title,
                workdir=workdir,
            )

        if not action_validate_error and action_obj:
            break

        if attempt < retries:
            prompt_for_attempt = (
                react_prompt
                + f"\n上一次输出不合法（{action_validate_error}）。请严格只输出 JSON（不要代码块、不要解释）。\n"
            )

    return action_obj, action_type, payload_obj, action_validate_error, last_action_text


def build_observation_line(
    *,
    action_type: str,
    title: str,
    result: Optional[dict],
    context: Dict,
) -> Tuple[str, Optional[str]]:
    """
    根据执行结果构建观测行。

    Args:
        action_type: 动作类型
        title: 步骤标题
        result: 执行结果
        context: 上下文字典（会被修改以存储 last_llm_response）

    Returns:
        (observation_line, visible_content) 观测行和可见内容（用于 task_output）
    """
    visible_content = None

    if action_type == ACTION_TYPE_SHELL_COMMAND and isinstance(result, dict):
        stdout = _truncate_observation(str(result.get("stdout") or ""))
        stderr = _truncate_observation(str(result.get("stderr") or ""))
        obs_line = f"{title}: shell stdout={stdout} stderr={stderr}"

    elif action_type == ACTION_TYPE_LLM_CALL and isinstance(result, dict):
        resp = str(result.get("response") or "")
        context["last_llm_response"] = resp
        obs_line = f"{title}: llm={_truncate_observation(resp)}"

    elif action_type == ACTION_TYPE_TASK_OUTPUT and isinstance(result, dict):
        content_text = str(result.get("content") or "").strip()
        obs_line = f"{title}: output={_truncate_observation(content_text)}"
        if content_text:
            visible_content = content_text

    elif action_type == ACTION_TYPE_FILE_WRITE and isinstance(result, dict):
        path = str(result.get("path") or "").strip()
        size = result.get("bytes")
        tail = f"{size} bytes" if isinstance(size, int) else ""
        obs_line = f"{title}: file_write {path} {tail}".strip()

    elif action_type == ACTION_TYPE_FILE_READ and isinstance(result, dict):
        path = str(result.get("path") or "").strip()
        size = result.get("bytes")
        tail = f"{size} bytes" if isinstance(size, int) else ""
        content = _truncate_observation(str(result.get("content") or ""))
        obs_line = f"{title}: file_read {path} {tail} content={content}".strip()

    elif action_type == ACTION_TYPE_FILE_APPEND and isinstance(result, dict):
        path = str(result.get("path") or "").strip()
        size = result.get("bytes")
        tail = f"{size} bytes" if isinstance(size, int) else ""
        obs_line = f"{title}: file_append {path} {tail}".strip()

    elif action_type == ACTION_TYPE_FILE_LIST and isinstance(result, dict):
        count = result.get("count")
        obs_line = f"{title}: file_list count={count}".strip()

    elif action_type == ACTION_TYPE_FILE_DELETE and isinstance(result, dict):
        deleted = result.get("deleted")
        obs_line = f"{title}: file_delete deleted={deleted}".strip()

    elif action_type == ACTION_TYPE_TOOL_CALL and isinstance(result, dict):
        tool_name = str(result.get("tool_id") or "")
        out = str(result.get("output") or "")
        obs_line = f"{title}: tool#{tool_name} output={_truncate_observation(out)}"

    elif action_type == ACTION_TYPE_HTTP_REQUEST and isinstance(result, dict):
        status_code = result.get("status_code")
        size = result.get("bytes")
        tail = f"{size} bytes" if isinstance(size, int) else ""
        obs_line = f"{title}: http {status_code} {tail}".strip()

    elif action_type == ACTION_TYPE_JSON_PARSE and isinstance(result, dict):
        picked = result.get("picked")
        obs_line = f"{title}: json_parse picked={picked}".strip()

    elif action_type == ACTION_TYPE_MEMORY_WRITE and isinstance(result, dict):
        obs_line = f"{title}: memory#{result.get('id')}"

    else:
        obs_line = f"{title}: ok"

    return obs_line, visible_content


def record_invalid_action_step(
    *,
    task_id: int,
    run_id: int,
    step_order: int,
    title: str,
    executor: Optional[str] = None,
    error: str,
    last_action_text: Optional[str],
    safe_write_debug: Callable,
) -> Optional[int]:
    """
    为无效 action 创建失败步骤记录。

    Args:
        task_id: 任务 ID
        run_id: 执行尝试 ID
        step_order: 步骤序号
        title: 步骤标题
        error: 错误信息
        last_action_text: 最后的 action 文本
        safe_write_debug: 调试输出函数

    Returns:
        创建的 step_id，失败返回 None
    """
    step_created_at = now_iso()
    detail = json.dumps(
        {
            "type": "react_action_invalid",
            "payload": {
                "error": str(error or "invalid_action"),
                "last_action_text": _truncate_observation(str(last_action_text or "")),
            },
        },
        ensure_ascii=False,
    )

    try:
        step_id, _created, _updated = create_task_step(
            TaskStepCreateParams(
                task_id=int(task_id),
                run_id=int(run_id),
                title=title,
                status=STEP_STATUS_RUNNING,
                executor=str(executor) if executor is not None else None,
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
        mark_task_step_failed(
            step_id=int(step_id),
            error=f"action_invalid:{error or 'invalid_action'}",
            finished_at=now_iso(),
        )
        return step_id

    except Exception as exc:
        safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.react.action_invalid.persist_failed",
            data={"step_order": int(step_order), "error": str(exc)},
            level="warning",
        )
        return None


def handle_user_prompt_action(
    *,
    task_id: int,
    run_id: int,
    step_order: int,
    title: str,
    payload_obj: dict,
    plan_items: List[dict],
    plan_titles: List[str],
    plan_allows: List[List[str]],
    plan_artifacts: List[str],
    agent_state: Dict,
    safe_write_debug: Callable,
    db_lock: Optional[object] = None,
) -> Generator[str, None, Tuple[str, bool]]:
    """
    处理 user_prompt 动作（暂停等待用户输入）。

    Args:
        task_id: 任务 ID
        run_id: 执行尝试 ID
        step_order: 步骤序号
        title: 步骤标题
        payload_obj: 动作载荷
        plan_items: 计划项列表
        plan_titles: 计划标题列表
        plan_allows: 允许的动作类型列表
        plan_artifacts: 产物列表
        agent_state: Agent 状态字典
        safe_write_debug: 调试输出函数

    Yields:
        SSE 事件

    Returns:
        (run_status, should_break) 运行状态和是否应该终止循环
    """
    from backend.src.agent.runner.react_plan_state import build_agent_plan_payload

    question = str(payload_obj.get("question") or "").strip()
    if not question:
        if 0 <= step_order - 1 < len(plan_items):
            plan_items[step_order - 1]["status"] = "failed"
            yield sse_plan_delta(task_id=task_id, plan_items=plan_items, indices=[step_order - 1])
        yield sse_json({"delta": f"{STREAM_TAG_FAIL} user_prompt.question 不能为空\n"})
        return RUN_STATUS_FAILED, True

    # 更新计划栏：当前步骤标记为 waiting
    if 0 <= step_order - 1 < len(plan_items):
        plan_items[step_order - 1]["status"] = "waiting"
        yield sse_plan_delta(task_id=task_id, plan_items=plan_items, indices=[step_order - 1])

    # docs/agent：waiting 也应落库到 task_steps，便于中断恢复与审计。
    step_created_at = now_iso()
    try:
        detail = json.dumps({"type": ACTION_TYPE_USER_PROMPT, "payload": payload_obj}, ensure_ascii=False)

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

        if db_lock is not None:
            with db_lock:
                step_id, _created, _updated = create_task_step(
                    TaskStepCreateParams(
                        task_id=int(task_id),
                        run_id=int(run_id),
                        title=title,
                        status=STEP_STATUS_WAITING,
                        executor=executor_value,
                        detail=detail,
                        result=None,
                        error=None,
                        attempts=1,
                        started_at=step_created_at,
                        finished_at=None,
                        step_order=int(step_order),
                        created_at=step_created_at,
                        updated_at=step_created_at,
                    )
                )
        else:
            step_id, _created, _updated = create_task_step(
                TaskStepCreateParams(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    title=title,
                    status=STEP_STATUS_WAITING,
                    executor=executor_value,
                    detail=detail,
                    result=None,
                    error=None,
                    attempts=1,
                    started_at=step_created_at,
                    finished_at=None,
                    step_order=int(step_order),
                    created_at=step_created_at,
                    updated_at=step_created_at,
                )
            )
    except Exception as exc:
        step_id = None
        safe_write_debug(
            task_id=int(task_id),
            run_id=int(run_id),
            message="agent.user_prompt.step_persist_failed",
            data={"step_order": int(step_order), "error": str(exc)},
            level="warning",
        )

    # 写入输出记录
    created_at = step_created_at
    try:
        if db_lock is not None:
            with db_lock:
                create_task_output(
                    task_id=int(task_id),
                    run_id=int(run_id),
                    output_type=TASK_OUTPUT_TYPE_USER_PROMPT,
                    content=question,
                    created_at=created_at,
                )
        else:
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
            message="agent.user_prompt.output_write_failed",
            data={"step_order": int(step_order), "error": str(exc)},
            level="warning",
        )

    safe_write_debug(
        task_id=int(task_id),
        run_id=int(run_id),
        message="agent.waiting_input",
        data={"step_order": int(step_order), "question": question},
        level="info",
    )

    # 持久化暂停状态
    agent_state["paused"] = {
        "question": question,
        "step_order": step_order,
        "step_title": title,
        "created_at": created_at,
    }
    if step_id is not None:
        agent_state["paused"]["step_id"] = int(step_id)
    agent_state["step_order"] = step_order
    updated_at = now_iso()

    try:
        if db_lock is not None:
            with db_lock:
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
        else:
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
            message="agent.waiting_state.persist_failed",
            data={"step_order": int(step_order), "error": str(exc)},
            level="error",
        )

    kind = str(payload_obj.get("kind") or "").strip()
    need_input_payload = {"type": "need_input", "task_id": task_id, "run_id": run_id, "question": question}
    if kind:
        need_input_payload["kind"] = kind

    yield sse_json(need_input_payload)
    yield sse_json({"delta": f"{STREAM_TAG_ASK} {question}\n"})

    return RUN_STATUS_WAITING, True


def handle_task_output_fallback(
    *,
    llm_call: Callable[[dict], dict],
    react_prompt: str,
    task_id: int,
    run_id: int,
    step_order: int,
    title: str,
    workdir: str,
    model: str,
    react_params: dict,
    variables_source: str,
    payload_obj: dict,
    context: Dict,
    safe_write_debug: Callable,
) -> Tuple[Optional[dict], Optional[str], Optional[dict], Optional[str]]:
    """
    处理 task_output.content 为空的情况，强制让模型补齐。

    Args:
        llm_call: LLM 调用函数
        react_prompt: ReAct 提示词
        task_id: 任务 ID
        run_id: 执行尝试 ID
        step_order: 步骤序号
        title: 步骤标题
        workdir: 工作目录
        model: 模型名称
        react_params: LLM 参数
        variables_source: 变量来源标识
        payload_obj: 原始载荷
        context: 上下文字典
        safe_write_debug: 调试输出函数

    Returns:
        (action_obj, action_type, payload_obj, error) 新的动作或错误信息
    """
    if not needs_nonempty_task_output_content(payload_obj, context):
        return None, None, None, None

    safe_write_debug(
        task_id=int(task_id),
        run_id=int(run_id),
        message="agent.task_output.content_missing",
        data={"step_order": int(step_order), "title": title},
        level="warning",
    )

    force_content_prompt = (
        react_prompt
        + "\n补充约束：你选择了 task_output，但当前没有可用的上一条 llm_call 输出用于补齐。"
        "因此你必须在 task_output.payload.content 中写入非空的最终结论（不要返回空字符串）。只输出 JSON。\n"
    )

    forced_text, forced_err = call_llm_for_text(
        llm_call,
        prompt=force_content_prompt,
        task_id=int(task_id),
        run_id=int(run_id),
        model=model,
        parameters=react_params,
        variables={
            "source": f"{variables_source}_force_task_output",
            "step_order": int(step_order),
        },
    )

    forced_obj, forced_type, forced_payload, forced_validate_error = validate_and_normalize_action_text(
        action_text=forced_text or "",
        step_title=title,
        workdir=workdir,
    )

    if forced_err or forced_validate_error or forced_type != ACTION_TYPE_TASK_OUTPUT:
        return None, None, None, "task_output.content 为空且无法自动补齐"

    if needs_nonempty_task_output_content(forced_payload or {}, context):
        return None, None, None, "task_output.content 仍为空"

    return forced_obj, forced_type, forced_payload or {}, None


def yield_memory_write_event(
    *,
    task_id: int,
    run_id: int,
    result: dict,
) -> str:
    """
    生成记忆写入的 SSE 事件。

    Args:
        task_id: 任务 ID
        run_id: 执行尝试 ID
        result: 记忆写入结果

    Returns:
        SSE JSON 字符串
    """
    return sse_json({
        "type": SSE_TYPE_MEMORY_ITEM,
        "task_id": int(task_id),
        "run_id": int(run_id),
        "item": result,
    })


def yield_visible_result(content: str) -> str:
    """
    生成可见结果的 SSE 事件。

    Args:
        content: 结果内容

    Returns:
        SSE JSON 字符串
    """
    return sse_json({"delta": f"{format_visible_result(content)}\n"})
