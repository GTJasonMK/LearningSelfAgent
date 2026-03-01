from __future__ import annotations

from typing import Optional, Tuple

from backend.src.actions.registry import normalize_action_type
from backend.src.common.task_error_codes import format_task_error
from backend.src.constants import (
    ACTION_TYPE_HTTP_REQUEST,
    ACTION_TYPE_SHELL_COMMAND,
    ACTION_TYPE_TASK_OUTPUT,
    ACTION_TYPE_TOOL_CALL,
    ACTION_TYPE_USER_PROMPT,
)


_REQUIRES_RESULT_ACTIONS = {
    "llm_call",
    "memory_write",
    "task_output",
    "tool_call",
    "shell_command",
    "http_request",
    "file_list",
    "file_read",
    "file_append",
    "file_write",
    "file_delete",
    "json_parse",
}
ACTION_RESULT_CONTRACT_VERSION = 1


def _merge_warning(result_obj: dict, warning: str) -> dict:
    warnings = result_obj.get("warnings")
    items = list(warnings) if isinstance(warnings, list) else []
    if warning not in items:
        items.append(str(warning))
    result_obj["warnings"] = items
    return result_obj


def _attach_result_contract(
    *,
    result_obj: Optional[dict],
    action_type: str,
    required_result: bool,
) -> Optional[dict]:
    if result_obj is None:
        return None
    warnings = result_obj.get("warnings")
    has_warning = isinstance(warnings, list) and any(str(item or "").strip() for item in warnings)
    result_obj["result_contract"] = {
        "version": int(ACTION_RESULT_CONTRACT_VERSION),
        "action_type": str(action_type or "").strip() or "unknown",
        "required_result": bool(required_result),
        "status": "warn" if has_warning else "ok",
    }
    return result_obj


def verify_and_normalize_action_result(
    *,
    action_type: object,
    payload: object,
    result: object,
    error: Optional[str],
    context: Optional[dict],
) -> Tuple[Optional[dict], Optional[str]]:
    """
    执行后验证器：
    - 保证 action 执行结果结构稳定；
    - 在不打断主链路前提下补充契约告警；
    - 对明显无效结果给出结构化错误码。
    """
    if isinstance(error, str) and error.strip():
        return _attach_result_contract(
            result_obj=(result if isinstance(result, dict) else None),
            action_type=str(action_type or ""),
            required_result=bool(
                (normalize_action_type(str(action_type or "")) or str(action_type or "").strip().lower())
                in _REQUIRES_RESULT_ACTIONS
            ),
        ), error

    normalized_action_type = normalize_action_type(str(action_type or "")) or str(action_type or "").strip().lower()
    payload_obj = dict(payload) if isinstance(payload, dict) else {}
    result_obj: Optional[dict]

    if isinstance(result, dict):
        result_obj = dict(result)
    elif result is None:
        result_obj = None
    else:
        result_obj = {"value": result}

    required_result = normalized_action_type in _REQUIRES_RESULT_ACTIONS
    if required_result and result_obj is None:
        return None, format_task_error(
            code="empty_action_result",
            message=f"{normalized_action_type or 'action'} 执行后结果为空",
        )

    if not isinstance(result_obj, dict):
        return _attach_result_contract(
            result_obj=result_obj,
            action_type=normalized_action_type,
            required_result=required_result,
        ), None

    # task_output：尽量补齐 content，减少“结束但无可展示结果”。
    if normalized_action_type == ACTION_TYPE_TASK_OUTPUT:
        content = str(result_obj.get("content") or "").strip()
        if not content:
            payload_content = str(payload_obj.get("content") or "").strip()
            last_llm_response = str((context or {}).get("last_llm_response") or "").strip()
            fallback_content = payload_content or last_llm_response
            if fallback_content:
                result_obj["content"] = fallback_content
                result_obj = _merge_warning(result_obj, "task_output.content auto-filled by verifier")
            else:
                result_obj = _merge_warning(result_obj, "task_output.content is empty")

    # http_request：缺少关键字段时打告警（不硬失败，避免 mock/兼容链路受影响）。
    if normalized_action_type == ACTION_TYPE_HTTP_REQUEST:
        if result_obj.get("status_code") is None:
            result_obj = _merge_warning(result_obj, "http_request.status_code missing")
        if not str(result_obj.get("content") or "").strip():
            result_obj = _merge_warning(result_obj, "http_request.content is empty")

    # tool_call：输出为空会影响后续 json_parse，提前标记。
    if normalized_action_type == ACTION_TYPE_TOOL_CALL:
        if not str(result_obj.get("output") or "").strip():
            result_obj = _merge_warning(result_obj, "tool_call.output is empty")

    # shell_command(script_run)：启用结构化输出契约，避免“脚本执行成功但中间产物缺失”。
    if normalized_action_type == ACTION_TYPE_SHELL_COMMAND:
        if bool(payload_obj.get("parse_json_output")) and ("parsed_output" not in result_obj):
            return None, format_task_error(
                code="missing_script_parsed_output",
                message="script_run.parse_json_output=true 但结果缺少 parsed_output",
            )
        expected_outputs = payload_obj.get("expected_outputs")
        if isinstance(expected_outputs, list) and expected_outputs:
            artifacts = result_obj.get("artifacts")
            if not isinstance(artifacts, list):
                result_obj = _merge_warning(result_obj, "script_run.expected_outputs provided but artifacts missing")

    # user_prompt：至少要有 question 字段（用于暂停渲染）。
    if normalized_action_type == ACTION_TYPE_USER_PROMPT:
        question = str(result_obj.get("question") or payload_obj.get("question") or "").strip()
        if question:
            result_obj.setdefault("question", question)
        else:
            return None, format_task_error(code="missing_user_prompt_question", message="user_prompt 缺少 question")

    return _attach_result_contract(
        result_obj=result_obj,
        action_type=normalized_action_type,
        required_result=required_result,
    ), None
