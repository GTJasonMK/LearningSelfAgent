import logging
import json
from typing import Optional

from backend.src.actions.registry import get_action_spec, normalize_action_type
from backend.src.common.errors import AppError
from backend.src.constants import (
    ACTION_TYPE_LLM_CALL,
    ERROR_MESSAGE_ACTION_UNSUPPORTED,
    ERROR_MESSAGE_PROMPT_RENDER_FAILED,
)
from backend.src.services.llm.prompt_templates import ensure_llm_call_template

logger = logging.getLogger(__name__)


def _execute_step_action(
    task_id: int, run_id: int, step_row, context: Optional[dict] = None
) -> tuple[Optional[dict], Optional[str]]:
    """
    执行单个步骤 action（Agent/ReAct 与 tasks.execute 共用）。

    返回：(result, error_message)：
    - result 为 dict（用于持久化到 task_steps.result）
    - error_message 非空表示本步失败
    """
    detail = step_row["detail"]
    if not detail:
        return None, ERROR_MESSAGE_PROMPT_RENDER_FAILED
    try:
        action = json.loads(detail)
    except json.JSONDecodeError:
        return None, ERROR_MESSAGE_PROMPT_RENDER_FAILED
    raw_type = action.get("type")
    action_type = normalize_action_type(raw_type) or raw_type
    payload = action.get("payload", {})
    if not isinstance(payload, dict):
        return None, ERROR_MESSAGE_PROMPT_RENDER_FAILED

    # 容错：LLM 规划时可能把 template_id 写成字符串（例如模板名称），导致 pydantic 校验失败。
    # 这里做一次归一化，尽量把“计划错误”转化为“可执行”的 llm_call。
    if action_type == ACTION_TYPE_LLM_CALL:
        step_title = (
            step_row["title"]
            if hasattr(step_row, "keys") and "title" in step_row.keys()
            else "未命名步骤"
        )
        ensure_llm_call_template(payload, step_title)

    spec = get_action_spec(str(action_type or "").strip())
    if not spec:
        return None, ERROR_MESSAGE_ACTION_UNSUPPORTED

    allowed_keys = spec.allowed_payload_keys or set()
    if allowed_keys:
        extra_keys = [key for key in payload.keys() if key not in allowed_keys]
        if extra_keys:
            # 丢弃未登记字段：避免“模型多输出字段/旧数据残留”导致整步失败。
            # 说明：detail 仍保留原始输入，便于审计；执行阶段仅使用过滤后的 payload。
            payload = {key: value for key, value in payload.items() if key in allowed_keys}
            logger.debug("drop_extra_payload_keys action_type=%s extra=%s", action_type, extra_keys)

    try:
        return spec.executor(int(task_id), int(run_id), step_row, payload, context)
    except AppError as exc:
        message = str(exc.message or "").strip()
        return None, message or ERROR_MESSAGE_PROMPT_RENDER_FAILED
    except Exception as exc:
        message = str(exc).strip()
        return None, message or ERROR_MESSAGE_PROMPT_RENDER_FAILED


__all__ = ["_execute_step_action"]
