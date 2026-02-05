import json
from typing import Optional

from backend.src.api.schemas import AgentRouteRequest
from backend.src.common.utils import error_response, extract_json_object
from backend.src.constants import (
    AGENT_ROUTE_MAX_MESSAGE_CHARS,
    AGENT_ROUTE_PROMPT_TEMPLATE,
    ERROR_CODE_INVALID_REQUEST,
    ERROR_MESSAGE_LLM_CHAT_MESSAGE_MISSING,
    HTTP_STATUS_BAD_REQUEST,
)
from backend.src.services.llm.llm_client import call_openai, resolve_default_model


def route_agent_mode(payload: AgentRouteRequest):
    """
    自动模式选择：让 LLM 决定当前输入是否需要启用 plan/ReAct。
    返回：{"mode":"chat|do|think","confidence":0-1,"reason":"..."}
    """

    message = (payload.message or "").strip()
    if not message:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            ERROR_MESSAGE_LLM_CHAT_MESSAGE_MISSING,
            HTTP_STATUS_BAD_REQUEST,
        )

    if len(message) > AGENT_ROUTE_MAX_MESSAGE_CHARS:
        message = message[:AGENT_ROUTE_MAX_MESSAGE_CHARS]

    model = (payload.model or "").strip() or resolve_default_model()
    params = payload.parameters or {}
    # 路由要尽量稳定：温度默认 0；输出很短
    params.setdefault("temperature", 0)
    params.setdefault("max_tokens", 120)

    prompt = AGENT_ROUTE_PROMPT_TEMPLATE.format(message=message)
    text, _, err = call_openai(prompt, model, params)
    if err or not text:
        # 兜底：LLM 不可用时默认走 chat（用户仍可用 /do 强制）
        return {"mode": "chat", "confidence": 0.0, "reason": err or "empty_response"}

    obj = extract_json_object(text)
    if not isinstance(obj, dict):
        return {"mode": "chat", "confidence": 0.0, "reason": "invalid_json"}

    mode = str(obj.get("mode") or "").strip().lower()
    confidence = obj.get("confidence")
    reason = str(obj.get("reason") or "").strip()

    if mode not in {"chat", "do", "think"}:
        mode = "chat"

    confidence_value: float
    try:
        confidence_value = float(confidence)
    except Exception:
        confidence_value = 0.0
    if confidence_value < 0:
        confidence_value = 0.0
    if confidence_value > 1:
        confidence_value = 1.0

    return {"mode": mode, "confidence": confidence_value, "reason": reason}
