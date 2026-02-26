import json
import re
from typing import Any, Optional, TYPE_CHECKING

from backend.src.common.utils import error_response, extract_json_object
from backend.src.constants import (
    AGENT_ROUTE_MAX_MESSAGE_CHARS,
    AGENT_ROUTE_PROMPT_TEMPLATE,
    ERROR_CODE_INVALID_REQUEST,
    ERROR_MESSAGE_LLM_CHAT_MESSAGE_MISSING,
    HTTP_STATUS_BAD_REQUEST,
)
from backend.src.services.llm.llm_client import call_openai, resolve_default_model

if TYPE_CHECKING:
    from backend.src.api.schemas import AgentRouteRequest
else:
    AgentRouteRequest = Any

_ROUTE_HINT_URL_RE = re.compile(r"(https?://|www\.)", re.IGNORECASE)
_ROUTE_DO_HINTS = (
    "抓取",
    "爬取",
    "拉取",
    "fetch",
    "crawl",
    "scrape",
    "查询",
    "搜索",
    "检索",
    "价格",
    "天气",
    "新闻",
    "汇率",
    "最新",
    "实时",
    "接口",
    "api",
    "网页",
    "网站",
    "调用",
)
_ROUTE_THINK_HINTS = (
    "架构",
    "设计",
    "方案",
    "权衡",
    "取舍",
    "比较",
    "对比",
    "规划",
    "拆解",
    "分析",
    "系统设计",
    "tradeoff",
    "roadmap",
)


def _contains_any_hint(text: str, hints: tuple[str, ...]) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    return any(str(h or "").lower() in lowered for h in hints if str(h or "").strip())


def _infer_route_mode_fallback(message: str) -> tuple[str, float, str]:
    text = str(message or "").strip()
    lowered = text.lower()

    if _ROUTE_HINT_URL_RE.search(text) or _contains_any_hint(lowered, _ROUTE_DO_HINTS):
        return "do", 0.62, "heuristic:url_or_external_task"
    if _contains_any_hint(lowered, _ROUTE_THINK_HINTS):
        return "think", 0.58, "heuristic:analysis_task"
    return "chat", 0.40, "heuristic:default_chat"


def _build_fallback_route_result(message: str, *, llm_reason: str) -> dict:
    mode, confidence, reason = _infer_route_mode_fallback(message)
    llm_reason_text = str(llm_reason or "").strip()
    if llm_reason_text:
        reason = f"{reason}; llm={llm_reason_text}"
    return {"mode": mode, "confidence": float(confidence), "reason": reason}


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
        # 兜底：LLM 路由失败时用轻量启发式，避免把外部抓取/执行任务误降级为 chat。
        return _build_fallback_route_result(message, llm_reason=err or "empty_response")

    obj = extract_json_object(text)
    if not isinstance(obj, dict):
        return _build_fallback_route_result(message, llm_reason="invalid_json")

    mode = str(obj.get("mode") or "").strip().lower()
    confidence = obj.get("confidence")
    reason = str(obj.get("reason") or "").strip()

    if mode not in {"chat", "do", "think"}:
        return _build_fallback_route_result(message, llm_reason=f"invalid_mode:{mode or 'empty'}")

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
