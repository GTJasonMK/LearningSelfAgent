from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from backend.src.common.utils import error_response, parse_optional_int
from backend.src.constants import (
    AGENT_DEFAULT_MAX_STEPS,
    ERROR_CODE_INVALID_REQUEST,
    ERROR_MESSAGE_LLM_CHAT_MESSAGE_MISSING,
    HTTP_STATUS_BAD_REQUEST,
)
from backend.src.services.llm.llm_client import resolve_default_model


@dataclass(frozen=True)
class ParsedStreamCommandRequest:
    message: str
    requested_max_steps: Any
    normalized_max_steps: int
    dry_run: bool
    model: str
    parameters: dict


def parse_stream_command_request(payload: Any):
    """
    统一解析 do/think 流式入口的通用请求字段。

    返回：
    - ParsedStreamCommandRequest：解析成功
    - FastAPI Response：请求非法（如 message 为空）
    """
    message = str(getattr(payload, "message", "") or "").strip()
    if not message:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            ERROR_MESSAGE_LLM_CHAT_MESSAGE_MISSING,
            HTTP_STATUS_BAD_REQUEST,
        )

    requested_max_steps = getattr(payload, "max_steps", None) or AGENT_DEFAULT_MAX_STEPS
    normalized_max_steps = parse_optional_int(requested_max_steps, default=None)
    if normalized_max_steps is None:
        normalized_max_steps = int(AGENT_DEFAULT_MAX_STEPS)

    return ParsedStreamCommandRequest(
        message=message,
        requested_max_steps=requested_max_steps,
        normalized_max_steps=int(normalized_max_steps),
        dry_run=bool(getattr(payload, "dry_run", False)),
        model=(str(getattr(payload, "model", "") or "").strip() or resolve_default_model()),
        parameters=getattr(payload, "parameters", None) or {"temperature": 0.2},
    )
