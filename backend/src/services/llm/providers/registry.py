from __future__ import annotations

import os
from typing import Optional

from backend.src.common.errors import AppError
from backend.src.constants import (
    ERROR_CODE_INVALID_REQUEST,
    ERROR_MESSAGE_LLM_PROVIDER_UNAVAILABLE,
    HTTP_STATUS_BAD_REQUEST,
    LLM_PROVIDER_OPENAI,
)
from backend.src.services.llm.providers.base import LLMProvider


def normalize_provider_name(name: Optional[str]) -> str:
    raw = str(name or "").strip().lower()
    if not raw:
        raw = str(os.getenv("LLM_PROVIDER") or "").strip().lower()
    return raw or LLM_PROVIDER_OPENAI


def create_provider(
    *,
    provider: Optional[str],
    api_key: str,
    base_url: Optional[str],
    default_model: str,
) -> LLMProvider:
    """
    通过 provider 名称创建 Provider 实例。

    说明：
    - 这里做“懒导入”：避免在没有安装某个 SDK 时整个服务无法启动；
    - 未支持的 provider 统一抛 AppError，交由上层转换为可读错误。
    """
    name = normalize_provider_name(provider)
    if name == LLM_PROVIDER_OPENAI:
        from backend.src.services.llm.providers.openai_provider import OpenAIProvider

        return OpenAIProvider(api_key=api_key, base_url=base_url, default_model=default_model)

    raise AppError(
        code=ERROR_CODE_INVALID_REQUEST,
        message=f"{ERROR_MESSAGE_LLM_PROVIDER_UNAVAILABLE}:{name}",
        status_code=HTTP_STATUS_BAD_REQUEST,
    )

