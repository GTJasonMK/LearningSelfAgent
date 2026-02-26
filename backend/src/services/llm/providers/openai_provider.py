from __future__ import annotations

import logging
import uuid
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

from backend.src.common.errors import AppError
from backend.src.constants import (
    DEFAULT_LLM_MODEL,
    ERROR_CODE_INVALID_REQUEST,
    ERROR_MESSAGE_LLM_CALL_FAILED,
    ERROR_MESSAGE_LLM_SDK_MISSING,
    HTTP_STATUS_BAD_REQUEST,
)
from backend.src.services.llm.providers.base import LLMProvider

logger = logging.getLogger(__name__)


def _sdk_missing_error(exc: Exception) -> AppError:
    return AppError(
        code=ERROR_CODE_INVALID_REQUEST,
        message=ERROR_MESSAGE_LLM_SDK_MISSING,
        status_code=HTTP_STATUS_BAD_REQUEST,
        details={"error": str(exc)},
    )


class OpenAIProvider(LLMProvider):
    """
    OpenAI Provider 实现。

    说明：
    - 仅承载 OpenAI SDK 依赖与调用细节；
    - 上层通过 providers.registry 选择 provider，从而解除 services 层对某个 SDK 的强耦合。
    """

    name = "openai"

    def __init__(self, *, api_key: str, base_url: Optional[str], default_model: str):
        self._api_key = api_key
        self._base_url = base_url
        self._default_model = default_model or DEFAULT_LLM_MODEL

        try:
            from openai import OpenAI
        except Exception as exc:
            raise _sdk_missing_error(exc) from exc

        # 同步 client：用于非流式（避免在 sync 路由里 asyncio.run 反复创建/销毁事件循环）
        self._sync_client = OpenAI(api_key=api_key, base_url=base_url) if base_url else OpenAI(api_key=api_key)
        # 异步 client：仅在需要流式输出时才创建
        self._async_client = None

    def _get_async_client(self):
        if self._async_client is not None:
            return self._async_client
        try:
            from openai import AsyncOpenAI
        except Exception as exc:
            raise _sdk_missing_error(exc) from exc
        self._async_client = (
            AsyncOpenAI(api_key=self._api_key, base_url=self._base_url)
            if self._base_url
            else AsyncOpenAI(api_key=self._api_key)
        )
        return self._async_client

    @staticmethod
    def _normalize_chat_completions_params(parameters: Optional[dict]) -> dict:
        """
        统一归一化 chat.completions 参数，避免不同 OpenAI 兼容实现间的字段差异导致调用失败。

        约定：
        - 优先使用 `max_tokens`；
        - 若仅提供 `max_output_tokens`，自动映射到 `max_tokens`；
        - 过滤值为 None 的键，减少 SDK 参数校验噪声。
        """
        raw = dict(parameters or {})

        if raw.get("max_tokens") is None and raw.get("max_output_tokens") is not None:
            max_output_tokens = raw.pop("max_output_tokens")
            try:
                max_tokens = int(max_output_tokens)
            except Exception:
                max_tokens = None
            if isinstance(max_tokens, int) and max_tokens > 0:
                raw["max_tokens"] = max_tokens
        else:
            raw.pop("max_output_tokens", None)

        normalized: dict = {}
        for key, value in raw.items():
            if value is None:
                continue
            normalized[str(key)] = value
        return normalized

    async def aclose(self) -> None:
        client = self._async_client
        self._async_client = None
        if client is None:
            return
        close_fn = getattr(client, "close", None)
        if callable(close_fn):
            result = close_fn()
            if hasattr(result, "__await__"):
                await result
            return
        aclose_fn = getattr(client, "aclose", None)
        if callable(aclose_fn):
            await aclose_fn()

    async def stream_chat(
        self,
        *,
        messages: List[Dict[str, str]],
        model: str,
        parameters: Optional[dict],
        timeout: int,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        request_id = str(uuid.uuid4())[:8]
        actual_model = model or self._default_model
        params = self._normalize_chat_completions_params(parameters)

        collected = ""
        chunk_count = 0

        logger.info("LLM stream start[%s]: model=%s base_url=%s", request_id, actual_model, self._base_url)
        try:
            payload = {
                "model": actual_model,
                "messages": messages,
                "stream": True,
                **params,
            }
            client = self._get_async_client()
            stream = await client.with_options(timeout=float(timeout)).chat.completions.create(**payload)
            async for chunk in stream:
                if not getattr(chunk, "choices", None):
                    continue
                choice = chunk.choices[0]
                delta = getattr(choice, "delta", None)
                content = getattr(delta, "content", None) if delta else None
                finish_reason = getattr(choice, "finish_reason", None)

                item: Dict[str, Any] = {"content": content, "finish_reason": finish_reason}

                # 兼容部分模型的 reasoning_content 字段
                if delta is not None and hasattr(delta, "reasoning_content"):
                    item["reasoning_content"] = getattr(delta, "reasoning_content", None)

                if content:
                    chunk_count += 1
                    collected += content

                yield item

            logger.info("LLM stream done[%s]: chunks=%d len=%d", request_id, chunk_count, len(collected))
        except Exception as exc:
            logger.error(
                "LLM stream failed[%s]: model=%s error=%s",
                request_id,
                actual_model,
                str(exc),
                exc_info=True,
            )
            raise

    def complete_prompt_sync(
        self,
        *,
        prompt: str,
        model: str,
        parameters: Optional[dict],
        timeout: int,
    ) -> Tuple[str, Optional[dict]]:
        actual_model = model or self._default_model
        params = self._normalize_chat_completions_params(parameters)
        try:
            resp = self._sync_client.with_options(timeout=float(timeout)).chat.completions.create(
                model=actual_model,
                messages=[{"role": "user", "content": prompt}],
                **params,
            )
        except Exception as exc:
            raise RuntimeError(f"{ERROR_MESSAGE_LLM_CALL_FAILED}:{exc}") from exc

        content = ""
        if getattr(resp, "choices", None):
            content = resp.choices[0].message.content or ""

        usage = getattr(resp, "usage", None)
        tokens = None
        if usage is not None:
            tokens = {
                "prompt": getattr(usage, "prompt_tokens", None),
                "completion": getattr(usage, "completion_tokens", None),
                "total": getattr(usage, "total_tokens", None),
            }
        return content, tokens

    async def complete_prompt(
        self,
        *,
        prompt: str,
        model: str,
        parameters: Optional[dict],
        timeout: int,
    ) -> Tuple[str, Optional[dict]]:
        actual_model = model or self._default_model
        params = self._normalize_chat_completions_params(parameters)
        try:
            client = self._get_async_client()
            resp = await client.with_options(timeout=float(timeout)).chat.completions.create(
                model=actual_model,
                messages=[{"role": "user", "content": prompt}],
                **params,
            )
        except Exception as exc:
            raise RuntimeError(f"{ERROR_MESSAGE_LLM_CALL_FAILED}:{exc}") from exc

        content = ""
        if getattr(resp, "choices", None):
            content = resp.choices[0].message.content or ""

        usage = getattr(resp, "usage", None)
        tokens = None
        if usage is not None:
            tokens = {
                "prompt": getattr(usage, "prompt_tokens", None),
                "completion": getattr(usage, "completion_tokens", None),
                "total": getattr(usage, "total_tokens", None),
            }
        return content, tokens
