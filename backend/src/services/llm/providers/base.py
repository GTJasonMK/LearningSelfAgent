from __future__ import annotations

from typing import Any, AsyncGenerator, Dict, List, Optional, Protocol, Tuple


class LLMProvider(Protocol):
    """
    LLM Provider 接口（最小集）。

    设计目标：
    - 隔离供应商 SDK（OpenAI/Claude/Gemini/Ollama...），上层只依赖统一接口；
    - 让 call_llm/stream_chat 在不改业务代码的情况下替换供应商；
    - 便于单测：可注入 mock provider，避免真实网络调用。
    """

    name: str

    async def stream_chat(
        self,
        *,
        messages: List[Dict[str, str]],
        model: str,
        parameters: Optional[dict],
        timeout: int,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        流式输出：yield {"content": "...", "finish_reason": "...", ...}
        """

    def complete_prompt_sync(
        self,
        *,
        prompt: str,
        model: str,
        parameters: Optional[dict],
        timeout: int,
    ) -> Tuple[str, Optional[dict]]:
        """
        同步一次性调用：返回 (content, tokens)。
        """

    async def complete_prompt(
        self,
        *,
        prompt: str,
        model: str,
        parameters: Optional[dict],
        timeout: int,
    ) -> Tuple[str, Optional[dict]]:
        """
        异步一次性调用：返回 (content, tokens)。
        """

    async def aclose(self) -> None:
        """
        释放连接池等资源（可为空实现）。
        """

