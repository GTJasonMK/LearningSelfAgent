import json
import logging
import os
import time
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

from backend.src.common.errors import AppError
from backend.src.constants import (
    AGENT_LLM_MAX_CONCURRENCY_GLOBAL,
    AGENT_LLM_MAX_CONCURRENCY_PER_MODEL,
    DEFAULT_LLM_MODEL,
    ERROR_CODE_INVALID_REQUEST,
    ERROR_MESSAGE_LLM_API_KEY_MISSING,
    ERROR_MESSAGE_LLM_CALL_FAILED,
    HTTP_STATUS_BAD_REQUEST,
    LLM_PROVIDER_OPENAI,
)
from backend.src.constants import SINGLETON_ROW_ID
from backend.src.services.llm.providers.registry import create_provider, normalize_provider_name
from backend.src.storage import get_connection
from backend.src.repositories.config_repo import fetch_llm_store_config

logger = logging.getLogger(__name__)

_LLM_CONCURRENCY_LOCK = threading.Lock()
_LLM_CONCURRENCY_STATE: Dict[str, Any] = {
    "global_limit": None,
    "per_model_limit": None,
    "global_sem": None,
    "model_sems": {},
    # 自适应限流（动态并发）：用于在 429/抖动时自动降并发，随后慢慢恢复。
    "adaptive_global": None,
    "adaptive_models": {},
}


def _normalize_concurrency_limit(value: object) -> int:
    try:
        v = int(value)  # type: ignore[arg-type]
    except Exception:
        return 0
    return v if v > 0 else 0


class _AdaptiveLimiter:
    """
    自适应并发限制器（同步、线程安全）。

    说明：
    - base_limit：来自配置常量的“上限”
    - current_limit：动态调整后的“当前并发上限”（<= base_limit）
    - 429/限流/抖动时降低 current_limit；成功一段时间后缓慢恢复
    """

    def __init__(self, base_limit: int):
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self.base_limit = max(0, int(base_limit))
        self.current_limit = self.base_limit if self.base_limit > 0 else 0
        self.in_flight = 0
        self.cooldown_until = 0.0
        self.last_increase_at = 0.0

    def sync_base_limit(self, base_limit: int) -> None:
        base = max(0, int(base_limit))
        with self._cond:
            if base == self.base_limit:
                return
            self.base_limit = base
            if self.base_limit <= 0:
                self.current_limit = 0
            else:
                # base_limit 变化时，保守处理：current_limit 不超过新 base
                if self.current_limit <= 0:
                    self.current_limit = self.base_limit
                else:
                    self.current_limit = min(int(self.current_limit), int(self.base_limit))
            self._cond.notify_all()

    def acquire(self) -> None:
        if self.base_limit <= 0:
            return
        with self._cond:
            while True:
                limit = int(self.current_limit or self.base_limit)
                if self.in_flight < limit:
                    self.in_flight += 1
                    return
                self._cond.wait(timeout=0.1)

    def release(self) -> None:
        if self.base_limit <= 0:
            return
        with self._cond:
            self.in_flight = max(0, int(self.in_flight) - 1)
            self._cond.notify_all()

    def on_success(self) -> None:
        if self.base_limit <= 0:
            return
        now_value = time.monotonic()
        with self._cond:
            if now_value < float(self.cooldown_until):
                return
            if int(self.current_limit) >= int(self.base_limit):
                return
            # 缓慢恢复：至少间隔 5s 才提高 1
            if (now_value - float(self.last_increase_at)) < 5.0:
                return
            self.current_limit = min(int(self.base_limit), int(self.current_limit) + 1)
            self.last_increase_at = now_value
            self._cond.notify_all()

    def on_rate_limited(self) -> None:
        if self.base_limit <= 0:
            return
        now_value = time.monotonic()
        with self._cond:
            # 429：快速降并发（最小 1），并进入短暂冷却期
            current = int(self.current_limit or self.base_limit)
            self.current_limit = max(1, current - 1)
            self.cooldown_until = max(float(self.cooldown_until), now_value + 2.0)
            self._cond.notify_all()

    def on_transient_failure(self) -> None:
        if self.base_limit <= 0:
            return
        now_value = time.monotonic()
        with self._cond:
            # 抖动类错误：温和降并发（每次最多降 1），冷却期更短
            current = int(self.current_limit or self.base_limit)
            if current > 1:
                self.current_limit = max(1, current - 1)
            self.cooldown_until = max(float(self.cooldown_until), now_value + 1.0)
            self._cond.notify_all()


def _classify_llm_exception(exc: BaseException) -> str:
    """
    将异常粗分为：
    - rate_limit：429/限流
    - transient：超时/连接抖动/5xx
    - other：其他错误（不做并发降级）
    """
    text = str(exc or "").lower()
    if not text:
        return "other"
    if "429" in text or "rate limit" in text or "ratelimit" in text or "too many requests" in text:
        return "rate_limit"
    if any(k in text for k in ("timeout", "timed out", "connection", "reset", "502", "503", "gateway", "temporarily")):
        return "transient"
    return "other"


def _get_llm_concurrency_semaphores(
    *,
    provider_model_key: str,
) -> Tuple[
    Optional[threading.BoundedSemaphore],
    Optional[threading.BoundedSemaphore],
    Optional[_AdaptiveLimiter],
    Optional[_AdaptiveLimiter],
]:
    """
    获取 LLM 并发控制信号量（懒加载 + 支持测试 patch 常量）。

    说明：
    - global：限制所有 LLM 同步调用的最大并发
    - per-model：按 provider+model 维度限制并发
    """
    global_limit = _normalize_concurrency_limit(AGENT_LLM_MAX_CONCURRENCY_GLOBAL)
    per_model_limit = _normalize_concurrency_limit(AGENT_LLM_MAX_CONCURRENCY_PER_MODEL)

    with _LLM_CONCURRENCY_LOCK:
        if _LLM_CONCURRENCY_STATE.get("global_limit") != global_limit:
            _LLM_CONCURRENCY_STATE["global_limit"] = global_limit
            _LLM_CONCURRENCY_STATE["global_sem"] = (
                threading.BoundedSemaphore(global_limit) if global_limit > 0 else None
            )
            _LLM_CONCURRENCY_STATE["adaptive_global"] = _AdaptiveLimiter(global_limit) if global_limit > 0 else None

        if _LLM_CONCURRENCY_STATE.get("per_model_limit") != per_model_limit:
            _LLM_CONCURRENCY_STATE["per_model_limit"] = per_model_limit
            # 限额变更时清空缓存，避免旧 semaphore 值不一致
            _LLM_CONCURRENCY_STATE["model_sems"] = {}
            _LLM_CONCURRENCY_STATE["adaptive_models"] = {}

        model_sem: Optional[threading.BoundedSemaphore] = None
        model_adaptive: Optional[_AdaptiveLimiter] = None
        if per_model_limit > 0:
            sems = _LLM_CONCURRENCY_STATE.get("model_sems")
            if not isinstance(sems, dict):
                sems = {}
                _LLM_CONCURRENCY_STATE["model_sems"] = sems
            sem = sems.get(provider_model_key)
            if not isinstance(sem, threading.BoundedSemaphore):
                sem = threading.BoundedSemaphore(per_model_limit)
                sems[provider_model_key] = sem
            model_sem = sem

            adaptives = _LLM_CONCURRENCY_STATE.get("adaptive_models")
            if not isinstance(adaptives, dict):
                adaptives = {}
                _LLM_CONCURRENCY_STATE["adaptive_models"] = adaptives
            limiter = adaptives.get(provider_model_key)
            if not isinstance(limiter, _AdaptiveLimiter):
                limiter = _AdaptiveLimiter(per_model_limit)
                adaptives[provider_model_key] = limiter
            else:
                limiter.sync_base_limit(per_model_limit)
            model_adaptive = limiter

        global_sem = _LLM_CONCURRENCY_STATE.get("global_sem")
        if not isinstance(global_sem, threading.BoundedSemaphore):
            global_sem = None
        global_adaptive = _LLM_CONCURRENCY_STATE.get("adaptive_global")
        if isinstance(global_adaptive, _AdaptiveLimiter):
            global_adaptive.sync_base_limit(global_limit)
        else:
            global_adaptive = None

        return global_sem, model_sem, global_adaptive, model_adaptive


@contextmanager
def _llm_concurrency_guard(provider_model_key: str):
    """
    LLM 并发限制（同步调用）：
    - 先 acquire global，再 acquire per-model，保证拿锁顺序稳定，避免死锁。
    """
    global_sem, model_sem, global_adaptive, model_adaptive = _get_llm_concurrency_semaphores(
        provider_model_key=provider_model_key
    )
    acquired: List[threading.BoundedSemaphore] = []
    acquired_adaptive: List[_AdaptiveLimiter] = []
    try:
        if global_sem is not None:
            global_sem.acquire()
            acquired.append(global_sem)
        if model_sem is not None:
            model_sem.acquire()
            acquired.append(model_sem)
        if global_adaptive is not None:
            global_adaptive.acquire()
            acquired_adaptive.append(global_adaptive)
        if model_adaptive is not None:
            model_adaptive.acquire()
            acquired_adaptive.append(model_adaptive)
        yield
        # success：尝试恢复并发（慢速）
        for limiter in acquired_adaptive:
            try:
                limiter.on_success()
            except Exception:
                continue
    except Exception as exc:
        kind = _classify_llm_exception(exc)
        if kind == "rate_limit":
            for limiter in acquired_adaptive:
                try:
                    limiter.on_rate_limited()
                except Exception:
                    continue
        elif kind == "transient":
            for limiter in acquired_adaptive:
                try:
                    limiter.on_transient_failure()
                except Exception:
                    continue
        raise
    finally:
        for limiter in reversed(acquired_adaptive):
            try:
                limiter.release()
            except Exception:
                continue
        for sem in reversed(acquired):
            try:
                sem.release()
            except Exception:
                continue


class ContentCollectMode(Enum):
    """流式响应收集模式（仿照 llm_tool.py）"""

    CONTENT_ONLY = "content_only"
    WITH_REASONING = "with_reasoning"
    REASONING_ONLY = "reasoning_only"


@dataclass
class StreamCollectResult:
    """流式收集结果"""

    content: str
    reasoning: str
    finish_reason: Optional[str]
    chunk_count: int


class LLMClient:
    """
    LLM 调用统一封装（仿照 llm_tool.py）。

    目标：
    - 统一读取配置（key/base_url/model）
    - 统一流式输出（用于 UI 实时展示）
    - 提供 stream_and_collect 便捷方法（用于需要完整内容的后端逻辑）
    """

    def __init__(
        self,
        provider: Optional[str] = None,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        default_model: Optional[str] = None,
        strict_mode: bool = False,
    ):
        if strict_mode:
            if not api_key:
                raise AppError(
                    code=ERROR_CODE_INVALID_REQUEST,
                    message=ERROR_MESSAGE_LLM_API_KEY_MISSING,
                    status_code=HTTP_STATUS_BAD_REQUEST,
                )
            key = api_key
            url = base_url
            model = default_model
            provider_value = provider
        else:
            store = self._load_store_config()
            key = api_key or store.get("api_key") or os.getenv("OPENAI_API_KEY")
            if not key:
                raise AppError(
                    code=ERROR_CODE_INVALID_REQUEST,
                    message=ERROR_MESSAGE_LLM_API_KEY_MISSING,
                    status_code=HTTP_STATUS_BAD_REQUEST,
                )
            # 兼容历史：OPENAI_BASE_URL / OPENAI_API_BASE
            url = (
                base_url
                or store.get("base_url")
                or os.getenv("OPENAI_BASE_URL")
                or os.getenv("OPENAI_API_BASE")
            )
            model = default_model or store.get("model") or os.getenv("MODEL")
            provider_value = provider or store.get("provider")

        self._default_model = model or DEFAULT_LLM_MODEL
        self._provider_name = normalize_provider_name(provider_value)
        self._provider = create_provider(
            provider=self._provider_name,
            api_key=key,
            base_url=url,
            default_model=self._default_model,
        )

    async def aclose(self) -> None:
        # provider 自己决定是否需要关闭连接池等资源
        try:
            await self._provider.aclose()
        except Exception:
            return

    @staticmethod
    def _load_store_config() -> Dict[str, Optional[str]]:
        """
        从 SQLite 配置表读取 LLM 配置（若字段不存在/未初始化则回退为空）。
        """
        try:
            return fetch_llm_store_config()
        except Exception:
            return {"provider": None, "api_key": None, "base_url": None, "model": None}

    async def stream_chat(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        parameters: Optional[dict] = None,
        timeout: int = 120,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        流式输出：yield {"content": "...", "finish_reason": "...", ...}。

        说明：
        - 具体 SDK 行为由 Provider 实现（OpenAI/其他供应商）；
        - LLMClient 只负责统一参数形态与默认值。
        """
        actual_model = model or self._default_model
        params = parameters or {}
        logger.info("LLM stream start: provider=%s model=%s", self._provider_name, actual_model)
        async for chunk in self._provider.stream_chat(
            messages=messages,
            model=actual_model,
            parameters=params,
            timeout=timeout,
        ):
            yield chunk

    async def stream_and_collect(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        parameters: Optional[dict] = None,
        timeout: int = 120,
        collect_mode: ContentCollectMode = ContentCollectMode.CONTENT_ONLY,
    ) -> StreamCollectResult:
        content = ""
        reasoning = ""
        finish_reason = None
        chunk_count = 0

        async for chunk in self.stream_chat(
            messages=messages,
            model=model,
            parameters=parameters,
            timeout=timeout,
        ):
            chunk_count += 1

            if collect_mode in (ContentCollectMode.CONTENT_ONLY, ContentCollectMode.WITH_REASONING):
                if chunk.get("content"):
                    content += chunk["content"]

            if collect_mode in (ContentCollectMode.WITH_REASONING, ContentCollectMode.REASONING_ONLY):
                if chunk.get("reasoning_content"):
                    reasoning += chunk["reasoning_content"]

            if chunk.get("finish_reason"):
                finish_reason = chunk["finish_reason"]

        return StreamCollectResult(
            content=content,
            reasoning=reasoning,
            finish_reason=finish_reason,
            chunk_count=chunk_count,
        )

    def complete_prompt_sync(
        self,
        prompt: str,
        model: Optional[str] = None,
        parameters: Optional[dict] = None,
        timeout: int = 120,
    ) -> Tuple[str, Optional[dict]]:
        """
        同步一次性调用：用于 sync 路由/后台线程（规划、图谱抽取等）。
        返回：(content, tokens)
        """
        actual_model = model or self._default_model
        return self._provider.complete_prompt_sync(
            prompt=prompt,
            model=actual_model,
            parameters=parameters or {},
            timeout=timeout,
        )

    async def complete_prompt(
        self,
        prompt: str,
        model: Optional[str] = None,
        parameters: Optional[dict] = None,
        timeout: int = 120,
    ) -> Tuple[str, Optional[dict]]:
        """
        非流式一次性调用（为了拿到 usage tokens 等信息）。
        返回：(content, tokens)
        """
        actual_model = model or self._default_model
        return await self._provider.complete_prompt(
            prompt=prompt,
            model=actual_model,
            parameters=parameters or {},
            timeout=timeout,
        )


def sse_json(data: dict, event: Optional[str] = None) -> str:
    """
    统一 SSE 格式输出（data 为 JSON）。
    """
    prefix = f"event: {event}\n" if event else ""
    return prefix + f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


def resolve_default_model() -> str:
    """
    解析默认模型：优先 DB 配置，其次环境变量 MODEL，最后回退常量 DEFAULT_LLM_MODEL。
    """
    try:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT llm_model FROM config_store WHERE id = ?",
                (SINGLETON_ROW_ID,),
            ).fetchone()
        model = row["llm_model"] if row else None
    except Exception:
        model = None
    model = (model or "").strip() or None
    return model or os.getenv("MODEL") or DEFAULT_LLM_MODEL


def resolve_default_provider() -> str:
    """
    解析默认 Provider：优先 DB 配置，其次环境变量 LLM_PROVIDER，最后回退 openai。
    """
    try:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT llm_provider FROM config_store WHERE id = ?",
                (SINGLETON_ROW_ID,),
            ).fetchone()
        provider = row["llm_provider"] if row else None
    except Exception:
        provider = None
    provider = (provider or "").strip() or None
    return normalize_provider_name(provider or os.getenv("LLM_PROVIDER") or LLM_PROVIDER_OPENAI)


def call_llm(
    prompt: str,
    model: Optional[str],
    parameters: Optional[dict],
    *,
    provider: Optional[str] = None,
):
    """
    轻量一次性调用封装（同步），失败直接抛 AppError。

    说明：
    - 供规划/后处理/技能抽象等同步链路复用；
    - provider 可选：用于多供应商扩展（对应质量报告 P2#8）。
    """
    try:
        client = LLMClient(provider=provider)
    except AppError as exc:
        raise exc
    except Exception as exc:
        raise AppError(
            code=ERROR_CODE_INVALID_REQUEST,
            message=str(exc) or ERROR_MESSAGE_LLM_CALL_FAILED,
            status_code=HTTP_STATUS_BAD_REQUEST,
        )

    try:
        actual_model = str(model or client._default_model or "").strip() or DEFAULT_LLM_MODEL
        key = f"{str(client._provider_name or '').strip() or LLM_PROVIDER_OPENAI}:{actual_model}"
        with _llm_concurrency_guard(key):
            content, tokens = client.complete_prompt_sync(
                prompt=prompt, model=actual_model, parameters=parameters
            )
    except Exception as exc:
        raise AppError(
            code=ERROR_CODE_INVALID_REQUEST,
            message=str(exc) or ERROR_MESSAGE_LLM_CALL_FAILED,
            status_code=HTTP_STATUS_BAD_REQUEST,
        )

    if not str(content or "").strip():
        raise AppError(
            code=ERROR_CODE_INVALID_REQUEST,
            message=ERROR_MESSAGE_LLM_CALL_FAILED,
            status_code=HTTP_STATUS_BAD_REQUEST,
        )

    return content, tokens


def call_openai(prompt: str, model: Optional[str], parameters: Optional[dict]):
    # 兼容旧接口：绝大多数调用方仍使用 call_openai
    try:
        content, tokens = call_llm(prompt, model, parameters, provider="openai")
        return content, tokens, None
    except AppError as exc:
        return None, None, exc.message or ERROR_MESSAGE_LLM_CALL_FAILED
    except Exception as exc:
        return None, None, str(exc) or ERROR_MESSAGE_LLM_CALL_FAILED
