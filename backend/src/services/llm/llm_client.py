import json
import logging
import os
import time
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

from backend.src.common.app_error_utils import invalid_request_error
from backend.src.common.errors import AppError
from backend.src.constants import (
    AGENT_LLM_MAX_CONCURRENCY_GLOBAL,
    AGENT_LLM_MAX_CONCURRENCY_PER_MODEL,
    DEFAULT_LLM_MODEL,
    ERROR_MESSAGE_LLM_API_KEY_MISSING,
    ERROR_MESSAGE_LLM_CALL_FAILED,
    LLM_PROVIDER_OPENAI,
    RIGHT_CODES_DEFAULT_BASE_URL,
)
from backend.src.constants import SINGLETON_ROW_ID
from backend.src.services.llm.providers.registry import (
    create_provider,
    is_right_codes_provider_name,
    normalize_provider_name,
)
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


def _read_int_env(name: str, default: int, *, min_value: int = 1) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return int(default)
    try:
        value = int(float(raw))
    except Exception:
        return int(default)
    if value < int(min_value):
        return int(min_value)
    return int(value)


# 同步 LLM 调用超时（秒）。
# 目的：避免单次供应商调用无限等待导致 run 一直处于 running 且无可观测失败信号。
LLM_CALL_TIMEOUT_SECONDS = _read_int_env("LLM_CALL_TIMEOUT_SECONDS", 45, min_value=5)


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
    return classify_llm_error_text(text)


def classify_llm_error_text(text: str) -> str:
    """
    统一的 LLM 错误文本分类（供并发控制和重试策略共用）。

    返回值：
    - "rate_limit"：429/限流，应等待后重试
    - "transient"：超时/连接抖动/5xx，可立即重试
    - "other"：永久性错误（配置、认证等），不应重试
    """
    lowered = str(text or "").strip().lower()
    if not lowered:
        return "other"
    if "429" in lowered or "rate limit" in lowered or "ratelimit" in lowered or "too many requests" in lowered:
        return "rate_limit"
    transient_markers = (
        "timeout", "timed out",
        "connection", "reset",
        "502", "503", "504",
        "gateway",
        "temporarily", "service unavailable",
    )
    if any(k in lowered for k in transient_markers):
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
    LLM 并发限制（同步调用）。

    设计：
    - 严格按 global → per-model 的顺序 acquire/release（避免死锁）；
    - 只使用 _AdaptiveLimiter（兼具并发限制与动态降级），去掉冗余的 BoundedSemaphore
      （旧实现同时 acquire sem + adaptive 共 4 层，在高并发下存在交叉持有风险）；
    - release 顺序与 acquire 相反，且放在 finally 中保证异常安全。
    """
    _, _, global_adaptive, model_adaptive = _get_llm_concurrency_semaphores(
        provider_model_key=provider_model_key
    )
    global_acquired = False
    model_acquired = False
    try:
        # 严格顺序：先 global，再 per-model
        if global_adaptive is not None:
            global_adaptive.acquire()
            global_acquired = True
        if model_adaptive is not None:
            model_adaptive.acquire()
            model_acquired = True
        yield
        # 成功：尝试恢复并发（慢速）
        if global_adaptive is not None:
            try:
                global_adaptive.on_success()
            except Exception:
                pass
        if model_adaptive is not None:
            try:
                model_adaptive.on_success()
            except Exception:
                pass
    except Exception as exc:
        kind = _classify_llm_exception(exc)
        for limiter in (global_adaptive, model_adaptive):
            if limiter is None:
                continue
            try:
                if kind == "rate_limit":
                    limiter.on_rate_limited()
                elif kind == "transient":
                    limiter.on_transient_failure()
            except Exception:
                continue
        raise
    finally:
        # 反序 release：先 per-model，再 global
        if model_acquired and model_adaptive is not None:
            try:
                model_adaptive.release()
            except Exception:
                pass
        if global_acquired and global_adaptive is not None:
            try:
                global_adaptive.release()
            except Exception:
                pass


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
            key = api_key
            url = base_url
            model = default_model
            provider_value = provider
        else:
            store = self._load_store_config()
            provider_value = provider or store.get("provider") or os.getenv("LLM_PROVIDER")
            # API Key 兜底顺序：
            # 1) 显式参数
            # 2) DB 配置
            # 3) OpenAI 兼容环境变量
            # 4) right.codes 专用环境变量（同样走 OpenAI 兼容协议）
            key = (
                api_key
                or store.get("api_key")
                or os.getenv("OPENAI_API_KEY")
                or os.getenv("RIGHT_CODES_API_KEY")
            )
            # 兼容历史：OPENAI_BASE_URL / OPENAI_API_BASE
            url = (
                base_url
                or store.get("base_url")
                or os.getenv("OPENAI_BASE_URL")
                or os.getenv("OPENAI_API_BASE")
                or os.getenv("RIGHT_CODES_BASE_URL")
            )
            # right.codes 文档默认基址（OpenAI 兼容）：
            # - 若 provider 明确为 right.codes 别名且未配置 base_url，则自动补齐。
            if not url and is_right_codes_provider_name(provider_value):
                url = RIGHT_CODES_DEFAULT_BASE_URL
            model = default_model or store.get("model") or os.getenv("MODEL")

        if not key:
            raise invalid_request_error(ERROR_MESSAGE_LLM_API_KEY_MISSING)

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


def resolve_default_provider_raw() -> str:
    """
    解析默认 Provider（原始值，不做别名归一化）。

    用途：
    - 让上层调用方在需要时保留 provider 别名语义（例如 right.codes），
      便于后续基于别名推导专属默认 base_url / fallback 规则。
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
    text = str(provider or "").strip() or str(os.getenv("LLM_PROVIDER") or "").strip()
    return text or LLM_PROVIDER_OPENAI


def resolve_llm_runtime_config(
    *,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    base_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    解析当前运行时 LLM 配置快照（用于 run 级固化，不发起网络请求）。
    """
    try:
        store = fetch_llm_store_config()
    except Exception:
        store = {"provider": None, "base_url": None, "model": None}

    provider_value = normalize_provider_name(
        provider
        or store.get("provider")
        or os.getenv("LLM_PROVIDER")
        or LLM_PROVIDER_OPENAI
    )

    url = (
        base_url
        or store.get("base_url")
        or os.getenv("OPENAI_BASE_URL")
        or os.getenv("OPENAI_API_BASE")
        or os.getenv("RIGHT_CODES_BASE_URL")
    )
    if not url and is_right_codes_provider_name(provider_value):
        url = RIGHT_CODES_DEFAULT_BASE_URL

    model_value = str(
        model
        or store.get("model")
        or os.getenv("MODEL")
        or DEFAULT_LLM_MODEL
    ).strip() or DEFAULT_LLM_MODEL

    fallback_urls = _resolve_base_url_fallbacks(provider_value)
    base_url_value = None
    if url is not None:
        base_url_value = str(url).strip() or None
    return {
        "provider": str(provider_value or LLM_PROVIDER_OPENAI),
        "model": str(model_value),
        "base_url": base_url_value,
        "fallback_base_urls": list(fallback_urls or []),
    }


def _parse_base_url_candidates(raw_value: Optional[str]) -> List[str]:
    text = str(raw_value or "").strip()
    if not text:
        return []
    out: List[str] = []
    seen = set()
    for item in text.split(","):
        url = str(item or "").strip()
        if not url:
            continue
        if url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def _resolve_base_url_fallbacks(provider_name: Optional[str]) -> List[str]:
    normalized = normalize_provider_name(provider_name)
    candidates: List[str] = []
    seen = set()

    env_keys: List[str] = ["LLM_BASE_URL_FALLBACKS", "OPENAI_BASE_URL_FALLBACKS"]
    if is_right_codes_provider_name(provider_name):
        env_keys.insert(0, "RIGHT_CODES_BASE_URL_FALLBACKS")
    elif normalized == LLM_PROVIDER_OPENAI:
        env_keys.append("RIGHT_CODES_BASE_URL_FALLBACKS")

    for key in env_keys:
        for url in _parse_base_url_candidates(os.getenv(key)):
            if url in seen:
                continue
            seen.add(url)
            candidates.append(url)
    return candidates


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
    fallback_urls = _resolve_base_url_fallbacks(provider)
    client_urls: List[Optional[str]] = [None, *fallback_urls]
    errors: List[str] = []
    content = ""
    tokens = None
    timeout_seconds = int(LLM_CALL_TIMEOUT_SECONDS)
    effective_parameters = dict(parameters or {})
    timeout_override = effective_parameters.pop("timeout", None)
    timeout_seconds_override = effective_parameters.pop("timeout_seconds", None)
    if timeout_override is None:
        timeout_override = timeout_seconds_override
    if timeout_override is not None:
        try:
            parsed_timeout = int(float(timeout_override))
            if parsed_timeout >= 5:
                timeout_seconds = parsed_timeout
        except Exception:
            pass

    for idx, base_url_candidate in enumerate(client_urls):
        try:
            client = LLMClient(provider=provider, base_url=base_url_candidate)
        except AppError as exc:
            raise exc
        except Exception as exc:
            raise invalid_request_error(str(exc) or ERROR_MESSAGE_LLM_CALL_FAILED)

        try:
            actual_model = str(model or client._default_model or "").strip() or DEFAULT_LLM_MODEL
            key = f"{str(client._provider_name or '').strip() or LLM_PROVIDER_OPENAI}:{actual_model}"
            with _llm_concurrency_guard(key):
                content, tokens = client.complete_prompt_sync(
                    prompt=prompt,
                    model=actual_model,
                    parameters=effective_parameters,
                    timeout=timeout_seconds,
                )
            if str(content or "").strip():
                break
            errors.append(f"attempt#{idx + 1} empty_response")
        except Exception as exc:
            err_text = str(exc) or ERROR_MESSAGE_LLM_CALL_FAILED
            errors.append(f"attempt#{idx + 1} {err_text}")
            error_kind = classify_llm_error_text(err_text)
            can_try_next = idx < len(client_urls) - 1
            if not can_try_next or error_kind not in {"rate_limit", "transient"}:
                raise invalid_request_error(err_text)
            continue
    else:
        summary = " | ".join(errors[:4]) if errors else ERROR_MESSAGE_LLM_CALL_FAILED
        raise invalid_request_error(f"{ERROR_MESSAGE_LLM_CALL_FAILED}: {summary}")

    if not str(content or "").strip():
        raise invalid_request_error(errors[-1] if errors else ERROR_MESSAGE_LLM_CALL_FAILED)

    return content, tokens


def call_openai(prompt: str, model: Optional[str], parameters: Optional[dict]):
    # 兼容旧接口：绝大多数调用方仍使用 call_openai
    try:
        provider_value = resolve_default_provider_raw()
        content, tokens = call_llm(prompt, model, parameters, provider=provider_value)
        return content, tokens, None
    except AppError as exc:
        return None, None, exc.message or ERROR_MESSAGE_LLM_CALL_FAILED
    except Exception as exc:
        return None, None, str(exc) or ERROR_MESSAGE_LLM_CALL_FAILED
