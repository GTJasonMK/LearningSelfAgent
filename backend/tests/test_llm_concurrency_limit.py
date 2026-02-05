import threading
import unittest
from unittest.mock import patch


class TestLLMConcurrencyLimit(unittest.TestCase):
    """
    回归：call_llm 应限制同步 LLM 调用的峰值并发，避免 Think 并行执行时触发供应商限流/抖动。

    说明：
    - 该测试通过 patch LLMClient 为 Fake（不依赖真实 API Key/网络）；
    - 通过 active/peak 统计进入 complete_prompt_sync 的线程数，验证 <= limit。
    """

    def test_call_llm_respects_global_and_per_model_concurrency_limits(self):
        import backend.src.services.llm.llm_client as llm_client

        limit = 2
        active_lock = threading.Lock()
        active = 0
        peak = 0
        entered_limit = threading.Event()
        release = threading.Event()

        class FakeLLMClient:
            def __init__(self, provider=None, api_key=None, base_url=None, default_model=None, strict_mode=False):
                self._provider_name = str(provider or "openai")
                self._default_model = str(default_model or "fake-model")

            def complete_prompt_sync(self, prompt: str, model=None, parameters=None, timeout: int = 120):
                nonlocal active, peak
                with active_lock:
                    active += 1
                    peak = max(peak, active)
                    if active >= limit:
                        entered_limit.set()
                # 阻塞住已进入的线程，制造并发竞争窗口；其他线程应被 semaphore 挡在外面。
                release.wait(timeout=2)
                with active_lock:
                    active -= 1
                return "ok", None

        def _worker():
            llm_client.call_llm(
                prompt="p",
                model="fake-model",
                parameters={"temperature": 0},
                provider="openai",
            )

        with patch.object(llm_client, "AGENT_LLM_MAX_CONCURRENCY_GLOBAL", limit), patch.object(
            llm_client, "AGENT_LLM_MAX_CONCURRENCY_PER_MODEL", limit
        ), patch.object(llm_client, "LLMClient", FakeLLMClient):
            # 重置 semaphore 缓存（避免受其他测试/导入影响）
            llm_client._LLM_CONCURRENCY_STATE["global_limit"] = None
            llm_client._LLM_CONCURRENCY_STATE["per_model_limit"] = None
            llm_client._LLM_CONCURRENCY_STATE["global_sem"] = None
            llm_client._LLM_CONCURRENCY_STATE["model_sems"] = {}

            threads = [threading.Thread(target=_worker, daemon=True) for _ in range(6)]
            for t in threads:
                t.start()

            try:
                self.assertTrue(
                    entered_limit.wait(timeout=2),
                    "未观察到并发进入窗口，可能 semaphore 未生效或线程未启动",
                )
            finally:
                release.set()

            for t in threads:
                t.join(timeout=2)
                self.assertFalse(t.is_alive(), "线程未按预期退出（可能发生死锁）")

        self.assertLessEqual(peak, limit)

