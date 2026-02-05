import unittest
from unittest.mock import patch


class TestFaultInjectionLLMRateLimitAndEmptyOutput(unittest.TestCase):
    def test_rate_limit_reduces_adaptive_concurrency(self):
        import backend.src.services.llm.llm_client as llm_client
        from backend.src.common.errors import AppError

        limit = 3

        class FakeLLMClient:
            def __init__(self, provider=None, api_key=None, base_url=None, default_model=None, strict_mode=False):
                self._provider_name = str(provider or "openai")
                self._default_model = str(default_model or "fake-model")

            def complete_prompt_sync(self, prompt: str, model=None, parameters=None, timeout: int = 120):
                raise RuntimeError("429 Too Many Requests")

        with (
            patch.object(llm_client, "AGENT_LLM_MAX_CONCURRENCY_GLOBAL", limit),
            patch.object(llm_client, "AGENT_LLM_MAX_CONCURRENCY_PER_MODEL", limit),
            patch.object(llm_client, "LLMClient", FakeLLMClient),
        ):
            # 重置并发状态缓存（避免受其他测试影响）
            llm_client._LLM_CONCURRENCY_STATE["global_limit"] = None
            llm_client._LLM_CONCURRENCY_STATE["per_model_limit"] = None
            llm_client._LLM_CONCURRENCY_STATE["global_sem"] = None
            llm_client._LLM_CONCURRENCY_STATE["model_sems"] = {}
            llm_client._LLM_CONCURRENCY_STATE["adaptive_global"] = None
            llm_client._LLM_CONCURRENCY_STATE["adaptive_models"] = {}

            with self.assertRaises(AppError):
                llm_client.call_llm(prompt="p", model="fake-model", parameters={"temperature": 0}, provider="openai")

            global_limiter = llm_client._LLM_CONCURRENCY_STATE.get("adaptive_global")
            self.assertIsNotNone(global_limiter)
            self.assertLessEqual(int(getattr(global_limiter, "current_limit", 0)), limit - 1)

            model_limiter = llm_client._LLM_CONCURRENCY_STATE.get("adaptive_models", {}).get("openai:fake-model")
            self.assertIsNotNone(model_limiter)
            self.assertLessEqual(int(getattr(model_limiter, "current_limit", 0)), limit - 1)

    def test_empty_output_raises_app_error(self):
        import backend.src.services.llm.llm_client as llm_client
        from backend.src.common.errors import AppError

        class FakeLLMClient:
            def __init__(self, provider=None, api_key=None, base_url=None, default_model=None, strict_mode=False):
                self._provider_name = str(provider or "openai")
                self._default_model = str(default_model or "fake-model")

            def complete_prompt_sync(self, prompt: str, model=None, parameters=None, timeout: int = 120):
                return "", None

        with patch.object(llm_client, "LLMClient", FakeLLMClient):
            with self.assertRaises(AppError):
                llm_client.call_llm(prompt="p", model="fake-model", parameters={"temperature": 0}, provider="openai")


if __name__ == "__main__":
    unittest.main()

