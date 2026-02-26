import contextlib
import os
import unittest
from unittest.mock import patch


class TestLlmBaseUrlFallback(unittest.TestCase):
    def test_call_llm_tries_fallback_base_urls_on_transient_errors(self):
        import backend.src.services.llm.llm_client as llm_client

        calls = []

        class FakeClient:
            def __init__(self, provider=None, api_key=None, base_url=None, default_model=None, strict_mode=False):
                _ = provider, api_key, default_model, strict_mode
                self._provider_name = "openai"
                self._default_model = "m1"
                self._base_url = base_url

            def complete_prompt_sync(self, prompt, model=None, parameters=None, timeout=120):
                _ = prompt, model, parameters, timeout
                calls.append(self._base_url)
                if self._base_url is None:
                    raise RuntimeError("429 rate limit")
                if str(self._base_url).startswith("https://fallback-a"):
                    raise RuntimeError("timeout")
                return "ok", {"total": 1}

        @contextlib.contextmanager
        def _fake_guard(_key):
            yield

        old = os.getenv("LLM_BASE_URL_FALLBACKS")
        os.environ["LLM_BASE_URL_FALLBACKS"] = "https://fallback-a/v1,https://fallback-b/v1"
        try:
            with patch.object(llm_client, "LLMClient", FakeClient), patch.object(
                llm_client, "_llm_concurrency_guard", side_effect=_fake_guard
            ):
                out, _tokens = llm_client.call_llm("hello", "m1", {"temperature": 0}, provider="openai")
        finally:
            if old is None:
                os.environ.pop("LLM_BASE_URL_FALLBACKS", None)
            else:
                os.environ["LLM_BASE_URL_FALLBACKS"] = old

        self.assertEqual(out, "ok")
        self.assertEqual(calls, [None, "https://fallback-a/v1", "https://fallback-b/v1"])

    def test_call_llm_does_not_fallback_on_permanent_error(self):
        import backend.src.services.llm.llm_client as llm_client

        calls = []

        class FakeClient:
            def __init__(self, provider=None, api_key=None, base_url=None, default_model=None, strict_mode=False):
                _ = provider, api_key, default_model, strict_mode
                self._provider_name = "openai"
                self._default_model = "m1"
                self._base_url = base_url

            def complete_prompt_sync(self, prompt, model=None, parameters=None, timeout=120):
                _ = prompt, model, parameters, timeout
                calls.append(self._base_url)
                raise RuntimeError("invalid api key")

        @contextlib.contextmanager
        def _fake_guard(_key):
            yield

        old = os.getenv("LLM_BASE_URL_FALLBACKS")
        os.environ["LLM_BASE_URL_FALLBACKS"] = "https://fallback-a/v1,https://fallback-b/v1"
        try:
            with patch.object(llm_client, "LLMClient", FakeClient), patch.object(
                llm_client, "_llm_concurrency_guard", side_effect=_fake_guard
            ):
                with self.assertRaises(Exception):
                    llm_client.call_llm("hello", "m1", {"temperature": 0}, provider="openai")
        finally:
            if old is None:
                os.environ.pop("LLM_BASE_URL_FALLBACKS", None)
            else:
                os.environ["LLM_BASE_URL_FALLBACKS"] = old

        self.assertEqual(calls, [None])


if __name__ == "__main__":
    unittest.main()
