import unittest
import time
from unittest.mock import patch


class TestLlmCallsRetry(unittest.TestCase):
    def test_retry_transient_error_then_success(self):
        from backend.src.services.llm.llm_calls import create_llm_call

        attempts = {"count": 0}

        def fake_call_llm(prompt: str, model: str, parameters: dict, provider: str = ""):
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise Exception("Connection error.")
            return "ok", {"prompt": 1, "completion": 1, "total": 2}

        with patch("backend.src.services.llm.llm_calls.call_llm", side_effect=fake_call_llm), patch(
            "backend.src.services.llm.llm_calls.time.sleep", return_value=None
        ):
            result = create_llm_call({"prompt": "hello", "provider": "openai", "model": "deepseek-chat"})

        self.assertEqual(attempts["count"], 3)
        self.assertEqual(str(result["record"].get("status") or ""), "success")
        self.assertEqual(str(result["record"].get("response") or ""), "ok")

    def test_non_transient_error_no_retry(self):
        from backend.src.services.llm.llm_calls import create_llm_call

        attempts = {"count": 0}

        def fake_call_llm(prompt: str, model: str, parameters: dict, provider: str = ""):
            attempts["count"] += 1
            raise Exception("invalid api key")

        with patch("backend.src.services.llm.llm_calls.call_llm", side_effect=fake_call_llm), patch(
            "backend.src.services.llm.llm_calls.time.sleep", return_value=None
        ):
            result = create_llm_call({"prompt": "hello", "provider": "openai", "model": "deepseek-chat"})

        self.assertEqual(attempts["count"], 1)
        self.assertEqual(str(result["record"].get("status") or ""), "error")
        self.assertIn("invalid api key", str(result["record"].get("error") or ""))

    def test_hard_timeout_marks_error_without_hanging(self):
        from backend.src.services.llm.llm_calls import create_llm_call

        def fake_call_llm(prompt: str, model: str, parameters: dict, provider: str = ""):
            _ = prompt, model, parameters, provider
            time.sleep(2.0)
            return "ok", {"prompt": 1, "completion": 1, "total": 2}

        with patch("backend.src.services.llm.llm_calls.call_llm", side_effect=fake_call_llm), patch(
            "backend.src.services.llm.llm_calls.LLM_CALL_HARD_TIMEOUT_SECONDS", 1
        ), patch(
            "backend.src.services.llm.llm_calls.LLM_CALL_MAX_ATTEMPTS", 1
        ):
            result = create_llm_call({"prompt": "hello", "provider": "openai", "model": "deepseek-chat"})

        self.assertEqual(str(result["record"].get("status") or ""), "error")
        self.assertIn("timeout", str(result["record"].get("error") or "").lower())


if __name__ == "__main__":
    unittest.main()
