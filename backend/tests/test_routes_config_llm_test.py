import json
import unittest
from unittest.mock import patch


class _FakeLLMClient:
    last_init_kwargs = None
    last_complete_kwargs = None
    last_closed = False

    def __init__(self, *, provider=None, api_key=None, base_url=None, default_model=None, strict_mode=False):
        type(self).last_init_kwargs = {
            "provider": provider,
            "api_key": api_key,
            "base_url": base_url,
            "default_model": default_model,
            "strict_mode": strict_mode,
        }
        type(self).last_closed = False
        self._default_model = default_model or "fake-model"

    async def complete_prompt(self, *, prompt, model, parameters, timeout):
        type(self).last_complete_kwargs = {
            "prompt": prompt,
            "model": model,
            "parameters": parameters,
            "timeout": timeout,
        }
        return "OK", {"total": 1}

    async def aclose(self):
        type(self).last_closed = True


class TestRoutesConfigLlmTest(unittest.IsolatedAsyncioTestCase):
    async def test_llm_test_uses_payload_overrides_and_returns_ok(self):
        try:
            from backend.src.api.schemas import LLMConfigUpdate
            from backend.src.api.system.routes_config import test_llm_config
        except ModuleNotFoundError as exc:
            if "fastapi" in str(exc) or "pydantic" in str(exc):
                self.skipTest("fastapi/pydantic 未安装，跳过路由导入测试")
            raise

        with patch(
            "backend.src.api.system.routes_config.fetch_llm_store_config",
            return_value={
                "provider": "openai",
                "api_key": "stored-key",
                "base_url": "https://stored.example/v1",
                "model": "stored-model",
            },
        ), patch(
            "backend.src.api.system.routes_config.LLMClient",
            _FakeLLMClient,
        ):
            result = await test_llm_config(
                LLMConfigUpdate(
                    provider="rightcode",
                    api_key="payload-key",
                    base_url="https://right.codes/codex/v1",
                    model="gpt-5.2",
                )
            )

        self.assertTrue(isinstance(result, dict))
        self.assertTrue(result.get("ok"))
        self.assertEqual("rightcode", result.get("provider"))
        self.assertEqual("gpt-5.2", result.get("model"))
        self.assertEqual("OK", result.get("response_preview"))
        self.assertEqual("payload-key", _FakeLLMClient.last_init_kwargs.get("api_key"))
        self.assertTrue(_FakeLLMClient.last_init_kwargs.get("strict_mode"))
        self.assertTrue(_FakeLLMClient.last_closed)
        self.assertEqual("请仅回复：OK", _FakeLLMClient.last_complete_kwargs.get("prompt"))

    async def test_llm_test_without_api_key_returns_error_response(self):
        try:
            from backend.src.api.schemas import LLMConfigUpdate
            from backend.src.api.system.routes_config import test_llm_config
        except ModuleNotFoundError as exc:
            if "fastapi" in str(exc) or "pydantic" in str(exc):
                self.skipTest("fastapi/pydantic 未安装，跳过路由导入测试")
            raise

        with patch(
            "backend.src.api.system.routes_config.fetch_llm_store_config",
            return_value={
                "provider": None,
                "api_key": None,
                "base_url": None,
                "model": None,
            },
        ):
            response = await test_llm_config(LLMConfigUpdate())

        self.assertEqual(400, getattr(response, "status_code", None))
        body = json.loads(bytes(response.body).decode("utf-8"))
        message = str(((body or {}).get("error") or {}).get("message") or "")
        self.assertTrue(message)
        self.assertIn("未配置", message)


if __name__ == "__main__":
    unittest.main()
