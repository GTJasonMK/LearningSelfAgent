import unittest
from unittest.mock import patch


class TestLlmProviderAlias(unittest.TestCase):
    def test_openai_compatible_aliases_are_normalized_to_openai(self):
        from backend.src.services.llm.providers.registry import normalize_provider_name
        from backend.src.constants import LLM_PROVIDER_OPENAI

        for name in (
            "deepseek",
            "deepseek-openai",
            "deepseek_openai",
            "right",
            "rightcode",
            "rightcodes",
            "right-codes",
            "right_codes",
            "right.codes",
            "rc",
        ):
            self.assertEqual(normalize_provider_name(name), LLM_PROVIDER_OPENAI)

    def test_llm_client_reads_right_codes_env_fallbacks(self):
        import backend.src.services.llm.llm_client as llm_client

        captured = {}

        def _fake_create_provider(*, provider, api_key, base_url, default_model):
            captured["provider"] = provider
            captured["api_key"] = api_key
            captured["base_url"] = base_url
            captured["default_model"] = default_model
            return object()

        with patch.object(
            llm_client.LLMClient,
            "_load_store_config",
            return_value={"provider": None, "api_key": None, "base_url": None, "model": None},
        ), patch.dict(
            llm_client.os.environ,
            {
                "OPENAI_API_KEY": "",
                "OPENAI_BASE_URL": "",
                "OPENAI_API_BASE": "",
                "RIGHT_CODES_API_KEY": "rc-test-key",
                "RIGHT_CODES_BASE_URL": "https://api.example-right.codes/v1",
            },
            clear=False,
        ), patch.object(llm_client, "create_provider", side_effect=_fake_create_provider):
            llm_client.LLMClient(provider="right.codes", default_model="test-model")

        self.assertEqual(captured.get("provider"), llm_client.LLM_PROVIDER_OPENAI)
        self.assertEqual(captured.get("api_key"), "rc-test-key")
        self.assertEqual(captured.get("base_url"), "https://api.example-right.codes/v1")
        self.assertEqual(captured.get("default_model"), "test-model")

    def test_llm_client_uses_right_codes_default_base_url_when_missing(self):
        import backend.src.services.llm.llm_client as llm_client

        captured = {}

        def _fake_create_provider(*, provider, api_key, base_url, default_model):
            captured["provider"] = provider
            captured["api_key"] = api_key
            captured["base_url"] = base_url
            captured["default_model"] = default_model
            return object()

        with patch.object(
            llm_client.LLMClient,
            "_load_store_config",
            return_value={"provider": None, "api_key": None, "base_url": None, "model": None},
        ), patch.dict(
            llm_client.os.environ,
            {
                "OPENAI_API_KEY": "",
                "OPENAI_BASE_URL": "",
                "OPENAI_API_BASE": "",
                "RIGHT_CODES_API_KEY": "rc-test-key",
                "RIGHT_CODES_BASE_URL": "",
            },
            clear=False,
        ), patch.object(llm_client, "create_provider", side_effect=_fake_create_provider):
            llm_client.LLMClient(provider="rightcode", default_model="test-model")

        self.assertEqual(captured.get("provider"), llm_client.LLM_PROVIDER_OPENAI)
        self.assertEqual(captured.get("api_key"), "rc-test-key")
        self.assertEqual(captured.get("base_url"), "https://right.codes/codex/v1")
        self.assertEqual(captured.get("default_model"), "test-model")


if __name__ == "__main__":
    unittest.main()

