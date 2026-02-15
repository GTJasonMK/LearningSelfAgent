import unittest


class TestLlmProviderAlias(unittest.TestCase):
    def test_deepseek_is_normalized_to_openai(self):
        from backend.src.services.llm.providers.registry import normalize_provider_name
        from backend.src.constants import LLM_PROVIDER_OPENAI

        for name in ("deepseek", "deepseek-openai", "deepseek_openai"):
            self.assertEqual(normalize_provider_name(name), LLM_PROVIDER_OPENAI)


if __name__ == "__main__":
    unittest.main()

