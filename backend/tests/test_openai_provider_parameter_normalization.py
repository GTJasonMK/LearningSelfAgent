import unittest


class TestOpenAIProviderParameterNormalization(unittest.TestCase):
    def test_maps_max_output_tokens_to_max_tokens(self):
        from backend.src.services.llm.providers.openai_provider import OpenAIProvider

        normalized = OpenAIProvider._normalize_chat_completions_params(
            {"temperature": 0.2, "max_output_tokens": 900}
        )
        self.assertEqual(normalized.get("temperature"), 0.2)
        self.assertEqual(normalized.get("max_tokens"), 900)
        self.assertNotIn("max_output_tokens", normalized)

    def test_preserves_explicit_max_tokens(self):
        from backend.src.services.llm.providers.openai_provider import OpenAIProvider

        normalized = OpenAIProvider._normalize_chat_completions_params(
            {"max_tokens": 256, "max_output_tokens": 900}
        )
        self.assertEqual(normalized.get("max_tokens"), 256)
        self.assertNotIn("max_output_tokens", normalized)

    def test_drops_none_values(self):
        from backend.src.services.llm.providers.openai_provider import OpenAIProvider

        normalized = OpenAIProvider._normalize_chat_completions_params(
            {"temperature": 0.2, "stop": None}
        )
        self.assertEqual(normalized, {"temperature": 0.2})


if __name__ == "__main__":
    unittest.main()
