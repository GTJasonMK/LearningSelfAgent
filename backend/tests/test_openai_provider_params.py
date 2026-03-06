import unittest


class TestOpenAIProviderParams(unittest.TestCase):
    def test_normalize_chat_completions_params_strips_runtime_only_controls(self):
        from backend.src.services.llm.providers.openai_provider import OpenAIProvider

        normalized = OpenAIProvider._normalize_chat_completions_params(
            {
                "temperature": 0,
                "max_output_tokens": 256,
                "retry_max_attempts": 2,
                "max_attempts": 3,
                "hard_timeout_seconds": 20,
                "timeout_seconds": 18,
            }
        )

        self.assertEqual(0, normalized.get("temperature"))
        self.assertEqual(256, normalized.get("max_tokens"))
        self.assertNotIn("max_output_tokens", normalized)
        self.assertNotIn("retry_max_attempts", normalized)
        self.assertNotIn("max_attempts", normalized)
        self.assertNotIn("hard_timeout_seconds", normalized)
        self.assertNotIn("timeout_seconds", normalized)


if __name__ == "__main__":
    unittest.main()
