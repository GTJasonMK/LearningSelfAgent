import unittest
from unittest.mock import patch


class TestCallOpenaiProviderPassthrough(unittest.TestCase):
    def test_call_openai_uses_raw_default_provider(self):
        import backend.src.services.llm.llm_client as llm_client

        with patch.object(
            llm_client,
            "resolve_default_provider_raw",
            return_value="right.codes",
        ), patch.object(
            llm_client,
            "call_llm",
            return_value=("ok", {"total": 1}),
        ) as mocked_call_llm:
            content, tokens, err = llm_client.call_openai(
                prompt="hello",
                model="m1",
                parameters={"temperature": 0},
            )

        self.assertEqual(content, "ok")
        self.assertEqual(tokens, {"total": 1})
        self.assertIsNone(err)
        self.assertEqual(
            mocked_call_llm.call_args.kwargs.get("provider"),
            "right.codes",
        )

    def test_call_openai_returns_error_text_when_call_llm_fails(self):
        import backend.src.services.llm.llm_client as llm_client

        with patch.object(
            llm_client,
            "resolve_default_provider_raw",
            return_value="openai",
        ), patch.object(
            llm_client,
            "call_llm",
            side_effect=RuntimeError("boom"),
        ):
            content, tokens, err = llm_client.call_openai(
                prompt="hello",
                model="m1",
                parameters={"temperature": 0},
            )

        self.assertIsNone(content)
        self.assertIsNone(tokens)
        self.assertEqual(err, "boom")


if __name__ == "__main__":
    unittest.main()
