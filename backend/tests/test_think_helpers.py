import unittest
from unittest.mock import patch

from backend.src.agent.runner.think_helpers import build_plan_briefs_from_items, create_llm_call_func


class TestThinkHelpers(unittest.TestCase):
    def test_build_plan_briefs_from_items_prefers_item_brief(self):
        briefs = build_plan_briefs_from_items(
            plan_titles=["step one", "step two"],
            plan_items=[{"brief": "b1"}, {}],
        )
        self.assertEqual(briefs[0], "b1")
        self.assertEqual(briefs[1], "step two"[:10])

    def test_create_llm_call_func_merges_parameters(self):
        with patch(
            "backend.src.agent.runner.think_helpers.call_openai",
            return_value=("ok", 11, None),
        ) as mock_call:
            llm = create_llm_call_func(base_model="m1", base_parameters={"temperature": 0.2})
            text, record_id = llm("prompt", "", {"max_tokens": 256})

        self.assertEqual(text, "ok")
        self.assertEqual(record_id, 11)
        args = mock_call.call_args.args
        self.assertEqual(args[0], "prompt")
        self.assertEqual(args[1], "m1")
        self.assertEqual(args[2], {"temperature": 0.2, "max_tokens": 256})

    def test_create_llm_call_func_on_error_hook(self):
        errors = []
        with patch(
            "backend.src.agent.runner.think_helpers.call_openai",
            return_value=("", None, "boom"),
        ):
            llm = create_llm_call_func(
                base_model="m1",
                base_parameters={},
                on_error=lambda err: errors.append(err),
            )
            text, record_id = llm("prompt", "m2", {})

        self.assertEqual(text, "")
        self.assertIsNone(record_id)
        self.assertEqual(errors, ["boom"])


if __name__ == "__main__":
    unittest.main()
