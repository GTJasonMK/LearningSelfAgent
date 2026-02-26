import unittest

from backend.src.services.tasks.postprocess.helpers import (
    allow_tool_approval_on_waiting_feedback,
    extract_tool_name_from_tool_call_step,
    find_unverified_text_output,
    is_selftest_title,
)


class TestTaskPostprocessHelpers(unittest.TestCase):
    def test_is_selftest_title(self):
        self.assertTrue(is_selftest_title("执行工具自测"))
        self.assertTrue(is_selftest_title("run self-test for parser"))
        self.assertFalse(is_selftest_title("普通步骤"))

    def test_extract_tool_name_from_tool_call_step(self):
        self.assertEqual(
            "fetch_gold_price",
            extract_tool_name_from_tool_call_step(
                "tool_call:fetch_gold_price run", {"tool_name": "fetch_gold_price"}
            ),
        )
        self.assertEqual(
            "calc",
            extract_tool_name_from_tool_call_step("tool_call:calc execute", None),
        )
        self.assertEqual("", extract_tool_name_from_tool_call_step("", None))

    def test_find_unverified_text_output(self):
        rows = [
            {"id": 1, "output_type": "text", "content": "正常输出"},
            {"id": 2, "output_type": "text", "content": "【未验证草稿】请补证据"},
        ]
        out = find_unverified_text_output(rows)
        self.assertIsInstance(out, dict)
        self.assertEqual(2, out.get("output_id"))

    def test_allow_tool_approval_on_waiting_feedback(self):
        run_row = {
            "status": "waiting",
            "agent_state": '{"paused":{"step_title":"确认满意度"}}',
        }
        self.assertTrue(allow_tool_approval_on_waiting_feedback(run_row))
        self.assertFalse(allow_tool_approval_on_waiting_feedback({"status": "done", "agent_state": "{}"}))


if __name__ == "__main__":
    unittest.main()
