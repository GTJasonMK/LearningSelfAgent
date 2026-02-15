import json
import unittest


class TestReviewRepairParseSteps(unittest.TestCase):
    def test_parse_insert_steps_infers_allow_from_title_prefix(self):
        from backend.src.agent.runner.review_repair import parse_insert_steps_from_text

        payload = {
            "insert_steps": [
                {
                    "title": "shell_command:运行验证",
                    "brief": "执行验证",
                }
            ]
        }
        steps = parse_insert_steps_from_text(json.dumps(payload, ensure_ascii=False))
        self.assertIsInstance(steps, list)
        self.assertEqual(len(steps), 1)
        self.assertEqual(steps[0]["allow"], ["shell_command"])

    def test_parse_insert_steps_fallback_to_task_output_when_allow_missing(self):
        from backend.src.agent.runner.review_repair import parse_insert_steps_from_text

        payload = {
            "steps": [
                {
                    "title": "补充最终说明",
                    "brief": "补充",
                    "allow": [],
                }
            ]
        }
        steps = parse_insert_steps_from_text(json.dumps(payload, ensure_ascii=False))
        self.assertIsInstance(steps, list)
        self.assertEqual(steps[0]["allow"], ["task_output"])

    def test_parse_insert_steps_skips_invalid_items(self):
        from backend.src.agent.runner.review_repair import parse_insert_steps_from_text

        payload = {
            "insert_steps": [
                {"brief": "无标题"},
                "bad",
                {"title": "file_write:out.txt 写入", "allow": ["file_write"]},
            ]
        }
        steps = parse_insert_steps_from_text(json.dumps(payload, ensure_ascii=False))
        self.assertIsInstance(steps, list)
        self.assertEqual(len(steps), 1)
        self.assertEqual(steps[0]["title"], "file_write:out.txt 写入")


if __name__ == "__main__":
    unittest.main()
