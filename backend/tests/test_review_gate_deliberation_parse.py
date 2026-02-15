import json
import unittest


class TestReviewGateDeliberationParse(unittest.TestCase):
    def test_parse_decision_proceed_feedback(self):
        from backend.src.agent.runner.review_repair import (
            REVIEW_GATE_DECISION_PROCEED_FEEDBACK,
            parse_review_gate_decision_from_text,
        )

        payload = {
            "decision": "proceed_feedback",
            "reasons": ["当前结果可先交由用户确认"],
            "evidence": ["评估建议优先收集主观反馈"],
            "insert_steps": [],
        }
        result = parse_review_gate_decision_from_text(json.dumps(payload, ensure_ascii=False))

        self.assertEqual(result.decision, REVIEW_GATE_DECISION_PROCEED_FEEDBACK)
        self.assertEqual(result.insert_steps, None)
        self.assertIn("当前结果可先交由用户确认", result.reasons)
        self.assertEqual(result.parse_error, None)

    def test_parse_decision_infers_repair_from_insert_steps(self):
        from backend.src.agent.runner.review_repair import (
            REVIEW_GATE_DECISION_REPAIR,
            parse_review_gate_decision_from_text,
        )

        payload = {
            "insert_steps": [
                {
                    "title": "shell_command:运行验证",
                    "brief": "执行验证",
                    "allow": ["shell_command"],
                }
            ]
        }
        result = parse_review_gate_decision_from_text(json.dumps(payload, ensure_ascii=False))

        self.assertEqual(result.decision, REVIEW_GATE_DECISION_REPAIR)
        self.assertIsInstance(result.insert_steps, list)
        self.assertEqual(len(result.insert_steps), 1)
        self.assertEqual(result.parse_error, None)

    def test_parse_decision_invalid_json_fallback(self):
        from backend.src.agent.runner.review_repair import (
            REVIEW_GATE_DECISION_PROCEED_FEEDBACK,
            parse_review_gate_decision_from_text,
        )

        result = parse_review_gate_decision_from_text("not json")
        self.assertEqual(result.decision, REVIEW_GATE_DECISION_PROCEED_FEEDBACK)
        self.assertEqual(result.parse_error, "decision_output_invalid")


if __name__ == "__main__":
    unittest.main()
