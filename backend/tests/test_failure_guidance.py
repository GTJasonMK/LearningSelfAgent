import unittest

from backend.src.agent.runner.failure_guidance import build_failure_guidance
from backend.src.agent.runner.goal_progress import evaluate_goal_progress


class TestFailureGuidance(unittest.TestCase):
    def test_build_failure_guidance_keeps_model_freedom(self):
        state = {
            "goal_progress": {"state": "none", "score": 0},
            "pending_retry_requirements": {
                "active": True,
                "failure_class": "source_unavailable",
                "must_change": ["source_selection", "query_strategy"],
                "retry_constraints": ["禁止继续使用同一外部源。"],
            },
        }
        text = build_failure_guidance(state)
        self.assertIn("source_unavailable", text)
        self.assertIn("不是唯一解", text)
        self.assertIn("扩大候选来源集合", text)

    def test_goal_progress_extracts_generic_requirements(self):
        result = evaluate_goal_progress(
            message="请你帮我收集最近三个月的黄金价格数据，单位元/克，并保存为csv文件",
            title="file_write:gold.csv",
            action_type="file_write",
            result={"output": "黄金价格,元/克\n2026-01-01,680"},
            error_message="",
            visible_content="已生成 gold.csv，单位元/克，最近三个月。",
            context={"latest_shell_artifacts": ["gold.csv"]},
            previous_score=10,
        )
        requirements = result.get("task_requirements") or {}
        matched_requirements = result.get("matched_requirements") or []
        self.assertEqual(str(requirements.get("file_type") or ""), "csv")
        self.assertEqual(str(requirements.get("unit") or ""), "元/克")
        self.assertTrue(bool(requirements.get("needs_tabular")))
        self.assertTrue(any(str(item).startswith("file_type:") for item in matched_requirements))
        self.assertTrue(any(str(item).startswith("unit:") for item in matched_requirements))


if __name__ == "__main__":
    unittest.main()
