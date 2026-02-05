import unittest

from backend.src.agent.think.think_planning import ThinkPlanResult


class TestThinkPlanOutputAliases(unittest.TestCase):
    def test_to_planning_result_includes_docs_alias_fields(self):
        result = ThinkPlanResult(
            plan_titles=["file_write:README.md 写文档", "task_output 输出最终结果"],
            plan_briefs=["写文档", "输出"],
            plan_allows=[["file_write"], ["task_output"]],
            plan_artifacts=["README.md"],
            winning_planner_id="planner_a",
            alternative_plans=[{"planner_id": "planner_b", "plan": []}],
            vote_records=[{"phase": "final_vote", "votes": {"planner_a": 2}}],
            llm_record_ids=[123],
        )

        payload = result.to_planning_result()

        self.assertIn("alternative_plans", payload)
        self.assertIn("vote_records", payload)
        self.assertIn("plan_alternatives", payload)
        self.assertIn("plan_votes", payload)
        self.assertEqual(payload["plan_alternatives"], payload["alternative_plans"])
        self.assertEqual(payload["plan_votes"], payload["vote_records"])

