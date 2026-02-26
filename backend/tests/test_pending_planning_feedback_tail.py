import unittest

from backend.src.agent.runner.feedback import is_task_feedback_step_title
from backend.src.agent.runner.pending_planning_flow import _ensure_single_feedback_tail
from backend.src.constants import AGENT_TASK_FEEDBACK_STEP_TITLE


class PendingPlanningFeedbackTailTests(unittest.TestCase):
    def _assert_plan_integrity(self, titles, items, allows):
        self.assertEqual(len(titles), len(items))
        self.assertEqual(len(titles), len(allows))
        for idx, item in enumerate(items, start=1):
            self.assertEqual(int(item.get("id") or 0), idx)

    def test_moves_feedback_step_to_tail_and_keeps_single(self):
        title = str(AGENT_TASK_FEEDBACK_STEP_TITLE or "").strip()
        if not title:
            self.skipTest("AGENT_TASK_FEEDBACK_STEP_TITLE 未配置")

        plan_titles = ["抓取网页", title, "整理结果"]
        plan_items = [
            {"id": 1, "brief": "抓取", "status": "pending"},
            {"id": 2, "brief": "反馈", "status": "pending"},
            {"id": 3, "brief": "整理", "status": "pending"},
        ]
        plan_allows = [["http_request"], ["user_prompt"], ["task_output"]]

        _ensure_single_feedback_tail(
            plan_titles=plan_titles,
            plan_items=plan_items,
            plan_allows=plan_allows,
        )

        feedback_indexes = [idx for idx, value in enumerate(plan_titles) if is_task_feedback_step_title(value)]
        self.assertEqual(feedback_indexes, [len(plan_titles) - 1])
        self._assert_plan_integrity(plan_titles, plan_items, plan_allows)

    def test_appends_feedback_tail_when_missing(self):
        title = str(AGENT_TASK_FEEDBACK_STEP_TITLE or "").strip()
        if not title:
            self.skipTest("AGENT_TASK_FEEDBACK_STEP_TITLE 未配置")

        plan_titles = ["检索资料", "输出结论"]
        plan_items = [
            {"id": 1, "brief": "检索", "status": "pending"},
            {"id": 2, "brief": "输出", "status": "pending"},
        ]
        plan_allows = [["http_request"], ["task_output"]]

        _ensure_single_feedback_tail(
            plan_titles=plan_titles,
            plan_items=plan_items,
            plan_allows=plan_allows,
        )

        self.assertTrue(is_task_feedback_step_title(plan_titles[-1]))
        feedback_count = sum(1 for value in plan_titles if is_task_feedback_step_title(value))
        self.assertEqual(feedback_count, 1)
        self._assert_plan_integrity(plan_titles, plan_items, plan_allows)


if __name__ == "__main__":
    unittest.main()
