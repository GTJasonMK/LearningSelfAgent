import unittest


class TestFeedbackStepCanonicalize(unittest.TestCase):
    def test_canonicalize_keeps_single_tail_feedback(self):
        from backend.src.agent.runner.feedback import canonicalize_task_feedback_steps
        from backend.src.constants import ACTION_TYPE_USER_PROMPT, AGENT_TASK_FEEDBACK_STEP_TITLE

        titles = ["步骤1", AGENT_TASK_FEEDBACK_STEP_TITLE, "步骤2", AGENT_TASK_FEEDBACK_STEP_TITLE]
        allows = [["tool_call"], [ACTION_TYPE_USER_PROMPT], ["shell_command"], [ACTION_TYPE_USER_PROMPT]]
        items = [
            {"id": 1, "brief": "b1", "status": "done"},
            {"id": 2, "brief": "fb1", "status": "pending"},
            {"id": 3, "brief": "b2", "status": "pending"},
            {"id": 4, "brief": "fb2", "status": "pending"},
        ]

        result = canonicalize_task_feedback_steps(
            plan_titles=titles,
            plan_items=items,
            plan_allows=allows,
            keep_single_tail=True,
            feedback_asked=False,
            max_steps=None,
        )

        self.assertTrue(bool(result.get("changed")))
        self.assertEqual(int(result.get("found") or 0), 2)
        self.assertEqual(int(result.get("removed") or 0), 1)
        self.assertTrue(bool(result.get("appended")))
        self.assertEqual(titles, ["步骤1", "步骤2", AGENT_TASK_FEEDBACK_STEP_TITLE])
        self.assertEqual(allows[-1], [ACTION_TYPE_USER_PROMPT])
        self.assertEqual(len(items), 3)
        self.assertEqual([it.get("id") for it in items], [1, 2, 3])

    def test_canonicalize_drops_feedback_when_already_asked(self):
        from backend.src.agent.runner.feedback import canonicalize_task_feedback_steps
        from backend.src.constants import ACTION_TYPE_USER_PROMPT, AGENT_TASK_FEEDBACK_STEP_TITLE

        titles = ["步骤1", AGENT_TASK_FEEDBACK_STEP_TITLE]
        allows = [["task_output"], [ACTION_TYPE_USER_PROMPT]]
        items = [{"id": 1, "brief": "out", "status": "done"}, {"id": 2, "brief": "fb", "status": "pending"}]

        result = canonicalize_task_feedback_steps(
            plan_titles=titles,
            plan_items=items,
            plan_allows=allows,
            keep_single_tail=True,
            feedback_asked=True,
            max_steps=None,
        )

        self.assertTrue(bool(result.get("changed")))
        self.assertEqual(titles, ["步骤1"])
        self.assertEqual(len(items), 1)
        self.assertFalse(any("确认满意度" in str(t) for t in titles))
        self.assertFalse(bool(result.get("appended")))

    def test_canonicalize_no_feedback_no_change(self):
        from backend.src.agent.runner.feedback import canonicalize_task_feedback_steps

        titles = ["步骤1", "步骤2"]
        allows = [["file_write"], ["shell_command"]]
        items = [{"id": 1, "brief": "a", "status": "done"}, {"id": 2, "brief": "b", "status": "pending"}]

        result = canonicalize_task_feedback_steps(
            plan_titles=titles,
            plan_items=items,
            plan_allows=allows,
            keep_single_tail=False,
            feedback_asked=False,
            max_steps=None,
        )

        self.assertFalse(bool(result.get("changed")))
        self.assertEqual(titles, ["步骤1", "步骤2"])
        self.assertEqual(len(items), 2)

    def test_canonicalize_repositions_feedback_under_full_max_steps(self):
        from backend.src.agent.runner.feedback import canonicalize_task_feedback_steps
        from backend.src.constants import AGENT_TASK_FEEDBACK_STEP_TITLE

        # 回归：当计划长度已经等于 max_steps 且反馈步骤在中间时，
        # 不能因为“append 超限”而保留中间反馈，应该重排到尾部。
        titles = ["步骤1", AGENT_TASK_FEEDBACK_STEP_TITLE, "步骤2"]
        allows = [["file_write"], ["user_prompt"], ["task_output"]]
        items = [
            {"id": 1, "brief": "a", "status": "pending"},
            {"id": 2, "brief": "fb", "status": "pending"},
            {"id": 3, "brief": "b", "status": "pending"},
        ]

        result = canonicalize_task_feedback_steps(
            plan_titles=titles,
            plan_items=items,
            plan_allows=allows,
            keep_single_tail=True,
            feedback_asked=False,
            max_steps=3,
        )

        self.assertTrue(bool(result.get("changed")))
        self.assertEqual(int(result.get("found") or 0), 1)
        self.assertEqual(titles, ["步骤1", "步骤2", AGENT_TASK_FEEDBACK_STEP_TITLE])


if __name__ == "__main__":
    unittest.main()
