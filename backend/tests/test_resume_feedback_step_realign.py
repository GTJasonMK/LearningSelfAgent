import unittest

from backend.src.agent.runner.feedback import realign_feedback_step_for_resume
from backend.src.constants import AGENT_TASK_FEEDBACK_STEP_TITLE, RUN_STATUS_STOPPED, RUN_STATUS_WAITING


class TestResumeFeedbackStepRealign(unittest.TestCase):
    def _build_plan(self):
        return (
            ["步骤A", AGENT_TASK_FEEDBACK_STEP_TITLE, "步骤B"],
            [
                {"id": 1, "brief": "A", "status": "done"},
                {"id": 2, "brief": "确认满意度", "status": "waiting"},
                {"id": 3, "brief": "B", "status": "pending"},
            ],
            [["tool_call"], ["user_prompt"], ["task_output"]],
        )

    def test_waiting_mid_feedback_should_move_to_tail_and_reset_flag(self):
        titles, items, allows = self._build_plan()
        result = realign_feedback_step_for_resume(
            run_status=RUN_STATUS_WAITING,
            plan_titles=titles,
            plan_items=items,
            plan_allows=allows,
            paused_step_order=2,
            paused_step_title=AGENT_TASK_FEEDBACK_STEP_TITLE,
            task_feedback_asked=True,
            max_steps=6,
        )

        self.assertTrue(bool(result.get("reask_feedback")))
        self.assertFalse(bool(result.get("task_feedback_asked")))
        self.assertEqual(titles, ["步骤A", "步骤B", AGENT_TASK_FEEDBACK_STEP_TITLE])
        self.assertEqual(allows[-1], ["user_prompt"])
        self.assertEqual([int(i.get("id") or 0) for i in items], [1, 2, 3])

    def test_waiting_tail_feedback_should_keep_feedback_asked(self):
        titles = ["步骤A", "步骤B", AGENT_TASK_FEEDBACK_STEP_TITLE]
        items = [
            {"id": 1, "brief": "A", "status": "done"},
            {"id": 2, "brief": "B", "status": "done"},
            {"id": 3, "brief": "确认满意度", "status": "waiting"},
        ]
        allows = [["tool_call"], ["task_output"], ["user_prompt"]]

        result = realign_feedback_step_for_resume(
            run_status=RUN_STATUS_WAITING,
            plan_titles=titles,
            plan_items=items,
            plan_allows=allows,
            paused_step_order=3,
            paused_step_title=AGENT_TASK_FEEDBACK_STEP_TITLE,
            task_feedback_asked=True,
            max_steps=6,
        )

        self.assertFalse(bool(result.get("reask_feedback")))
        self.assertTrue(bool(result.get("task_feedback_asked")))
        self.assertEqual(titles, ["步骤A", "步骤B", AGENT_TASK_FEEDBACK_STEP_TITLE])
        self.assertEqual(allows[-1], ["user_prompt"])

    def test_stopped_run_with_stale_feedback_flag_should_reset_flag(self):
        titles, items, allows = self._build_plan()
        result = realign_feedback_step_for_resume(
            run_status=RUN_STATUS_STOPPED,
            plan_titles=titles,
            plan_items=items,
            plan_allows=allows,
            paused_step_order=None,
            paused_step_title="",
            task_feedback_asked=True,
            max_steps=6,
        )

        self.assertFalse(bool(result.get("task_feedback_asked")))
        self.assertFalse(bool(result.get("reask_feedback")))
        self.assertEqual(titles, ["步骤A", "步骤B", AGENT_TASK_FEEDBACK_STEP_TITLE])


if __name__ == "__main__":
    unittest.main()
