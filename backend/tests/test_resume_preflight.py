import unittest
from unittest.mock import patch

from backend.src.agent.runner.resume_preflight import (
    apply_resume_user_input,
    infer_resume_step_decision,
    normalize_plan_items_for_resume,
)
from backend.src.constants import ACTION_TYPE_USER_PROMPT


class TestResumePreflight(unittest.TestCase):
    def test_infer_resume_step_decision_prefers_last_active_step(self):
        result = infer_resume_step_decision(
            paused_step_order=None,
            state_step_order=1,
            last_done_step=1,
            last_active_step_order=2,
            last_active_step_status="running",
            plan_total_steps=4,
            pending_planning=False,
        )
        self.assertEqual(result.resume_step_order, 2)
        self.assertFalse(result.skip_execution)

    def test_infer_resume_step_decision_detects_skip_execution(self):
        result = infer_resume_step_decision(
            paused_step_order=None,
            state_step_order=10,
            last_done_step=4,
            last_active_step_order=4,
            last_active_step_status="done",
            plan_total_steps=4,
            pending_planning=False,
        )
        self.assertEqual(result.resume_step_order, 5)
        self.assertTrue(result.skip_execution)

    def test_normalize_plan_items_for_resume(self):
        items = [
            {"id": 1, "status": "pending"},
            {"id": 2, "status": "running"},
            {"id": 3, "status": "planned"},
        ]
        normalize_plan_items_for_resume(plan_items=items, last_done_step=1)
        self.assertEqual(items[0]["status"], "done")
        self.assertEqual(items[1]["status"], "pending")
        self.assertEqual(items[2]["status"], "pending")

    def test_apply_resume_user_input_user_prompt_only_advances_step(self):
        async def _run_case():
            plan_titles = ["user_prompt: 请补充", "执行步骤"]
            plan_items = [{"id": 1, "status": "pending"}, {"id": 2, "status": "pending"}]
            plan_allows = [[ACTION_TYPE_USER_PROMPT], ["shell_command"]]
            state_obj = {"paused": {"step_id": 8}, "step_order": 1}

            with patch(
                "backend.src.agent.runner.resume_preflight.persist_run_stage",
                side_effect=lambda **kwargs: (dict(kwargs["run_ctx"].state), None, []),
            ), patch(
                "backend.src.agent.runner.resume_preflight.persist_checkpoint_async",
                return_value=None,
            ), patch(
                "backend.src.agent.runner.resume_preflight.create_task_output",
                return_value=9,
            ), patch(
                "backend.src.agent.runner.resume_preflight.mark_task_step_done",
                return_value=True,
            ), patch(
                "backend.src.agent.runner.resume_preflight.update_task",
                return_value=True,
            ), patch(
                "backend.src.agent.runner.resume_preflight.now_iso",
                return_value="2026-01-01T00:00:00Z",
            ):
                next_step, next_state = await apply_resume_user_input(
                    task_id=1,
                    run_id=2,
                    user_input="补充信息",
                    question="请补充",
                    paused={"step_id": 8},
                    paused_step_order=1,
                    resume_step_order=1,
                    plan_titles=plan_titles,
                    plan_items=plan_items,
                    plan_allows=plan_allows,
                    plan_artifacts=[],
                    observations=["obs-1"],
                    context={"k": "v"},
                    state_obj=state_obj,
                    safe_write_debug=lambda *_a, **_k: None,
                    is_task_feedback_step_title_func=lambda _title: False,
                )
            self.assertEqual(next_step, 2)
            self.assertEqual(plan_items[0]["status"], "done")
            self.assertEqual(next_state.get("paused"), None)
            self.assertEqual(next_state.get("step_order"), 2)

        import asyncio

        asyncio.run(_run_case())


if __name__ == "__main__":
    unittest.main()
