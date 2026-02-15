import unittest


class TestReplanContextUnlimited(unittest.TestCase):
    def test_prepare_replan_context_uses_unlimited_when_no_limit(self):
        from backend.src.agent.runner.react_state_manager import prepare_replan_context
        from backend.src.constants import AGENT_MAX_STEPS_UNLIMITED

        ctx = prepare_replan_context(
            step_order=2,
            agent_state={},
            max_steps_limit=None,
            plan_titles=["step1", "step2", "step3"],
        )

        self.assertTrue(ctx.can_replan)
        self.assertEqual(ctx.done_count, 1)
        self.assertEqual(ctx.max_steps_value, int(AGENT_MAX_STEPS_UNLIMITED))

    def test_prepare_replan_context_uses_remaining_when_limited(self):
        from backend.src.agent.runner.react_state_manager import prepare_replan_context

        ctx = prepare_replan_context(
            step_order=2,
            agent_state={},
            max_steps_limit=5,
            plan_titles=["step1", "step2", "step3"],
        )

        self.assertTrue(ctx.can_replan)
        self.assertEqual(ctx.remaining_limit, 4)
        self.assertEqual(ctx.max_steps_value, 4)


if __name__ == "__main__":
    unittest.main()
