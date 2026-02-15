import unittest
from unittest.mock import patch

from backend.src.agent.core.run_context import AgentRunContext
from backend.src.agent.runner.run_stage import persist_run_stage


class TestRunStage(unittest.IsolatedAsyncioTestCase):
    async def test_persist_run_stage_success_with_sse_event(self):
        run_ctx = AgentRunContext.from_agent_state({}, mode="do", message="m", model="x", workdir="/tmp")

        with patch(
            "backend.src.agent.runner.run_stage.persist_checkpoint_async",
            return_value=None,
        ) as mock_persist:
            state, err, event = await persist_run_stage(
                run_ctx=run_ctx,
                task_id=1,
                run_id=2,
                stage="planning",
                where="unit.run_stage",
            )

        self.assertIsNone(err)
        self.assertEqual(state.get("stage"), "planning")
        self.assertTrue(isinstance(event, str) and "agent_stage" in event)
        self.assertIn("planning", event)
        kwargs = mock_persist.call_args.kwargs
        self.assertEqual(kwargs.get("run_id"), 2)
        self.assertEqual(kwargs.get("where"), "unit.run_stage")

    async def test_persist_run_stage_failure_without_event(self):
        run_ctx = AgentRunContext.from_agent_state({}, mode="do")

        with patch(
            "backend.src.agent.runner.run_stage.persist_checkpoint_async",
            return_value="db_error",
        ):
            state, err, event = await persist_run_stage(
                run_ctx=run_ctx,
                task_id=3,
                run_id=4,
                stage="execute",
                where="unit.run_stage.fail",
            )

        self.assertEqual(state.get("stage"), "execute")
        self.assertEqual(err, "db_error")
        self.assertIsNone(event)


if __name__ == "__main__":
    unittest.main()
