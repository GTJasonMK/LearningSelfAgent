import unittest
from unittest.mock import patch

from backend.src.agent.core.run_context import AgentRunContext
from backend.src.agent.runner.run_stage import is_legal_run_stage_transition, persist_run_stage


class TestRunStage(unittest.IsolatedAsyncioTestCase):
    def test_is_legal_run_stage_transition(self):
        self.assertTrue(is_legal_run_stage_transition("retrieval", "planning"))
        self.assertTrue(is_legal_run_stage_transition("planning", "execute"))
        self.assertFalse(is_legal_run_stage_transition("execute", "retrieval"))

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

    async def test_persist_run_stage_blocked_transition_keeps_current_stage(self):
        run_ctx = AgentRunContext.from_agent_state(
            {"stage": "execute"},
            mode="do",
            message="m",
            model="x",
            workdir="/tmp",
        )

        with patch(
            "backend.src.agent.runner.run_stage.persist_checkpoint_async",
            return_value=None,
        ):
            state, err, event = await persist_run_stage(
                run_ctx=run_ctx,
                task_id=5,
                run_id=6,
                stage="retrieval",
                where="unit.run_stage.blocked",
            )

        self.assertIsNone(err)
        self.assertEqual(state.get("stage"), "execute")
        self.assertIsNone(event)
        barrier = state.get("stage_barrier") if isinstance(state, dict) else None
        self.assertTrue(isinstance(barrier, dict))
        self.assertEqual(str((barrier or {}).get("from") or ""), "execute")
        self.assertEqual(str((barrier or {}).get("to") or ""), "retrieval")


if __name__ == "__main__":
    unittest.main()
