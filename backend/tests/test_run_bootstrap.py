import unittest
from unittest.mock import AsyncMock, patch

from backend.src.agent.runner.run_bootstrap import bootstrap_new_mode_run


class TestRunBootstrap(unittest.IsolatedAsyncioTestCase):
    async def test_bootstrap_new_mode_run_returns_events_and_context(self):
        with patch(
            "backend.src.agent.runner.run_bootstrap.create_task_and_run_records_for_agent",
            return_value=(11, 22),
        ) as mock_create, patch(
            "backend.src.agent.runner.run_bootstrap.persist_run_stage",
            new_callable=AsyncMock,
            return_value=({"mode": "think"}, None, "stage-event"),
        ) as mock_stage:
            boot = await bootstrap_new_mode_run(
                message="hello",
                mode="think",
                model="m1",
                parameters={"temperature": 0.2},
                max_steps=8,
                workdir="/tmp/wd",
                stage_where_prefix="think_run",
                state_overrides={"think_config": {"x": 1}},
                tools_hint="(无)",
                skills_hint="(无)",
                solutions_hint="(无)",
                memories_hint="(无)",
                graph_hint="",
            )

        self.assertEqual(boot.task_id, 11)
        self.assertEqual(boot.run_id, 22)
        self.assertEqual(boot.stage_event, "stage-event")
        self.assertIn("run_created", boot.run_created_event)
        self.assertEqual(boot.run_ctx.state.get("mode"), "think")
        self.assertEqual(boot.run_ctx.state.get("think_config"), {"x": 1})
        self.assertEqual(boot.run_ctx.state.get("max_steps"), 8)
        mock_create.assert_called_once()
        stage_kwargs = mock_stage.call_args.kwargs
        self.assertEqual(stage_kwargs.get("run_id"), 22)
        self.assertEqual(stage_kwargs.get("stage"), "retrieval")
        self.assertEqual(stage_kwargs.get("where"), "think_run.stage.retrieval")


if __name__ == "__main__":
    unittest.main()
