import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.runner.mode_do_runner import DoExecutionConfig, run_do_mode_execution_from_config


async def _iter_success(*args, **kwargs):
    yield ("msg", "chunk-1")
    yield ("done", SimpleNamespace(run_status="done", last_step_order=3))


async def _iter_error(*args, **kwargs):
    yield ("err", "bad")


class TestModeDoRunner(unittest.IsolatedAsyncioTestCase):
    async def test_run_do_mode_execution_success(self):
        chunks = []
        safe_debug = Mock()
        plan_struct = PlanStructure.from_legacy(
            plan_titles=["a"],
            plan_items=[{"id": 1}],
            plan_allows=[["tool_call"]],
            plan_artifacts=[],
        )
        with patch(
            "backend.src.agent.runner.mode_do_runner.run_react_loop",
            return_value=object(),
        ) as mock_loop, patch(
            "backend.src.agent.runner.mode_do_runner.pump_sync_generator",
            side_effect=_iter_success,
        ) as mock_pump:
            result = await run_do_mode_execution_from_config(
                DoExecutionConfig(
                    task_id=1,
                    run_id=2,
                    message="m",
                    workdir="/tmp",
                    model="gpt",
                    parameters={"temperature": 0.2},
                    plan_struct=plan_struct,
                    tools_hint="(无)",
                    skills_hint="(无)",
                    memories_hint="(无)",
                    graph_hint="(无)",
                    agent_state={},
                    context={},
                    observations=[],
                    start_step_order=1,
                    variables_source="agent_react",
                    yield_func=lambda msg: chunks.append(msg),
                    safe_write_debug=safe_debug,
                )
            )

        self.assertEqual(result.run_status, "done")
        self.assertEqual(result.last_step_order, 3)
        self.assertEqual(chunks, ["chunk-1"])
        mock_loop.assert_called_once()
        mock_pump.assert_called_once()
        safe_debug.assert_called_once()

    async def test_run_do_mode_execution_error_raises(self):
        plan_struct = PlanStructure.from_legacy(
            plan_titles=["a"],
            plan_items=[{"id": 1}],
            plan_allows=[["tool_call"]],
            plan_artifacts=[],
        )
        with patch(
            "backend.src.agent.runner.mode_do_runner.run_react_loop",
            return_value=object(),
        ), patch(
            "backend.src.agent.runner.mode_do_runner.pump_sync_generator",
            side_effect=_iter_error,
        ):
            with self.assertRaises(RuntimeError):
                await run_do_mode_execution_from_config(
                    DoExecutionConfig(
                        task_id=1,
                        run_id=2,
                        message="m",
                        workdir="/tmp",
                        model="gpt",
                        parameters={},
                        plan_struct=plan_struct,
                        tools_hint="(无)",
                        skills_hint="(无)",
                        memories_hint="(无)",
                        graph_hint="(无)",
                        agent_state={},
                        context={},
                        observations=[],
                        start_step_order=1,
                        variables_source="agent_react",
                        yield_func=lambda _msg: None,
                    )
                )


if __name__ == "__main__":
    unittest.main()
