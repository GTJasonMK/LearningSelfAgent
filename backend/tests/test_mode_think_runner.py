import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.runner.mode_think_runner import (
    ThinkExecutionConfig,
    run_think_mode_execution_from_config,
)


async def _iter_parallel_done(*args, **kwargs):
    yield ("msg", "chunk-parallel")
    yield ("done", SimpleNamespace(run_status="done", last_step_order=2))


async def _iter_parallel_failed(*args, **kwargs):
    yield ("done", SimpleNamespace(run_status="failed", last_step_order=1))


class TestModeThinkRunner(unittest.IsolatedAsyncioTestCase):
    def _build_kwargs(self):
        plan_struct = PlanStructure.from_legacy(
            plan_titles=["步骤1"],
            plan_items=[{"id": 1, "title": "步骤1", "status": "pending"}],
            plan_allows=[["tool_call"]],
            plan_artifacts=[],
        )
        return {
            "task_id": 1,
            "run_id": 2,
            "message": "m",
            "workdir": "/tmp",
            "model": "gpt",
            "parameters": {"temperature": 0.2},
            "plan_struct": plan_struct,
            "plan_briefs": ["brief1"],
            "tools_hint": "(无)",
            "skills_hint": "(无)",
            "memories_hint": "(无)",
            "graph_hint": "(无)",
            "agent_state": {},
            "context": {},
            "observations": [],
            "think_config": SimpleNamespace(executors={"executor_code": SimpleNamespace()}),
            "llm_call_func": lambda *_args, **_kwargs: {},
            "step_llm_config_resolver": lambda _order, _title, _allow: ("gpt", {}),
        }

    async def test_run_think_mode_execution_success(self):
        chunks = []
        safe_debug = Mock()
        kwargs = self._build_kwargs()
        with patch(
            "backend.src.agent.runner.mode_think_runner.run_think_parallel_loop",
            return_value=object(),
        ) as mock_parallel, patch(
            "backend.src.agent.runner.mode_think_runner.pump_sync_generator",
            side_effect=_iter_parallel_done,
        ) as mock_pump, patch(
            "backend.src.agent.runner.mode_think_runner.run_react_loop"
        ) as mock_tail:
            result = await run_think_mode_execution_from_config(
                ThinkExecutionConfig(
                    **kwargs,
                    yield_func=lambda msg: chunks.append(msg),
                    safe_write_debug=safe_debug,
                )
            )

        self.assertEqual(result.run_status, "done")
        self.assertEqual(result.last_step_order, 2)
        self.assertEqual(result.reflection_count, 0)
        self.assertIn("chunk-parallel", chunks)
        mock_parallel.assert_called_once()
        mock_pump.assert_called_once()
        mock_tail.assert_not_called()
        safe_debug.assert_called()

    async def test_run_think_mode_execution_reflection_stops_when_no_fix_steps(self):
        chunks = []
        kwargs = self._build_kwargs()
        with patch(
            "backend.src.agent.runner.mode_think_runner.run_think_parallel_loop",
            return_value=object(),
        ), patch(
            "backend.src.agent.runner.mode_think_runner.pump_sync_generator",
            side_effect=_iter_parallel_failed,
        ), patch(
            "backend.src.agent.runner.mode_think_runner.run_reflection",
            return_value=SimpleNamespace(fix_steps=[], winning_analysis=None),
        ) as mock_reflection:
            result = await run_think_mode_execution_from_config(
                ThinkExecutionConfig(
                    **kwargs,
                    yield_func=lambda msg: chunks.append(msg),
                    safe_write_debug=Mock(),
                    persist_reflection_plan_func=AsyncMock(return_value=None),
                )
            )

        self.assertEqual(result.run_status, "failed")
        self.assertEqual(result.reflection_count, 1)
        self.assertTrue(any("反思未能生成修复步骤" in text for text in chunks))
        mock_reflection.assert_called_once()

    async def test_run_think_mode_execution_from_config_delegates(self):
        kwargs = self._build_kwargs()
        fake_result = SimpleNamespace(
            run_status="done",
            last_step_order=1,
            reflection_count=0,
            plan_briefs=[],
            agent_state={},
            plan_struct=PlanStructure(steps=[], artifacts=[]),
        )
        with patch(
            "backend.src.agent.runner.mode_think_runner._run_think_mode_execution_impl",
            AsyncMock(return_value=fake_result),
        ) as mock_run:
            result = await run_think_mode_execution_from_config(
                ThinkExecutionConfig(
                    **kwargs,
                    yield_func=lambda _msg: None,
                )
            )
        self.assertEqual(result.run_status, "done")
        mock_run.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
