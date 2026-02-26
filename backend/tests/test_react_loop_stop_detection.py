import unittest
from unittest.mock import patch
import sys
import types

if "httpx" not in sys.modules:
    sys.modules["httpx"] = types.ModuleType("httpx")

from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.runner.react_loop_impl import run_react_loop_impl
from backend.src.constants import RUN_STATUS_STOPPED


class TestReactLoopStopDetection(unittest.TestCase):
    def test_loop_stops_immediately_when_run_marked_stopped(self):
        plan_struct = PlanStructure.from_legacy(
            plan_titles=["task_output:输出结果"],
            plan_items=[{"status": "pending"}],
            plan_allows=[["task_output"]],
            plan_artifacts=[],
        )
        execute_called = {"value": False}

        def _exec_step_action(*args, **kwargs):
            _ = args
            _ = kwargs
            execute_called["value"] = True
            return {"ok": True}, None

        gen = run_react_loop_impl(
            task_id=1,
            run_id=1,
            message="test",
            workdir=".",
            model="test-model",
            parameters={},
            plan_struct=plan_struct,
            tools_hint="",
            skills_hint="",
            memories_hint="",
            graph_hint="",
            agent_state={},
            context={},
            observations=[],
            start_step_order=1,
            variables_source="unit_test",
            llm_call=lambda _payload: {"text": ""},
            execute_step_action=_exec_step_action,
        )

        with patch(
            "backend.src.agent.runner.react_loop_impl.get_task_run",
            return_value={"status": "stopped"},
        ):
            try:
                while True:
                    _ = next(gen)
            except StopIteration as stop:
                result = stop.value

        self.assertEqual(result.run_status, RUN_STATUS_STOPPED)
        self.assertFalse(execute_called["value"])


if __name__ == "__main__":
    unittest.main()
