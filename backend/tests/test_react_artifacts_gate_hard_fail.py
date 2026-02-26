import unittest
import sys
import types

if "httpx" not in sys.modules:
    sys.modules["httpx"] = types.ModuleType("httpx")

from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.runner.react_artifacts_gate import apply_artifacts_gates
from backend.src.constants import RUN_STATUS_FAILED


def _run_replan_stub(**kwargs):
    _ = kwargs
    if False:
        yield ""
    return None


class TestReactArtifactsGateHardFail(unittest.TestCase):
    def _collect_outcome(self, gen):
        chunks = []
        try:
            while True:
                chunks.append(next(gen))
        except StopIteration as stop:
            return chunks, stop.value

    def test_prior_failed_steps_before_task_output_allows_with_risk_when_cannot_replan(self):
        plan_struct = PlanStructure.from_legacy(
            plan_titles=["shell_command:执行", "task_output:输出结果"],
            plan_items=[{"status": "failed"}, {"status": "pending"}],
            plan_allows=[["shell_command"], ["task_output"]],
            plan_artifacts=[],
        )
        agent_state = {"replan_attempts": 999}

        gen = apply_artifacts_gates(
            task_id=1,
            run_id=1,
            idx=1,
            step_order=2,
            title="task_output:输出结果",
            workdir=".",
            message="test",
            model="test-model",
            react_params={},
            tools_hint="",
            skills_hint="",
            memories_hint="",
            graph_hint="",
            allowed=["task_output"],
            plan_struct=plan_struct,
            agent_state=agent_state,
            observations=[],
            max_steps_limit=None,
            run_replan_and_merge=_run_replan_stub,
            safe_write_debug=lambda **kwargs: None,
        )
        chunks, outcome = self._collect_outcome(gen)

        self.assertIsNone(outcome.run_status)
        self.assertTrue(any("已记录风险并继续执行最终输出" in str(item) for item in chunks))
        self.assertEqual(agent_state.get("prior_failed_steps_before_output"), [1])
        self.assertTrue(bool(agent_state.get("output_risk_has_failed_steps")))

    def test_missing_artifacts_before_task_output_hard_fail(self):
        plan_struct = PlanStructure.from_legacy(
            plan_titles=["task_output:输出结果"],
            plan_items=[{"status": "pending"}],
            plan_allows=[["task_output"]],
            plan_artifacts=["artifacts/out.csv"],
        )

        gen = apply_artifacts_gates(
            task_id=1,
            run_id=1,
            idx=0,
            step_order=1,
            title="task_output:输出结果",
            workdir=".",
            message="test",
            model="test-model",
            react_params={},
            tools_hint="",
            skills_hint="",
            memories_hint="",
            graph_hint="",
            allowed=["task_output"],
            plan_struct=plan_struct,
            agent_state={},
            observations=[],
            max_steps_limit=None,
            run_replan_and_merge=_run_replan_stub,
            safe_write_debug=lambda **kwargs: None,
        )
        chunks, outcome = self._collect_outcome(gen)

        self.assertEqual(outcome.run_status, RUN_STATUS_FAILED)
        self.assertTrue(any("缺少必需产物" in str(item) for item in chunks))


if __name__ == "__main__":
    unittest.main()
