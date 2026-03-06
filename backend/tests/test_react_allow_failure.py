import unittest

from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.runner.react_error_handler import handle_allow_failure
from backend.src.constants import RUN_STATUS_FAILED


def _drain_generator(gen):
    chunks = []
    try:
        while True:
            chunks.append(next(gen))
    except StopIteration as stop:
        return chunks, stop.value


class TestReactAllowFailure(unittest.TestCase):
    def test_file_write_allow_failure_replan_failed_should_not_skip_next_step(self):
        plan_struct = PlanStructure.from_agent_plan_payload(
            {
                "titles": ["file_write:backend/.agent/workspace/parse_web_table.py", "step2"],
                "items": [
                    {"allow": ["file_write"], "status": "running"},
                    {"allow": ["task_output"], "status": "pending"},
                ],
            }
        )

        replan_calls = {"n": 0}

        def _replan_none(**kwargs):
            replan_calls["n"] += 1
            if False:
                yield kwargs
            return None

        def _noop(**kwargs):
            return None

        gen = handle_allow_failure(
            task_id=1,
            run_id=1,
            step_order=1,
            idx=0,
            title="file_write:backend/.agent/workspace/parse_web_table.py",
            message="测试",
            workdir=".",
            model="gpt",
            react_params={},
            tools_hint="",
            skills_hint="",
            memories_hint="",
            graph_hint="",
            allow_err="action.type 不在 allow 内",
            plan_struct=plan_struct,
            agent_state={},
            context={},
            observations=[],
            max_steps_limit=10,
            run_replan_and_merge=_replan_none,
            safe_write_debug=_noop,
        )

        chunks, ret = _drain_generator(gen)
        self.assertEqual(replan_calls["n"], 1)
        self.assertEqual(ret, (RUN_STATUS_FAILED, None))
        self.assertTrue(
            any("关键产物步骤 allow 约束失败且重规划未恢复" in str(chunk) for chunk in chunks),
            "file_write 产物步骤的 allow 失败在 replan 无效时应直接终止",
        )


if __name__ == "__main__":
    unittest.main()
