import unittest
import sys
import types
from unittest.mock import patch


def _install_httpx_stub() -> None:
    if "httpx" in sys.modules:
        return

    module = types.ModuleType("httpx")

    class _Client:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    module.Client = _Client
    sys.modules["httpx"] = module


_install_httpx_stub()

from backend.src.agent.core.plan_structure import PlanStructure


class TestReplanPreserveFeedbackStep(unittest.TestCase):
    def test_replan_restores_feedback_step_when_original_plan_has_it(self):
        from backend.src.agent.planning_phase import PlanPhaseResult
        from backend.src.agent.runner.react_replan import run_replan_and_merge
        from backend.src.constants import AGENT_TASK_FEEDBACK_STEP_TITLE, ACTION_TYPE_USER_PROMPT

        plan_struct = PlanStructure.from_legacy(
            plan_titles=["task_output: 输出最终结果", AGENT_TASK_FEEDBACK_STEP_TITLE],
            plan_items=[
                {"id": 1, "brief": "输出结果", "status": "done"},
                {"id": 2, "brief": "反馈", "status": "pending"},
            ],
            plan_allows=[["task_output"], [ACTION_TYPE_USER_PROMPT]],
            plan_artifacts=[],
        )

        debug_messages = []

        def _safe_write_debug(**kwargs):
            debug_messages.append(str(kwargs.get("message") or ""))

        def _fake_run_replan_phase(**kwargs):
            _ = kwargs
            if False:
                yield ""
            return PlanPhaseResult(
                plan_titles=["file_write:backend/.agent/workspace/final.txt"],
                plan_briefs=["写入结果"],
                plan_allows=[["file_write"]],
                plan_artifacts=["backend/.agent/workspace/final.txt"],
                plan_items=[{"id": 1, "brief": "写入结果", "status": "pending"}],
                plan_llm_id=1,
            )

        with patch(
            "backend.src.agent.runner.react_replan.run_replan_phase",
            side_effect=_fake_run_replan_phase,
        ), patch(
            "backend.src.agent.runner.react_replan.update_task_run",
            return_value=None,
        ):
            gen = run_replan_and_merge(
                task_id=1,
                run_id=1,
                message="m",
                workdir="/tmp",
                model="deepseek-chat",
                react_params={"temperature": 0},
                max_steps_value=20,
                tools_hint="(无)",
                skills_hint="(无)",
                solutions_hint="(无)",
                memories_hint="(无)",
                graph_hint="(无)",
                plan_struct=plan_struct,
                agent_state={"task_feedback_asked": False},
                observations=[],
                done_count=1,
                error="",
                sse_notice="",
                replan_attempts=0,
                safe_write_debug=_safe_write_debug,
                extra_observations=None,
            )
            try:
                while True:
                    next(gen)
            except StopIteration as exc:
                result = exc.value

        self.assertIsNotNone(result)
        result_titles = [s.title for s in result.plan_struct.steps]
        result_allows = [s.allow for s in result.plan_struct.steps]
        self.assertEqual(result_titles[-1], AGENT_TASK_FEEDBACK_STEP_TITLE)
        self.assertEqual(result_allows[-1], [ACTION_TYPE_USER_PROMPT])
        self.assertIn("agent.replan.feedback_step_restored", debug_messages)

    def test_replan_dedups_feedback_steps_to_single_tail(self):
        from backend.src.agent.planning_phase import PlanPhaseResult
        from backend.src.agent.runner.react_replan import run_replan_and_merge
        from backend.src.constants import AGENT_TASK_FEEDBACK_STEP_TITLE, ACTION_TYPE_USER_PROMPT

        plan_struct = PlanStructure.from_legacy(
            plan_titles=["task_output: 输出最终结果", AGENT_TASK_FEEDBACK_STEP_TITLE],
            plan_items=[
                {"id": 1, "brief": "输出结果", "status": "done"},
                {"id": 2, "brief": "反馈", "status": "pending"},
            ],
            plan_allows=[["task_output"], [ACTION_TYPE_USER_PROMPT]],
            plan_artifacts=[],
        )

        debug_messages = []

        def _safe_write_debug(**kwargs):
            debug_messages.append(str(kwargs.get("message") or ""))

        def _fake_run_replan_phase(**kwargs):
            _ = kwargs
            if False:
                yield ""
            return PlanPhaseResult(
                plan_titles=[
                    "file_write:backend/.agent/workspace/final.txt",
                    AGENT_TASK_FEEDBACK_STEP_TITLE,
                    AGENT_TASK_FEEDBACK_STEP_TITLE,
                ],
                plan_briefs=["写入结果", "反馈", "反馈"],
                plan_allows=[["file_write"], [ACTION_TYPE_USER_PROMPT], [ACTION_TYPE_USER_PROMPT]],
                plan_artifacts=["backend/.agent/workspace/final.txt"],
                plan_items=[
                    {"id": 1, "brief": "写入结果", "status": "pending"},
                    {"id": 2, "brief": "反馈", "status": "pending"},
                    {"id": 3, "brief": "反馈", "status": "pending"},
                ],
                plan_llm_id=1,
            )

        with patch(
            "backend.src.agent.runner.react_replan.run_replan_phase",
            side_effect=_fake_run_replan_phase,
        ), patch(
            "backend.src.agent.runner.react_replan.update_task_run",
            return_value=None,
        ):
            gen = run_replan_and_merge(
                task_id=1,
                run_id=1,
                message="m",
                workdir="/tmp",
                model="deepseek-chat",
                react_params={"temperature": 0},
                max_steps_value=20,
                tools_hint="(无)",
                skills_hint="(无)",
                solutions_hint="(无)",
                memories_hint="(无)",
                graph_hint="(无)",
                plan_struct=plan_struct,
                agent_state={"task_feedback_asked": False},
                observations=[],
                done_count=1,
                error="",
                sse_notice="",
                replan_attempts=0,
                safe_write_debug=_safe_write_debug,
                extra_observations=None,
            )
            try:
                while True:
                    next(gen)
            except StopIteration as exc:
                result = exc.value

        self.assertIsNotNone(result)
        result_titles = [s.title for s in result.plan_struct.steps]
        feedback_count = sum(1 for title in result_titles if title == AGENT_TASK_FEEDBACK_STEP_TITLE)
        self.assertEqual(feedback_count, 1)
        self.assertEqual(result_titles[-1], AGENT_TASK_FEEDBACK_STEP_TITLE)
        self.assertIn("agent.replan.feedback_step_dedup", debug_messages)


if __name__ == "__main__":
    unittest.main()
