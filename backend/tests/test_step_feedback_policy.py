import unittest

from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.runner.attempt_controller import ensure_strategy_state, update_progress_state
from backend.src.agent.runner.retry_policy import maybe_enforce_retry_change
from backend.src.agent.runner.step_feedback import (
    build_step_feedback,
    register_step_feedback,
    summarize_recent_step_feedback_for_prompt,
    summarize_retry_requirements_for_prompt,
)


def _build_plan_struct() -> PlanStructure:
    return PlanStructure.from_agent_plan_payload(
        {
            "titles": ["tool_call:web_fetch 搜索黄金价格", "task_output: 输出 csv"],
            "items": [
                {"title": "tool_call:web_fetch 搜索黄金价格", "status": "pending", "allow": ["tool_call"]},
                {"title": "task_output: 输出 csv", "status": "pending", "allow": ["task_output"]},
            ],
            "allows": [["tool_call"], ["task_output"]],
        }
    )


class TestStepFeedbackPolicy(unittest.TestCase):
    def test_register_failed_feedback_creates_retry_requirements(self):
        state = {"strategy_fingerprint": "fp_a", "attempt_index": 1}
        feedback = build_step_feedback(
            message="请你帮我收集最近三个月的黄金价格数据，单位元/克，并保存为csv文件",
            step_order=1,
            title="tool_call:web_fetch 搜索黄金价格",
            action_type="tool_call",
            status="failed",
            error_message="source_http_timeout",
            failure_class="source_unavailable",
            failure_signature="tool_call|code:source_http_timeout",
            context={},
            strategy_fingerprint="fp_a",
            attempt_index=1,
            previous_goal_progress_score=0,
        )
        register_step_feedback(state, feedback)

        pending = state.get("pending_retry_requirements")
        self.assertIsInstance(pending, dict)
        self.assertTrue(bool(pending.get("active")))
        self.assertIn("source_selection", pending.get("must_change") or [])
        self.assertIn("query_strategy", pending.get("must_change") or [])
        summary = summarize_retry_requirements_for_prompt(state)
        self.assertIn("failure_class=source_unavailable", summary)

    def test_maybe_enforce_retry_change_rotates_strategy(self):
        plan_struct = _build_plan_struct()
        state = {}
        first = ensure_strategy_state(agent_state=state, plan_struct=plan_struct, reason="run_start")
        state["pending_retry_requirements"] = {
            "active": True,
            "failure_class": "source_unavailable",
            "blocked_strategy_fingerprint": str(first.get("strategy_fingerprint") or ""),
            "must_change": ["source_selection"],
            "retry_constraints": ["禁止继续使用同一外部源。"],
            "enforcement_count": 0,
        }

        event = maybe_enforce_retry_change(agent_state=state, plan_struct=plan_struct, step_order=2)

        self.assertIsInstance(event, dict)
        self.assertTrue(bool(event.get("switched")))
        self.assertNotEqual(str(event.get("strategy_fingerprint") or ""), str(first.get("strategy_fingerprint") or ""))
        pending = state.get("pending_retry_requirements")
        self.assertEqual(int(pending.get("enforcement_count") or 0), 1)

    def test_progress_update_uses_goal_progress_score(self):
        plan_struct = _build_plan_struct()
        state = {}
        ensure_strategy_state(agent_state=state, plan_struct=plan_struct, reason="run_start")
        state["goal_progress"] = {"state": "none", "score": 0}
        first = update_progress_state(agent_state=state, plan_struct=plan_struct, context={}, reason="start", step_order=1)
        state["goal_progress"] = {"state": "strong", "score": 80}
        second = update_progress_state(agent_state=state, plan_struct=plan_struct, context={}, reason="improved", step_order=1)

        self.assertEqual(str(first.get("goal_progress") or ""), "none")
        self.assertEqual(int((first.get("metrics") or {}).get("goal_progress_score") or 0), 0)
        self.assertEqual(str(second.get("goal_progress") or ""), "strong")
        self.assertTrue(bool(second.get("improved")))
        self.assertGreater(int(second.get("score") or 0), int(first.get("score") or 0))

    def test_prompt_summaries_include_recent_feedback(self):
        state = {"strategy_fingerprint": "fp_a", "attempt_index": 1}
        feedback = build_step_feedback(
            message="请抓取黄金价格并保存 csv",
            step_order=1,
            title="tool_call:web_fetch 搜索黄金价格",
            action_type="tool_call",
            status="failed",
            error_message="source_http_timeout",
            failure_class="source_unavailable",
            failure_signature="tool_call|code:source_http_timeout",
            context={},
            strategy_fingerprint="fp_a",
            attempt_index=1,
            previous_goal_progress_score=0,
        )
        register_step_feedback(state, feedback)

        summary = summarize_recent_step_feedback_for_prompt(state)
        self.assertIn("step#1", summary)
        self.assertIn("source_unavailable", summary)


if __name__ == "__main__":
    unittest.main()
