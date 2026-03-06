import unittest

from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.runner.attempt_controller import (
    build_unreachable_proof_event,
    ensure_strategy_state,
    rotate_strategy,
    update_progress_state,
)


def _build_plan_struct() -> PlanStructure:
    return PlanStructure.from_agent_plan_payload(
        {
            "titles": ["http_request: 拉取数据", "task_output: 输出结果"],
            "items": [
                {"title": "http_request: 拉取数据", "status": "pending", "allow": ["http_request"]},
                {"title": "task_output: 输出结果", "status": "pending", "allow": ["task_output"]},
            ],
            "allows": [["http_request"], ["task_output"]],
        }
    )


class TestAttemptController(unittest.TestCase):
    def test_ensure_strategy_state_initializes_history(self):
        plan_struct = _build_plan_struct()
        agent_state = {}

        event = ensure_strategy_state(agent_state=agent_state, plan_struct=plan_struct, reason="run_start")

        self.assertEqual(str(event.get("type") or ""), "strategy_update")
        self.assertEqual(int(event.get("attempt_index") or 0), 1)
        self.assertFalse(bool(event.get("switched")))
        self.assertTrue(str(event.get("strategy_fingerprint") or ""))
        self.assertEqual(int(agent_state.get("attempt_index") or 0), 1)
        self.assertEqual(len(agent_state.get("strategy_history") or []), 1)

    def test_rotate_strategy_switches_fingerprint_and_attempt(self):
        plan_struct = _build_plan_struct()
        agent_state = {}
        first = ensure_strategy_state(agent_state=agent_state, plan_struct=plan_struct, reason="run_start")
        second = rotate_strategy(
            agent_state=agent_state,
            plan_struct=plan_struct,
            reason="retry_replan",
            failure_class="source_unavailable",
        )

        self.assertTrue(bool(second.get("switched")))
        self.assertEqual(str(second.get("failure_class") or ""), "source_unavailable")
        self.assertNotEqual(
            str(second.get("strategy_fingerprint") or ""),
            str(first.get("strategy_fingerprint") or ""),
        )
        self.assertEqual(int(second.get("attempt_index") or 0), 2)
        self.assertEqual(int(agent_state.get("attempt_index") or 0), 2)

    def test_update_progress_state_tracks_no_progress_streak(self):
        plan_struct = _build_plan_struct()
        agent_state = {}
        ensure_strategy_state(agent_state=agent_state, plan_struct=plan_struct, reason="run_start")

        first = update_progress_state(
            agent_state=agent_state,
            plan_struct=plan_struct,
            context={},
            reason="run_start",
            step_order=1,
        )
        second = update_progress_state(
            agent_state=agent_state,
            plan_struct=plan_struct,
            context={},
            reason="same_round",
            step_order=1,
        )
        third = update_progress_state(
            agent_state=agent_state,
            plan_struct=plan_struct,
            context={"latest_external_url": "https://example.com/data"},
            reason="artifact_found",
            step_order=1,
        )

        self.assertEqual(str(first.get("type") or ""), "progress_update")
        self.assertEqual(int(second.get("no_progress_streak") or 0), 1)
        self.assertTrue(bool(third.get("improved")))
        self.assertEqual(int(third.get("no_progress_streak") or 0), 0)

    def test_build_unreachable_proof_event_persists_to_state(self):
        plan_struct = _build_plan_struct()
        agent_state = {}
        ensure_strategy_state(agent_state=agent_state, plan_struct=plan_struct, reason="run_start")
        rotate_strategy(
            agent_state=agent_state,
            plan_struct=plan_struct,
            reason="retry_replan",
            failure_class="execution_error",
        )
        agent_state["failure_signatures"] = {
            "http_request|code=source_http_timeout": {"count": 3},
            "json_parse|msg:invalid": {"count": 1},
        }
        agent_state["no_progress_streak"] = 4

        proof = build_unreachable_proof_event(
            agent_state=agent_state,
            task_id=11,
            run_id=22,
            reason="repeat_failure_budget_exceeded",
            failure_class="source_unavailable",
            error_message="上游超时",
        )

        self.assertEqual(str(proof.get("type") or ""), "unreachable_proof")
        self.assertEqual(int(proof.get("task_id") or 0), 11)
        self.assertEqual(int(proof.get("run_id") or 0), 22)
        self.assertTrue(str(proof.get("proof_id") or ""))
        self.assertEqual(str(proof.get("failure_class") or ""), "source_unavailable")
        self.assertEqual(int(proof.get("strategy_attempts") or 0), 2)
        self.assertEqual(int(proof.get("no_progress_streak") or 0), 4)
        self.assertEqual(str((proof.get("recent_failures") or [{}])[0].get("signature") or ""), "http_request|code=source_http_timeout")
        self.assertIsInstance(agent_state.get("unreachable_proof"), dict)


if __name__ == "__main__":
    unittest.main()
