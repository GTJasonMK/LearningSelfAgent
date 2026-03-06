import unittest

from backend.src.agent.runner.react_error_handler import (
    _record_failure_signature,
    clear_failure_streak,
)


class TestReactFailureStreak(unittest.TestCase):
    def test_clear_failure_streak_resets_consecutive_budget_counter(self):
        state = {}

        _sig1, count1 = _record_failure_signature(
            agent_state=state,
            action_type="action_invalid",
            step_error="LLM call timeout after 30s",
        )
        _sig2, count2 = _record_failure_signature(
            agent_state=state,
            action_type="action_invalid",
            step_error="LLM call timeout after 30s",
        )
        self.assertEqual(int(count1), 1)
        self.assertEqual(int(count2), 2)

        clear_failure_streak(state)

        _sig3, count3 = _record_failure_signature(
            agent_state=state,
            action_type="action_invalid",
            step_error="LLM call timeout after 30s",
        )
        self.assertEqual(int(count3), 1)
        # failure_signatures 仍保留全局统计用于诊断，不受 streak 清理影响。
        stats = state.get("failure_signatures") if isinstance(state.get("failure_signatures"), dict) else {}
        self.assertTrue(stats)
        self.assertEqual(int((next(iter(stats.values())) or {}).get("count") or 0), 3)


if __name__ == "__main__":
    unittest.main()
