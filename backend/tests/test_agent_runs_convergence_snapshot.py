import unittest

import importlib.util


HAS_FASTAPI = importlib.util.find_spec("fastapi") is not None


class TestAgentRunsConvergenceSnapshot(unittest.TestCase):
    def setUp(self):
        if not HAS_FASTAPI:
            self.skipTest("fastapi 未安装，跳过 routes_agent_runs 的快照测试")

    def test_compute_convergence_snapshot_extracts_expected_fields(self):
        from backend.src.api.agent.routes_agent_runs import _compute_convergence_snapshot

        snapshot = _compute_convergence_snapshot(
            agent_state={
                "progress_score": 77,
                "no_progress_streak": 1,
                "strategy_fingerprint": "fp_001",
                "attempt_index": "4",
                "last_failure_class": "llm_rate_limit",
                "unreachable_proof": {"proof_id": "proof_001"},
            }
        )

        self.assertEqual(int(snapshot.get("progress_score") or 0), 77)
        self.assertEqual(int(snapshot.get("no_progress_streak") or 0), 1)
        self.assertEqual(str(snapshot.get("strategy_fingerprint") or ""), "fp_001")
        self.assertEqual(int(snapshot.get("attempt_index") or 0), 4)
        self.assertEqual(str(snapshot.get("last_failure_class") or ""), "llm_rate_limit")
        self.assertEqual(str(snapshot.get("proof_id") or ""), "proof_001")

    def test_compute_convergence_snapshot_handles_empty_state(self):
        from backend.src.api.agent.routes_agent_runs import _compute_convergence_snapshot

        snapshot = _compute_convergence_snapshot(agent_state=None)
        self.assertIsNone(snapshot.get("progress_score"))
        self.assertIsNone(snapshot.get("no_progress_streak"))
        self.assertIsNone(snapshot.get("strategy_fingerprint"))
        self.assertIsNone(snapshot.get("attempt_index"))
        self.assertIsNone(snapshot.get("last_failure_class"))
        self.assertIsNone(snapshot.get("proof_id"))


if __name__ == "__main__":
    unittest.main()
