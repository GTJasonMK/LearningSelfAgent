import unittest


class TestRunConfigSnapshot(unittest.TestCase):
    def test_apply_run_config_snapshot_if_missing(self):
        from backend.src.agent.runner.run_config_snapshot import apply_run_config_snapshot_if_missing

        state = apply_run_config_snapshot_if_missing(
            agent_state={},
            mode="do",
            requested_model="gpt-test",
            parameters={"temperature": 0},
        )
        snapshot = state.get("config_snapshot") if isinstance(state, dict) else None
        self.assertTrue(isinstance(snapshot, dict))
        self.assertEqual(int(snapshot.get("version") or 0), 1)
        self.assertEqual(str(snapshot.get("mode") or ""), "do")
        llm = snapshot.get("llm") if isinstance(snapshot.get("llm"), dict) else {}
        self.assertEqual(str(llm.get("model") or ""), "gpt-test")
        self.assertTrue(isinstance(snapshot.get("context_budget"), dict))
        pipeline = snapshot.get("context_budget_pipeline") if isinstance(snapshot.get("context_budget_pipeline"), dict) else {}
        self.assertEqual(int(pipeline.get("version") or 0), 1)
        self.assertEqual(list(pipeline.get("stages") or []), ["load", "trim", "compress"])

    def test_apply_run_config_snapshot_keeps_existing(self):
        from backend.src.agent.runner.run_config_snapshot import apply_run_config_snapshot_if_missing

        state = apply_run_config_snapshot_if_missing(
            agent_state={"config_snapshot": {"version": 99, "llm": {"model": "fixed"}}},
            mode="do",
            requested_model="new-model",
            parameters={"temperature": 0.5},
        )
        snapshot = state.get("config_snapshot") if isinstance(state, dict) else None
        self.assertEqual(int((snapshot or {}).get("version") or 0), 99)
        llm = snapshot.get("llm") if isinstance(snapshot.get("llm"), dict) else {}
        self.assertEqual(str(llm.get("model") or ""), "fixed")


if __name__ == "__main__":
    unittest.main()
