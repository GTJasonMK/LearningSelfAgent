import unittest


class TestContextBudget(unittest.TestCase):
    def test_trim_text_for_budget_keeps_tail_notice(self):
        from backend.src.agent.core.context_budget import trim_text_for_budget

        raw = "x" * 200
        out = trim_text_for_budget(raw, 60)
        self.assertTrue(len(out) <= 80)
        self.assertIn("已截断", out)

    def test_apply_context_budgets_only_trims_known_sections(self):
        from backend.src.agent.core.context_budget import apply_context_budgets

        result = apply_context_budgets(
            {
                "tools": "a" * 2000,
                "custom": "b" * 2000,
            },
            budgets={"tools": 80},
        )
        self.assertIn("已截断", str(result.get("tools") or ""))
        self.assertEqual(len(str(result.get("custom") or "")), 2000)

    def test_apply_context_budget_pipeline_returns_meta(self):
        from backend.src.agent.core.context_budget import apply_context_budget_pipeline

        out, meta = apply_context_budget_pipeline(
            {"observations": "x" * 4000, "custom": "ok"},
            budgets={"observations": 120},
        )
        self.assertTrue(isinstance(out, dict))
        self.assertTrue(isinstance(meta, dict))
        self.assertEqual(int(meta.get("version") or 0), 1)
        self.assertEqual(list(meta.get("stages") or []), ["load", "trim", "compress"])
        self.assertIn("observations", out)
        self.assertIn("observations", meta.get("raw_lengths") or {})


if __name__ == "__main__":
    unittest.main()
