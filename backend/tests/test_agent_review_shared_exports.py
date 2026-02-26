import unittest


class TestAgentReviewSharedExports(unittest.TestCase):
    def test_shared_review_modules_export_callables(self):
        from backend.src.services.agent_review.review_decision import evaluate_review_decision
        from backend.src.services.agent_review.review_prompt import (
            build_review_prompt_text,
            resolve_review_model,
        )
        from backend.src.services.agent_review.review_snapshot import (
            build_artifacts_check,
            build_run_meta,
            compact_outputs_for_review,
            compact_steps_for_review,
            compact_tools_for_review,
        )

        self.assertTrue(callable(evaluate_review_decision))
        self.assertTrue(callable(build_review_prompt_text))
        self.assertTrue(callable(resolve_review_model))
        self.assertTrue(callable(build_artifacts_check))
        self.assertTrue(callable(build_run_meta))
        self.assertTrue(callable(compact_steps_for_review))
        self.assertTrue(callable(compact_outputs_for_review))
        self.assertTrue(callable(compact_tools_for_review))


if __name__ == "__main__":
    unittest.main()
