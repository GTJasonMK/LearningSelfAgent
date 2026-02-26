import unittest


class TestTaskPostprocessReviewModuleExports(unittest.TestCase):
    def test_review_related_modules_export_callables(self):
        from backend.src.services.tasks.postprocess.review import ensure_agent_review_record_core
        from backend.src.services.tasks.postprocess.review_data import collect_review_data
        from backend.src.services.tasks.postprocess.review_decision import evaluate_review_decision
        from backend.src.services.tasks.postprocess.review_prompt import (
            build_review_prompt_text,
            resolve_review_model,
        )
        from backend.src.services.tasks.postprocess.review_tool_approval import (
            approve_tools_after_review,
            ensure_existing_review_tool_approval,
        )

        self.assertTrue(callable(ensure_agent_review_record_core))
        self.assertTrue(callable(collect_review_data))
        self.assertTrue(callable(evaluate_review_decision))
        self.assertTrue(callable(build_review_prompt_text))
        self.assertTrue(callable(resolve_review_model))
        self.assertTrue(callable(ensure_existing_review_tool_approval))
        self.assertTrue(callable(approve_tools_after_review))


if __name__ == "__main__":
    unittest.main()
