import unittest


class TestTaskPostprocessFacadeExports(unittest.TestCase):
    def test_task_postprocess_exports_are_callable(self):
        from backend.src.services.tasks.task_postprocess import (
            backfill_missing_agent_reviews,
            backfill_waiting_feedback_agent_reviews,
            ensure_agent_review_record,
            postprocess_task_run,
            write_task_result_memory_if_missing,
        )

        self.assertTrue(callable(ensure_agent_review_record))
        self.assertTrue(callable(backfill_missing_agent_reviews))
        self.assertTrue(callable(backfill_waiting_feedback_agent_reviews))
        self.assertTrue(callable(write_task_result_memory_if_missing))
        self.assertTrue(callable(postprocess_task_run))

    def test_postprocess_package_exports_run_finalize_core(self):
        from backend.src.services.tasks.postprocess import postprocess_task_run_core

        self.assertTrue(callable(postprocess_task_run_core))


if __name__ == "__main__":
    unittest.main()
