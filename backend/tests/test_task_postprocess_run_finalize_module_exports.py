import unittest


class TestTaskPostprocessRunFinalizeModuleExports(unittest.TestCase):
    def test_run_finalize_related_modules_export_callables(self):
        from backend.src.services.tasks.postprocess.run_distill_actions import (
            autogen_skills_response,
            autogen_solution_if_allowed,
            collect_graph_update_if_allowed,
            sync_draft_skill_status,
            sync_review_skills,
        )
        from backend.src.services.tasks.postprocess.run_eval import create_eval_response
        from backend.src.services.tasks.postprocess.run_finalize import postprocess_task_run_core
        from backend.src.services.tasks.postprocess.run_gate import resolve_distill_gate
        from backend.src.services.tasks.postprocess.run_memory import write_task_result_memory_safe

        self.assertTrue(callable(create_eval_response))
        self.assertTrue(callable(resolve_distill_gate))
        self.assertTrue(callable(sync_draft_skill_status))
        self.assertTrue(callable(collect_graph_update_if_allowed))
        self.assertTrue(callable(autogen_solution_if_allowed))
        self.assertTrue(callable(autogen_skills_response))
        self.assertTrue(callable(write_task_result_memory_safe))
        self.assertTrue(callable(sync_review_skills))
        self.assertTrue(callable(postprocess_task_run_core))


if __name__ == "__main__":
    unittest.main()
