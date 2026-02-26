import unittest


class TestTaskQueriesFacadeExports(unittest.TestCase):
    def test_facade_exports_expected_symbols(self):
        from backend.src.services.tasks import task_queries as q

        expected = [
            "TaskStepCreateParams",
            "task_exists",
            "get_task",
            "create_task",
            "count_tasks",
            "list_tasks",
            "update_task",
            "get_task_run",
            "create_task_run",
            "list_task_runs",
            "list_task_runs_for_task",
            "update_task_run",
            "list_task_run_events",
            "get_max_step_order_for_run_by_status",
            "list_task_steps_for_run",
            "create_task_step",
            "mark_task_step_done",
            "mark_task_step_failed",
            "mark_task_step_skipped",
            "list_task_outputs_for_run",
            "create_task_output",
            "get_task_output",
        ]

        missing = [name for name in expected if not hasattr(q, name)]
        self.assertEqual([], missing)


if __name__ == "__main__":
    unittest.main()
