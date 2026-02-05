import os
import tempfile
import unittest


class TestTaskRecoveryTargeted(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_test.db")

        os.environ["AGENT_DB_PATH"] = self._db_path
        os.environ["AGENT_PROMPT_ROOT"] = os.path.join(self._tmpdir.name, "prompt")
        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
        except Exception:
            pass
        self._tmpdir.cleanup()

    def test_stop_task_run_records_only_affects_target_run(self):
        from backend.src.api.utils import now_iso
        from backend.src.services.tasks.task_recovery import stop_task_run_records
        from backend.src.storage import get_connection

        created_at = now_iso()
        with get_connection() as conn:
            # task/run 1
            cur = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("t1", "running", created_at, None, created_at, None),
            )
            task_id_1 = int(cur.lastrowid)
            cur = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (task_id_1, "running", "r1", created_at, None, created_at, created_at),
            )
            run_id_1 = int(cur.lastrowid)
            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, created_at, updated_at, step_order) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (task_id_1, run_id_1, "s1", "running", created_at, created_at, 1),
            )

            # task/run 2（不应被影响）
            cur = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("t2", "running", created_at, None, created_at, None),
            )
            task_id_2 = int(cur.lastrowid)
            cur = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (task_id_2, "running", "r2", created_at, None, created_at, created_at),
            )
            run_id_2 = int(cur.lastrowid)
            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, created_at, updated_at, step_order) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (task_id_2, run_id_2, "s2", "running", created_at, created_at, 1),
            )

        result = stop_task_run_records(task_id=task_id_1, run_id=run_id_1, reason="test")
        self.assertEqual(result["task_id"], task_id_1)
        self.assertEqual(result["run_id"], run_id_1)

        with get_connection() as conn:
            run1 = conn.execute(
                "SELECT status, finished_at FROM task_runs WHERE id = ?",
                (run_id_1,),
            ).fetchone()
            task1 = conn.execute(
                "SELECT status, finished_at FROM tasks WHERE id = ?",
                (task_id_1,),
            ).fetchone()
            step1 = conn.execute(
                "SELECT status FROM task_steps WHERE task_id = ? AND run_id = ?",
                (task_id_1, run_id_1),
            ).fetchone()

            run2 = conn.execute(
                "SELECT status FROM task_runs WHERE id = ?",
                (run_id_2,),
            ).fetchone()
            task2 = conn.execute(
                "SELECT status FROM tasks WHERE id = ?",
                (task_id_2,),
            ).fetchone()
            step2 = conn.execute(
                "SELECT status FROM task_steps WHERE task_id = ? AND run_id = ?",
                (task_id_2, run_id_2),
            ).fetchone()

        self.assertEqual(run1["status"], "stopped")
        self.assertIsNotNone(run1["finished_at"])
        self.assertEqual(task1["status"], "stopped")
        self.assertIsNone(task1["finished_at"])
        self.assertEqual(step1["status"], "planned")

        self.assertEqual(run2["status"], "running")
        self.assertEqual(task2["status"], "running")
        self.assertEqual(step2["status"], "running")


if __name__ == "__main__":
    unittest.main()
