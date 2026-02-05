import os
import tempfile
import unittest
from pathlib import Path


class TestTaskRecovery(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp.name) / "agent_test.db"
        os.environ["AGENT_DB_PATH"] = str(db_path)
        os.environ["AGENT_PROMPT_ROOT"] = str(Path(self._tmp.name) / "prompt")

        import backend.src.storage as storage
        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
            self._tmp.cleanup()
        except Exception:
            pass

    def test_stop_running_task_records_marks_running_as_stopped(self):
        from datetime import datetime, timezone
        from backend.src.constants import (
            RUN_STATUS_RUNNING,
            RUN_STATUS_STOPPED,
            STATUS_RUNNING,
            STATUS_STOPPED,
            STEP_STATUS_PLANNED,
            STEP_STATUS_RUNNING,
        )
        from backend.src.services.tasks.task_recovery import stop_running_task_records
        from backend.src.storage import get_connection

        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("test", STATUS_RUNNING, created_at, None, created_at, None),
            )
            task_id = cursor.lastrowid
            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (task_id, RUN_STATUS_RUNNING, "run", created_at, None, created_at, created_at),
            )
            run_id = cursor.lastrowid
            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_id,
                    "step1",
                    STEP_STATUS_RUNNING,
                    "{\"type\":\"task_output\",\"payload\":{\"output_type\":\"text\",\"content\":\"hi\"}}",
                    None,
                    None,
                    0,
                    created_at,
                    None,
                    1,
                    created_at,
                    created_at,
                ),
            )

        result = stop_running_task_records(reason="test")
        self.assertEqual(result["stopped_runs"], 1)
        self.assertEqual(result["stopped_tasks"], 1)
        self.assertEqual(result["reset_steps"], 1)

        with get_connection() as conn:
            run_row = conn.execute("SELECT * FROM task_runs WHERE id = ?", (run_id,)).fetchone()
            task_row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            step_row = conn.execute(
                "SELECT * FROM task_steps WHERE task_id = ? ORDER BY id ASC LIMIT 1",
                (task_id,),
            ).fetchone()

        self.assertEqual(run_row["status"], RUN_STATUS_STOPPED)
        self.assertIsNotNone(run_row["finished_at"])
        self.assertEqual(task_row["status"], STATUS_STOPPED)
        self.assertIsNone(task_row["finished_at"])
        self.assertEqual(step_row["status"], STEP_STATUS_PLANNED)


if __name__ == "__main__":
    unittest.main()
