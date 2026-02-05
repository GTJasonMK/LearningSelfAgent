import json
import os
import tempfile
import unittest


class TestTaskExecuteStreamCancelMarksStopped(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_test.db")
        os.environ["AGENT_DB_PATH"] = self._db_path
        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
        except Exception:
            pass
        self._tmpdir.cleanup()

    def test_close_generator_marks_run_and_task_stopped(self):
        """
        回归：当 /tasks/{task_id}/execute/stream 的底层 generator 被提前 close（例如 SSE 客户端断开）时，
        不应把 run/task 错误标记为 done。
        """
        from backend.src.api.utils import now_iso
        from backend.src.storage import get_connection

        from backend.src.api.tasks.routes_task_execute import _execute_task_with_messages
        from backend.src.constants import RUN_STATUS_STOPPED, STATUS_STOPPED, STEP_STATUS_PLANNED

        created_at = now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("测试任务", "queued", created_at, None, None, None),
            )
            task_id = int(cursor.lastrowid)
            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    None,
                    "第一步",
                    STEP_STATUS_PLANNED,
                    json.dumps({"type": "task_output", "payload": {"output_type": "text", "content": "hi"}}, ensure_ascii=False),
                    None,
                    None,
                    None,
                    None,
                    None,
                    1,
                    created_at,
                    created_at,
                ),
            )

        gen = _execute_task_with_messages(task_id)
        first = next(gen)
        self.assertIn("开始执行任务", first)

        # 模拟 SSE 断连：提前关闭 generator
        gen.close()

        with get_connection() as conn:
            run = conn.execute(
                "SELECT id, status FROM task_runs WHERE task_id = ? ORDER BY id DESC LIMIT 1",
                (int(task_id),),
            ).fetchone()
            self.assertIsNotNone(run)
            self.assertEqual(str(run["status"]), RUN_STATUS_STOPPED)

            task = conn.execute("SELECT status FROM tasks WHERE id = ? LIMIT 1", (int(task_id),)).fetchone()
            self.assertIsNotNone(task)
            self.assertEqual(str(task["status"]), STATUS_STOPPED)


if __name__ == "__main__":
    unittest.main()

