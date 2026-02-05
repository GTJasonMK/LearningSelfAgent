import os
import tempfile
import unittest
from pathlib import Path


class TestTaskRepositories(unittest.TestCase):
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

    def test_create_task_and_run_without_conn(self):
        from backend.src.constants import RUN_STATUS_RUNNING, STATUS_QUEUED
        from backend.src.repositories.task_runs_repo import create_task_run
        from backend.src.repositories.tasks_repo import create_task
        from backend.src.storage import get_connection

        task_id, created_at = create_task(title="t1", status=STATUS_QUEUED)
        run_id, run_created_at, run_updated_at = create_task_run(
            task_id=task_id,
            status=RUN_STATUS_RUNNING,
            summary="test",
            started_at=created_at,
        )

        self.assertTrue(task_id > 0)
        self.assertTrue(run_id > 0)
        self.assertEqual(run_created_at, run_updated_at)

        with get_connection() as conn:
            task_row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            run_row = conn.execute("SELECT * FROM task_runs WHERE id = ?", (run_id,)).fetchone()

        self.assertEqual(task_row["title"], "t1")
        self.assertEqual(task_row["status"], STATUS_QUEUED)
        self.assertEqual(task_row["created_at"], created_at)

        self.assertEqual(run_row["task_id"], task_id)
        self.assertEqual(run_row["status"], RUN_STATUS_RUNNING)
        self.assertEqual(run_row["started_at"], created_at)

    def test_create_task_and_run_with_shared_conn(self):
        from backend.src.constants import RUN_STATUS_RUNNING, STATUS_RUNNING
        from backend.src.repositories.task_runs_repo import create_task_run
        from backend.src.repositories.tasks_repo import create_task
        from backend.src.storage import get_connection

        with get_connection() as conn:
            task_id, created_at = create_task(
                title="t2",
                status=STATUS_RUNNING,
                started_at="2026-01-01T00:00:00Z",
                conn=conn,
            )
            run_id, _, _ = create_task_run(
                task_id=task_id,
                status=RUN_STATUS_RUNNING,
                summary="shared",
                started_at=created_at,
                conn=conn,
            )

            # 同一连接内应可立即读到写入结果
            task_row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            run_row = conn.execute("SELECT * FROM task_runs WHERE id = ?", (run_id,)).fetchone()

        self.assertEqual(task_row["title"], "t2")
        self.assertEqual(run_row["summary"], "shared")

    def test_create_step_and_output_with_shared_conn(self):
        from backend.src.constants import (
            RUN_STATUS_RUNNING,
            STATUS_RUNNING,
            STEP_STATUS_DONE,
            STEP_STATUS_RUNNING,
            TASK_OUTPUT_TYPE_TEXT,
        )
        from backend.src.repositories.task_outputs_repo import create_task_output
        from backend.src.repositories.task_runs_repo import create_task_run
        from backend.src.repositories.task_steps_repo import TaskStepCreateParams, create_task_step, mark_task_step_done
        from backend.src.repositories.tasks_repo import create_task
        from backend.src.storage import get_connection

        with get_connection() as conn:
            task_id, created_at = create_task(
                title="t3",
                status=STATUS_RUNNING,
                started_at="2026-01-01T00:00:00Z",
                conn=conn,
            )
            run_id, _, _ = create_task_run(
                task_id=task_id,
                status=RUN_STATUS_RUNNING,
                summary="shared",
                started_at=created_at,
                conn=conn,
            )
            step_id, step_created_at, step_updated_at = create_task_step(
                TaskStepCreateParams(
                    task_id=task_id,
                    run_id=run_id,
                    title="s1",
                    status=STEP_STATUS_RUNNING,
                    detail='{"type":"task_output","payload":{"content":"hi"}}',
                    attempts=1,
                    started_at=created_at,
                    step_order=1,
                    created_at=created_at,
                    updated_at=created_at,
                ),
                conn=conn,
            )
            self.assertEqual(step_created_at, created_at)
            self.assertEqual(step_updated_at, created_at)

            output_id, out_created_at = create_task_output(
                task_id=task_id,
                run_id=run_id,
                output_type=TASK_OUTPUT_TYPE_TEXT,
                content="hello",
                created_at=created_at,
                conn=conn,
            )
            self.assertTrue(output_id > 0)
            self.assertEqual(out_created_at, created_at)

            done_at = "2026-01-02T00:00:00Z"
            mark_task_step_done(
                step_id=step_id,
                result='{"ok":1}',
                finished_at=done_at,
                updated_at=done_at,
                conn=conn,
            )

            step_row = conn.execute("SELECT * FROM task_steps WHERE id = ?", (step_id,)).fetchone()
            out_row = conn.execute("SELECT * FROM task_outputs WHERE id = ?", (output_id,)).fetchone()

        self.assertEqual(step_row["status"], STEP_STATUS_DONE)
        self.assertEqual(step_row["finished_at"], done_at)
        self.assertEqual(step_row["result"], '{"ok":1}')
        self.assertEqual(out_row["content"], "hello")

    def test_update_task_run_persists_status_and_state(self):
        import json

        from backend.src.constants import (
            RUN_STATUS_DONE,
            RUN_STATUS_RUNNING,
            RUN_STATUS_STOPPED,
            STATUS_RUNNING,
        )
        from backend.src.repositories.task_runs_repo import create_task_run, update_task_run
        from backend.src.repositories.tasks_repo import create_task
        from backend.src.storage import get_connection

        with get_connection() as conn:
            task_id, created_at = create_task(
                title="t-run",
                status=STATUS_RUNNING,
                started_at="2026-01-01T00:00:00Z",
                conn=conn,
            )
            run_id, _, _ = create_task_run(
                task_id=task_id,
                status=RUN_STATUS_RUNNING,
                summary="agent_test",
                started_at=created_at,
                conn=conn,
            )

            # 1) 写入 agent_plan/agent_state
            plan_obj = {"items": [{"id": 1, "brief": "a", "status": "running"}]}
            state_obj = {"step_order": 1, "observations": ["x"]}
            updated_at = "2026-01-02T00:00:00Z"
            row = update_task_run(
                run_id=run_id,
                agent_plan=plan_obj,
                agent_state=state_obj,
                updated_at=updated_at,
                conn=conn,
            )
            self.assertIsNotNone(row)
            self.assertEqual(row["updated_at"], updated_at)
            self.assertEqual(json.loads(row["agent_plan"])["items"][0]["brief"], "a")
            self.assertEqual(json.loads(row["agent_state"])["step_order"], 1)

            # 2) 终态：done 应写 finished_at
            done_at = "2026-01-03T00:00:00Z"
            row = update_task_run(run_id=run_id, status=RUN_STATUS_DONE, updated_at=done_at, conn=conn)
            self.assertEqual(row["status"], RUN_STATUS_DONE)
            self.assertEqual(row["finished_at"], done_at)

            # 3) 清空 finished_at：模拟 stopped run 继续执行
            stop_at = "2026-01-04T00:00:00Z"
            row = update_task_run(run_id=run_id, status=RUN_STATUS_STOPPED, updated_at=stop_at, conn=conn)
            self.assertEqual(row["status"], RUN_STATUS_STOPPED)
            self.assertEqual(row["finished_at"], stop_at)

            resume_at = "2026-01-05T00:00:00Z"
            row = update_task_run(
                run_id=run_id,
                status=RUN_STATUS_RUNNING,
                clear_finished_at=True,
                updated_at=resume_at,
                conn=conn,
            )
            self.assertEqual(row["status"], RUN_STATUS_RUNNING)
            self.assertIsNone(row["finished_at"])


if __name__ == "__main__":
    unittest.main()
