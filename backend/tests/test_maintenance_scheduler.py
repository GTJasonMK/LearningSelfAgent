import json
import os
import shutil
import tempfile
import time
import unittest
from unittest.mock import patch


class _FakeThread:
    created = []

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.started = False
        _FakeThread.created.append(self)

    def start(self):
        self.started = True


class TestMaintenanceScheduler(unittest.TestCase):
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
        # 某些测试会触发后台线程写库（例如 SSE 断连收敛/后处理），可能与 TemporaryDirectory cleanup 产生竞态：
        # sqlite 会短暂创建 journal 文件，导致 rmtree 最后一步 rmdir 抛 “Directory not empty”。
        for _ in range(10):
            try:
                self._tmpdir.cleanup()
                break
            except OSError:
                time.sleep(0.05)
        else:
            shutil.rmtree(self._tmpdir.name, ignore_errors=True)
        _FakeThread.created.clear()

    def test_compute_next_run_is_always_in_future(self):
        from backend.src.api.system import routes_maintenance as mod

        with patch("backend.src.api.system.routes_maintenance.now_iso", return_value="2026-01-31T00:00:00Z"):
            next_run = mod._compute_next_run("2020-01-01T00:00:00Z", 60)
        self.assertEqual(next_run, "2026-01-31T01:00:00Z")

    def test_run_job_async_advances_schedule_before_thread_start(self):
        from backend.src.storage import get_connection
        from backend.src.api.system import routes_maintenance as mod

        created_at = "2026-01-31T00:00:00Z"
        next_run_at = "2026-01-30T00:00:00Z"
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO cleanup_jobs (name, status, mode, tables, retention_days, before, limit_value, last_run_at, next_run_at, created_at, updated_at, interval_minutes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "job",
                    "enabled",
                    "delete",
                    json.dumps(["task_outputs"], ensure_ascii=False),
                    1,
                    None,
                    None,
                    None,
                    next_run_at,
                    created_at,
                    created_at,
                    60,
                ),
            )
            job_id = int(cursor.lastrowid)
            job_row = conn.execute("SELECT * FROM cleanup_jobs WHERE id = ?", (job_id,)).fetchone()

        with (
            patch("backend.src.api.system.routes_maintenance.now_iso", return_value="2026-01-31T00:00:00Z"),
            patch("backend.src.api.system.routes_maintenance.threading.Thread", _FakeThread),
        ):
            mod._run_job_async(job_row)

        with get_connection() as conn:
            row2 = conn.execute("SELECT * FROM cleanup_jobs WHERE id = ?", (job_id,)).fetchone()

        self.assertEqual(row2["last_run_at"], "2026-01-31T00:00:00Z")
        self.assertEqual(row2["next_run_at"], "2026-01-31T01:00:00Z")
        self.assertEqual(len(_FakeThread.created), 1)
        self.assertTrue(_FakeThread.created[0].started)


class TestMaintenanceSchedulerValidation(unittest.TestCase):
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
        for _ in range(10):
            try:
                self._tmpdir.cleanup()
                break
            except OSError:
                time.sleep(0.05)
        else:
            shutil.rmtree(self._tmpdir.name, ignore_errors=True)

    def test_create_cleanup_job_rejects_non_positive_interval(self):
        from backend.src.api.schemas import CleanupJobCreate
        from backend.src.api.system.routes_maintenance import create_cleanup_job

        resp = create_cleanup_job(CleanupJobCreate(name="job", retention_days=1, interval_minutes=0))
        self.assertEqual(resp.status_code, 400)

    def test_update_cleanup_job_rejects_non_positive_interval(self):
        from backend.src.storage import get_connection
        from backend.src.api.schemas import CleanupJobUpdate
        from backend.src.api.system.routes_maintenance import update_cleanup_job

        created_at = "2026-01-31T00:00:00Z"
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO cleanup_jobs (name, status, mode, tables, retention_days, before, limit_value, last_run_at, next_run_at, created_at, updated_at, interval_minutes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "job",
                    "enabled",
                    "delete",
                    json.dumps(["task_outputs"], ensure_ascii=False),
                    1,
                    None,
                    None,
                    None,
                    "2026-01-31T01:00:00Z",
                    created_at,
                    created_at,
                    60,
                ),
            )
            job_id = int(cursor.lastrowid)

        resp = update_cleanup_job(job_id, CleanupJobUpdate(interval_minutes=0))
        self.assertEqual(resp.status_code, 400)


if __name__ == "__main__":
    unittest.main()
