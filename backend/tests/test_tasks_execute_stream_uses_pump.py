import os
import tempfile
import unittest
from unittest.mock import patch

try:
    import httpx  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    httpx = None


async def _fake_pump_sync_generator(*, inner, label, poll_interval_seconds, idle_timeout_seconds):
    _ = inner
    _ = label
    _ = poll_interval_seconds
    _ = idle_timeout_seconds
    yield ("msg", "hello")
    yield ("done", {})


class TestTasksExecuteStreamUsesPump(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        if httpx is None:
            self.skipTest("httpx 未安装，跳过需要 ASGI 客户端的测试")

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

    async def test_execute_task_stream_uses_pump(self):
        from backend.src.main import create_app
        from backend.src.storage import get_connection
        from backend.src.api.utils import now_iso

        created_at = now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("t", "queued", created_at, None, None, None),
            )
            task_id = int(cursor.lastrowid)

        app = create_app()
        transport = httpx.ASGITransport(app=app)

        with patch(
            "backend.src.api.tasks.routes_task_execute.pump_sync_generator",
            _fake_pump_sync_generator,
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream(
                    "POST",
                    f"/api/tasks/{task_id}/execute/stream",
                    json={},
                    timeout=10,
                ) as resp:
                    body = await resp.aread()

        text = body.decode("utf-8", errors="ignore")
        self.assertIn("hello", text)
        self.assertIn("event: done", text)


if __name__ == "__main__":
    unittest.main()

