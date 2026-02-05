import os
import tempfile
import unittest

try:
    import httpx  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    httpx = None


class TestTasksListFilter(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        if httpx is None:
            self.skipTest("httpx 未安装，跳过需要 ASGI 客户端的测试")

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

    async def test_list_tasks_orders_by_created_at_desc(self):
        from backend.src.main import create_app
        from backend.src.storage import get_connection

        t1 = "2026-01-01T10:00:00Z"
        t2 = "2026-01-02T10:00:00Z"
        t3 = "2026-01-02T12:00:00Z"

        with get_connection() as conn:
            conn.execute(
                "INSERT INTO tasks (title, status, created_at) VALUES (?, ?, ?)",
                ("任务1", "done", t1),
            )
            conn.execute(
                "INSERT INTO tasks (title, status, created_at) VALUES (?, ?, ?)",
                ("任务2", "done", t2),
            )
            conn.execute(
                "INSERT INTO tasks (title, status, created_at) VALUES (?, ?, ?)",
                ("任务3", "done", t3),
            )

        app = create_app()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/tasks")
        self.assertEqual(resp.status_code, 200)
        items = resp.json().get("items") or []
        self.assertEqual([it.get("title") for it in items], ["任务3", "任务2", "任务1"])

    async def test_list_tasks_filters_by_date_and_days(self):
        from backend.src.main import create_app
        from backend.src.storage import get_connection

        t1 = "2026-01-01T10:00:00Z"
        t2 = "2026-01-02T10:00:00Z"
        t3 = "2026-01-02T12:00:00Z"

        with get_connection() as conn:
            conn.execute(
                "INSERT INTO tasks (title, status, created_at) VALUES (?, ?, ?)",
                ("任务1", "done", t1),
            )
            conn.execute(
                "INSERT INTO tasks (title, status, created_at) VALUES (?, ?, ?)",
                ("任务2", "done", t2),
            )
            conn.execute(
                "INSERT INTO tasks (title, status, created_at) VALUES (?, ?, ?)",
                ("任务3", "done", t3),
            )

        app = create_app()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:

            # 仅 1 天：2026-01-02
            resp = await client.get("/api/tasks", params={"date": "2026-01-02", "days": 1})
            self.assertEqual(resp.status_code, 200)
            items = resp.json().get("items") or []
            self.assertEqual([it.get("title") for it in items], ["任务3", "任务2"])

            # 连续 2 天：2026-01-01 + 2026-01-02
            resp2 = await client.get("/api/tasks", params={"date": "2026-01-01", "days": 2})
            self.assertEqual(resp2.status_code, 200)
            items2 = resp2.json().get("items") or []
            self.assertEqual([it.get("title") for it in items2], ["任务3", "任务2", "任务1"])

    async def test_list_tasks_rejects_invalid_date(self):
        from backend.src.main import create_app

        app = create_app()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/tasks", params={"date": "2026-01-99", "days": 1})
        self.assertEqual(resp.status_code, 400)
        payload = resp.json()
        self.assertEqual(payload.get("error", {}).get("code"), "INVALID_REQUEST")


if __name__ == "__main__":
    unittest.main()
