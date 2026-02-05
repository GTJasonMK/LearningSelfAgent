import os
import tempfile
import unittest

try:
    import httpx  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    httpx = None


class TestRecentRecords(unittest.IsolatedAsyncioTestCase):
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

    async def test_records_recent_returns_items(self):
        from backend.src.main import create_app
        from backend.src.storage import get_connection

        t1 = "2026-01-01T00:00:00Z"
        t2 = "2026-01-02T00:00:00Z"
        t3 = "2026-01-03T00:00:00Z"

        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("任务A", "done", t1, None, t1, t1),
            )
            task_id = int(cursor.lastrowid)

            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (task_id, "running", "agent_command_react", t2, None, t2, t2),
            )
            run_id = int(cursor.lastrowid)

            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_id,
                    "步骤1",
                    "done",
                    '{"type":"tool_call","payload":{"tool_name":"x","input":"y"}}',
                    None,
                    None,
                    1,
                    t2,
                    t2,
                    1,
                    t2,
                    t2,
                ),
            )

            conn.execute(
                "INSERT INTO task_outputs (task_id, run_id, output_type, content, created_at) VALUES (?, ?, ?, ?, ?)",
                (task_id, run_id, "text", "结果内容", t2),
            )

            conn.execute(
                "INSERT INTO memory_items (content, created_at, memory_type, tags, task_id) VALUES (?, ?, ?, ?, ?)",
                ("记忆内容", t3, "note", "[]", task_id),
            )

        app = create_app()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/records/recent", params={"limit": 10})
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertIsInstance(payload.get("items"), list)
        items = payload["items"]
        self.assertTrue(items)

        # 最新应包含 memory
        types = [it.get("type") for it in items]
        self.assertIn("memory", types)
        self.assertIn("run", types)
        self.assertIn("step", types)
        self.assertIn("output", types)


if __name__ == "__main__":
    unittest.main()
