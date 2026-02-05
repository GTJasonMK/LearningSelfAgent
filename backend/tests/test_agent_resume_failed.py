import json
import os
import shutil
import tempfile
import time
import unittest
from unittest.mock import patch

try:
    import httpx  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    httpx = None


class TestAgentResumeFailed(unittest.IsolatedAsyncioTestCase):
    """
    回归：run.status=failed 也允许通过 /agent/command/resume/stream 继续执行。
    目的：与 docs/agent 的“失败步骤可恢复继续”语义对齐，并提升编排可用性。
    """

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

        for _ in range(10):
            try:
                self._tmpdir.cleanup()
                return
            except OSError:
                time.sleep(0.05)
        shutil.rmtree(self._tmpdir.name, ignore_errors=True)

    async def test_resume_supports_failed_run(self):
        from backend.src.api.utils import now_iso
        from backend.src.main import create_app
        from backend.src.storage import get_connection

        created_at = now_iso()
        with get_connection() as conn:
            cur = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("测试 failed 继续", "failed", created_at, None, created_at, created_at),
            )
            task_id = int(cur.lastrowid)
            plan = {
                "titles": ["task_output 输出结果"],
                "allows": [["task_output"]],
                "artifacts": [],
                "items": [{"id": 1, "brief": "输出", "status": "pending"}],
            }
            state = {
                "message": "测试 failed 继续",
                "workdir": os.getcwd(),
                "tools_hint": "(无)",
                "skills_hint": "(无)",
                "memories_hint": "(无)",
                "graph_hint": "(无)",
                "context": {"last_llm_response": None},
                "observations": [],
                "step_order": 1,
                "paused": None,
                "mode": "do",
                "model": "gpt-4o-mini",
                "parameters": {"temperature": 0},
            }
            cur = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_plan, agent_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    "failed",
                    "agent_test",
                    created_at,
                    created_at,
                    created_at,
                    created_at,
                    json.dumps(plan, ensure_ascii=False),
                    json.dumps(state, ensure_ascii=False),
                ),
            )
            run_id = int(cur.lastrowid)

        fake_action = json.dumps(
            {"action": {"type": "task_output", "payload": {"output_type": "text", "content": "ok"}}},
            ensure_ascii=False,
        )

        app = create_app()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            with patch(
                "backend.src.agent.runner.react_loop.create_llm_call",
                return_value={"record": {"status": "success", "response": fake_action}},
            ), patch(
                "backend.src.agent.runner.stream_resume_run.enqueue_postprocess_thread",
                return_value=None,
            ):
                async with client.stream(
                    "POST",
                    "/api/agent/command/resume/stream",
                    json={"run_id": run_id, "message": "继续"},
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        with get_connection() as conn:
            row = conn.execute("SELECT status FROM task_runs WHERE id = ?", (run_id,)).fetchone()
        self.assertIsNotNone(row)
        self.assertIn(str(row["status"] or ""), {"done", "failed", "waiting"})


if __name__ == "__main__":
    unittest.main()

