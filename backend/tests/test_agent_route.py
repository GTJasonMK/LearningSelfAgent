import json
import os
import tempfile
import unittest
from unittest.mock import patch

try:
    import httpx  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    httpx = None


class TestAgentRoute(unittest.IsolatedAsyncioTestCase):
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

    async def test_agent_route_returns_mode(self):
        from backend.src.main import create_app

        fake_route_json = json.dumps(
            {"mode": "do", "confidence": 0.8, "reason": "ok"},
            ensure_ascii=False,
        )

        app = create_app()
        transport = httpx.ASGITransport(app=app)

        with patch(
            "backend.src.agent.runner.route_mode.call_openai",
            return_value=(fake_route_json, None, None),
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post("/api/agent/route", json={"message": "hi"})
                self.assertEqual(resp.status_code, 200)
                data = resp.json()
                self.assertEqual(data.get("mode"), "do")
                self.assertAlmostEqual(float(data.get("confidence", 0.0)), 0.8, places=3)


if __name__ == "__main__":
    unittest.main()

