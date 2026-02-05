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


class TestAgentResumeThinkMode(unittest.IsolatedAsyncioTestCase):
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
        # 部分测试会触发后台线程短暂写库，可能与 TemporaryDirectory cleanup 产生竞态
        for _ in range(10):
            try:
                self._tmpdir.cleanup()
                return
            except OSError:
                time.sleep(0.05)
        shutil.rmtree(self._tmpdir.name, ignore_errors=True)

    async def test_resume_waiting_think_run_uses_executor_model(self):
        """
        验证：think 模式进入 waiting 后，通过 /agent/command/resume/stream 恢复执行时，
        ReAct 每步 LLM 调用会按 executor 配置选择模型（step_llm_config_resolver 生效）。
        """
        from backend.src.common.utils import now_iso
        from backend.src.main import create_app
        from backend.src.storage import get_connection

        created_at = now_iso()
        with get_connection() as conn:
            cur = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("测试 think resume", "waiting", created_at, None, created_at, None),
            )
            task_id = int(cur.lastrowid)

            plan = {
                "titles": ["输出结果"],
                "allows": [["task_output"]],
                "artifacts": [],
                "items": [{"id": 1, "brief": "输出", "status": "pending"}],
            }
            state = {
                "mode": "think",
                "message": "测试 think resume",
                "workdir": os.getcwd(),
                "tools_hint": "(无)",
                "skills_hint": "(无)",
                "solutions_hint": "(无)",
                "memories_hint": "(无)",
                "graph_hint": "(无)",
                "context": {"last_llm_response": None},
                "observations": [],
                "step_order": 1,
                "paused": {"step_order": 1, "question": "是否继续？"},
                "model": "base-model",
                "parameters": {"temperature": 0},
                # 自定义 think_config：executor_code 使用不同模型，方便断言
                "think_config": {"agents": {"planner_a": "planner-model", "executor_code": "executor-model"}},
            }

            cur = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_plan, agent_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    "waiting",
                    "agent_test",
                    created_at,
                    None,
                    created_at,
                    created_at,
                    json.dumps(plan, ensure_ascii=False),
                    json.dumps(state, ensure_ascii=False),
                ),
            )
            run_id = int(cur.lastrowid)

        captured_models: list[str] = []

        def fake_create_llm_call(payload: dict):
            captured_models.append(str(payload.get("model") or ""))
            action = {
                "action": {"type": "task_output", "payload": {"output_type": "text", "content": "ok"}}
            }
            return {"record": {"status": "success", "response": json.dumps(action, ensure_ascii=False)}}

        app = create_app()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            with patch("backend.src.agent.runner.react_loop.create_llm_call", side_effect=fake_create_llm_call):
                async with client.stream(
                    "POST",
                    "/api/agent/command/resume/stream",
                    json={"run_id": run_id, "message": "继续"},
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        # 至少一次 LLM 调用应使用 executor_code 配置的模型
        self.assertTrue(captured_models, "应至少调用一次 LLM")
        self.assertIn("executor-model", captured_models)


if __name__ == "__main__":
    unittest.main()

