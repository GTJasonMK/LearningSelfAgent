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


class TestUserPromptResumeAdvances(unittest.IsolatedAsyncioTestCase):
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

    async def test_resume_skips_user_prompt_only_step(self):
        """
        验证：当 paused 的步骤 allow 仅为 user_prompt 时，用户回答后 resume 应推进到下一步，
        避免重复提问导致卡死；同时把 paused.step_id 对应的 task_steps 从 waiting 标记为 done。
        """
        from backend.src.common.utils import now_iso
        from backend.src.main import create_app
        from backend.src.storage import get_connection

        created_at = now_iso()
        with get_connection() as conn:
            cur = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("测试 user_prompt resume", "waiting", created_at, None, created_at, None),
            )
            task_id = int(cur.lastrowid)

            plan = {
                "titles": ["user_prompt:请提供信息", "输出结果"],
                "allows": [["user_prompt"], ["task_output"]],
                "artifacts": [],
                "items": [
                    {"id": 1, "brief": "提问", "status": "waiting"},
                    {"id": 2, "brief": "输出", "status": "pending"},
                ],
            }

            # 预先插入一个 waiting 的 step（模拟 user_prompt 动作触发的暂停）
            detail = json.dumps(
                {"type": "user_prompt", "payload": {"question": "请提供信息"}},
                ensure_ascii=False,
            )
            cur = conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at, executor) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    1,
                    "user_prompt:请提供信息",
                    "waiting",
                    detail,
                    None,
                    None,
                    1,
                    created_at,
                    None,
                    1,
                    created_at,
                    created_at,
                    None,
                ),
            )
            paused_step_id = int(cur.lastrowid)

            state = {
                "mode": "do",
                "message": "测试 user_prompt resume",
                "workdir": os.getcwd(),
                "tools_hint": "(无)",
                "skills_hint": "(无)",
                "solutions_hint": "(无)",
                "memories_hint": "(无)",
                "graph_hint": "(无)",
                "context": {"last_llm_response": None},
                "observations": [],
                "step_order": 1,
                "paused": {
                    "step_order": 1,
                    "question": "请提供信息",
                    "step_title": "user_prompt:请提供信息",
                    "step_id": paused_step_id,
                },
                "model": "base-model",
                "parameters": {"temperature": 0},
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

            # 修正前插入的 task_steps.run_id（为了保持一致性）
            conn.execute("UPDATE task_steps SET run_id = ? WHERE id = ?", (int(run_id), int(paused_step_id)))

        def fake_create_llm_call(payload: dict):
            prompt_text = str(payload.get("prompt") or "")
            if "请提供信息" in prompt_text:
                raise AssertionError("resume 不应回到 user_prompt 步骤重复提问")
            action = {"action": {"type": "task_output", "payload": {"output_type": "text", "content": "ok"}}}
            return {"record": {"status": "success", "response": json.dumps(action, ensure_ascii=False)}}

        def fake_execute_step_action(_task_id, _run_id, _step_row, context=None):
            return {"ok": True}, None

        app = create_app()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            with patch("backend.src.agent.runner.react_loop.create_llm_call", side_effect=fake_create_llm_call), patch(
                "backend.src.agent.runner.react_loop._execute_step_action", side_effect=fake_execute_step_action
            ):
                async with client.stream(
                    "POST",
                    "/api/agent/command/resume/stream",
                    json={"run_id": run_id, "message": "答案"},
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        # waiting step 应被标记为 done，并写入 result
        with get_connection() as conn:
            row = conn.execute("SELECT status, result FROM task_steps WHERE id = ?", (int(paused_step_id),)).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["status"], "done")
        self.assertIn("答案", str(row["result"] or ""))


if __name__ == "__main__":
    unittest.main()

