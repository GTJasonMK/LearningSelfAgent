import json
import importlib.util
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

HAS_FASTAPI = importlib.util.find_spec("fastapi") is not None


class TestResumePromptTokenIdempotency(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        if httpx is None:
            self.skipTest("httpx 未安装，跳过需要 ASGI 客户端的测试")
        if not HAS_FASTAPI:
            self.skipTest("fastapi 未安装，跳过需要 ASGI 客户端的测试")

        import backend.src.storage as storage
        from backend.src.agent.runner import stream_resume_run

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_test.db")

        os.environ["AGENT_DB_PATH"] = self._db_path
        os.environ["AGENT_PROMPT_ROOT"] = os.path.join(self._tmpdir.name, "prompt")
        storage.init_db()
        stream_resume_run._RESUME_TOKEN_STATE.clear()

    def tearDown(self):
        from backend.src.agent.runner import stream_resume_run

        stream_resume_run._RESUME_TOKEN_STATE.clear()
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

    def _create_waiting_run(self, prompt_token: str, *, session_key: str = "") -> int:
        from backend.src.api.utils import now_iso
        from backend.src.storage import get_connection

        created_at = now_iso()
        with get_connection() as conn:
            cur = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("测试 prompt_token resume", "waiting", created_at, None, created_at, None),
            )
            task_id = int(cur.lastrowid)

            plan = {
                "titles": ["task_output 输出结果"],
                "allows": [["task_output"]],
                "artifacts": [],
                "items": [{"id": 1, "brief": "输出", "status": "pending"}],
            }
            state = {
                "mode": "do",
                "message": "测试 prompt_token resume",
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
                    "question": "请确认是否继续",
                    "step_title": "user_prompt:请确认是否继续",
                    "kind": "user_prompt",
                    "prompt_token": str(prompt_token or "").strip(),
                },
                "session_key": str(session_key or "").strip() or None,
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
            return int(cur.lastrowid)

    async def test_resume_rejects_mismatched_prompt_token(self):
        from backend.src.main import create_app

        run_id = self._create_waiting_run(prompt_token="token-expected")
        app = create_app()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/api/agent/command/resume/stream",
                json={"run_id": run_id, "message": "继续", "prompt_token": "token-wrong"},
            )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("prompt_token 不匹配", resp.text)

    async def test_resume_rejects_missing_prompt_token_when_required(self):
        from backend.src.main import create_app

        run_id = self._create_waiting_run(prompt_token="token-required")
        app = create_app()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/api/agent/command/resume/stream",
                json={"run_id": run_id, "message": "继续"},
            )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("缺少 prompt_token", resp.text)

    async def test_resume_rejects_missing_session_key_when_required(self):
        from backend.src.main import create_app

        run_id = self._create_waiting_run(prompt_token="token-required", session_key="sess-required")
        app = create_app()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/api/agent/command/resume/stream",
                json={"run_id": run_id, "message": "继续", "prompt_token": "token-required"},
            )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("缺少 session_key", resp.text)

    async def test_resume_token_can_run_once_and_reject_duplicate_submit(self):
        from backend.src.constants import RUN_STATUS_DONE
        from backend.src.main import create_app

        run_id = self._create_waiting_run(prompt_token="token-once")

        async def _fake_iter_resume_mode_execution_events(config):
            yield (
                "result",
                {
                    "run_status": RUN_STATUS_DONE,
                    "last_step_order": 1,
                    "state_obj": dict(config.state_obj or {}),
                    "plan_struct": config.plan_struct,
                },
            )

        async def _fake_iter_resume_finalization_events(_config):
            yield ("status", RUN_STATUS_DONE)

        async def _fake_apply_resume_user_input(
            *,
            task_id,
            run_id,
            user_input,
            question,
            paused,
            paused_step_order,
            resume_step_order,
            plan_titles,
            plan_items,
            plan_allows,
            plan_artifacts,
            observations,
            context,
            state_obj,
            safe_write_debug,
            is_task_feedback_step_title_func,
        ):
            _ = (
                task_id,
                run_id,
                user_input,
                question,
                paused,
                paused_step_order,
                plan_titles,
                plan_items,
                plan_allows,
                plan_artifacts,
                observations,
                context,
                safe_write_debug,
                is_task_feedback_step_title_func,
            )
            return int(resume_step_order), dict(state_obj or {})

        app = create_app()
        transport = httpx.ASGITransport(app=app)
        with patch(
            "backend.src.agent.runner.stream_resume_run.iter_resume_mode_execution_events",
            side_effect=_fake_iter_resume_mode_execution_events,
        ), patch(
            "backend.src.agent.runner.stream_resume_run.iter_resume_finalization_events",
            side_effect=_fake_iter_resume_finalization_events,
        ), patch(
            "backend.src.agent.runner.stream_resume_run.apply_resume_user_input",
            side_effect=_fake_apply_resume_user_input,
        ), patch(
            "backend.src.agent.runner.stream_resume_run.enqueue_postprocess_thread",
            return_value=None,
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream(
                    "POST",
                    "/api/agent/command/resume/stream",
                    json={"run_id": run_id, "message": "继续", "prompt_token": "token-once"},
                ) as resp1:
                    self.assertEqual(resp1.status_code, 200)
                    body = (await resp1.aread()).decode("utf-8", errors="ignore")
                    self.assertIn("event: done", body)

                resp2 = await client.post(
                    "/api/agent/command/resume/stream",
                    json={"run_id": run_id, "message": "再次继续", "prompt_token": "token-once"},
                )
                self.assertEqual(resp2.status_code, 400)
                self.assertIn("已提交", resp2.text)


if __name__ == "__main__":
    unittest.main()
