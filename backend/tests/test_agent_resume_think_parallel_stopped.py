import json
import os
import shutil
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    import httpx  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    httpx = None


class TestAgentResumeThinkParallelStopped(unittest.IsolatedAsyncioTestCase):
    """
    回归：think 并行阶段中断（stopped）后，通过 resume 继续执行仍应正常推进：
    - 不会因为 step_order 漂移而跳过未完成步骤（并行 loop 会回退到最早未完成 step）
    - 仍能按 roles 并行调度执行（至少 executor_doc/executor_code 可同时跑）
    """

    def setUp(self):
        if httpx is None:
            self.skipTest("httpx 未安装，跳过需要 ASGI 客户端的测试")

        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = Path(self._tmpdir.name) / "agent_test.db"
        self._prompt_root = Path(self._tmpdir.name) / "prompt"
        self._workdir = Path(self._tmpdir.name) / "workdir"

        os.environ["AGENT_DB_PATH"] = str(self._db_path)
        os.environ["AGENT_PROMPT_ROOT"] = str(self._prompt_root)
        os.makedirs(self._prompt_root, exist_ok=True)
        os.makedirs(self._workdir, exist_ok=True)

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

    async def test_resume_stopped_think_parallel_continues_and_runs_pending_steps_in_parallel(self):
        from backend.src.common.utils import now_iso
        from backend.src.main import create_app
        from backend.src.storage import get_connection

        created_at = now_iso()
        with get_connection() as conn:
            cur = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("测试 think parallel stopped resume", "stopped", created_at, None, created_at, created_at),
            )
            task_id = int(cur.lastrowid)

            plan = {
                "titles": [
                    "file_write:out/code.py 写入代码（已完成）",
                    "file_write:README.md 写入文档（并行）",
                    "shell_command: echo ok（并行）",
                    "task_output 输出结果",
                ],
                "allows": [["file_write"], ["file_write"], ["shell_command"], ["task_output"]],
                "artifacts": [],
                "items": [
                    {"id": 1, "brief": "写代码", "status": "done"},
                    {"id": 2, "brief": "写文档", "status": "pending"},
                    {"id": 3, "brief": "执行命令", "status": "pending"},
                    {"id": 4, "brief": "输出", "status": "pending"},
                ],
            }
            state = {
                "mode": "think",
                "message": "测试 think parallel stopped resume",
                "workdir": str(self._workdir),
                "tools_hint": "(无)",
                "skills_hint": "(无)",
                "solutions_hint": "(无)",
                "memories_hint": "(无)",
                "graph_hint": "(无)",
                "context": {"last_llm_response": None},
                "observations": [],
                # 故意设置为最后一步：验证并行 loop 会回退到最早未完成步骤
                "step_order": 4,
                "paused": None,
                "model": "base-model",
                "parameters": {"temperature": 0},
            }
            cur = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_plan, agent_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    "stopped",
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

        # 并行断言：step2（doc）与 step3（code）应并行进入 execute_step_action
        started_doc = threading.Event()
        started_code = threading.Event()

        llm_actions_by_step_order = {
            2: {"action": {"type": "file_write", "payload": {"path": "README.md", "content": "doc"}}},
            3: {"action": {"type": "shell_command", "payload": {"command": "echo ok"}}},
            4: {"action": {"type": "task_output", "payload": {"output_type": "text", "content": "ok"}}},
        }

        def _fake_create_llm_call(payload: dict):
            try:
                step_order = int((payload.get("variables") or {}).get("step_order") or 0)
            except Exception:
                step_order = 0
            action = llm_actions_by_step_order.get(step_order)
            if action is None:
                raise AssertionError(f"不应请求该 step_order 的 action：{step_order}")
            return {"record": {"status": "success", "response": json.dumps(action, ensure_ascii=False)}}

        def _fake_execute_step_action(_task_id, _run_id, step_row, context=None):
            title = str(step_row.get("title") or "")
            if "README.md" in title:
                started_doc.set()
                if not started_code.wait(timeout=2):
                    raise AssertionError("executor_code 未并行启动（可能退化为串行调度）")
            if "echo ok" in title:
                started_code.set()
                if not started_doc.wait(timeout=2):
                    raise AssertionError("executor_doc 未并行启动（可能退化为串行调度）")
            return {"ok": True}, None

        app = create_app()
        transport = httpx.ASGITransport(app=app)

        with patch(
            "backend.src.agent.runner.execution_pipeline.enqueue_postprocess_thread",
            return_value=None,
        ), patch(
            "backend.src.agent.runner.react_loop.create_llm_call",
            side_effect=_fake_create_llm_call,
        ), patch(
            "backend.src.agent.runner.react_loop._execute_step_action",
            side_effect=_fake_execute_step_action,
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream(
                    "POST",
                    "/api/agent/command/resume/stream",
                    json={"run_id": int(run_id), "message": "继续"},
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        self.assertTrue(started_doc.is_set(), "应执行到 executor_doc 的并行步骤")
        self.assertTrue(started_code.is_set(), "应执行到 executor_code 的并行步骤")

        with get_connection() as conn:
            row = conn.execute(
                "SELECT status, agent_plan FROM task_runs WHERE id = ?",
                (int(run_id),),
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(str(row["status"] or ""), "done")

        plan_obj = json.loads(row["agent_plan"] or "{}")
        items = plan_obj.get("items") or []
        self.assertEqual([str(it.get("status") or "") for it in items], ["done", "done", "done", "done"])


if __name__ == "__main__":
    unittest.main()

