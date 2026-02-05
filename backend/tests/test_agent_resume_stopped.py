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


class TestAgentResumeStopped(unittest.IsolatedAsyncioTestCase):
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
        # 某些测试会触发后台线程写库（例如评估/后处理/日志），可能与 TemporaryDirectory cleanup 产生竞态：
        # sqlite 会短暂创建 journal 文件，导致 rmtree 最后一步 rmdir 抛 “Directory not empty”。
        for _ in range(10):
            try:
                self._tmpdir.cleanup()
                return
            except OSError:
                time.sleep(0.05)
        shutil.rmtree(self._tmpdir.name, ignore_errors=True)

    async def test_resume_supports_stopped_run(self):
        from backend.src.api.utils import now_iso
        from backend.src.main import create_app
        from backend.src.storage import get_connection

        created_at = now_iso()
        with get_connection() as conn:
            cur = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("测试 stopped 继续", "stopped", created_at, None, created_at, None),
            )
            task_id = int(cur.lastrowid)
            plan = {
                "titles": ["输出结果"],
                "allows": [["task_output"]],
                "artifacts": [],
                "items": [{"id": 1, "brief": "输出", "status": "pending"}],
            }
            state = {
                "message": "测试 stopped 继续",
                "workdir": os.getcwd(),
                "tools_hint": "(无)",
                "skills_hint": "(无)",
                "memories_hint": "(无)",
                "graph_hint": "(无)",
                "context": {"last_llm_response": None},
                "observations": [],
                "step_order": 1,
                "paused": None,
                "model": "gpt-4o-mini",
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

        fake_action = json.dumps(
            {
                "action": {
                    "type": "task_output",
                    "payload": {"output_type": "text", "content": "ok"},
                }
            },
            ensure_ascii=False,
        )

        app = create_app()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            with patch(
                "backend.src.agent.runner.react_loop.create_llm_call",
                return_value={"record": {"status": "success", "response": fake_action}},
            ):
                async with client.stream(
                    "POST",
                    "/api/agent/command/resume/stream",
                    json={"run_id": run_id, "message": "继续"},
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        # 继续执行后：run 应有新 finished_at（done）且不再是 stopped
        with get_connection() as conn:
            row = conn.execute("SELECT status, finished_at FROM task_runs WHERE id = ?", (run_id,)).fetchone()
        self.assertIsNotNone(row)
        self.assertIn(row["status"], {"done", "failed", "waiting"})

    async def test_resume_normalizes_allow_aliases(self):
        """
        回归：旧 run 的 plan_allows 可能包含 alias（如 tool/cmd），若不归一化会触发 allow_mismatch
        并导致 create_llm_call 被额外调用（甚至进入错误处理/重规划）。
        """
        from backend.src.api.utils import now_iso
        from backend.src.main import create_app
        from backend.src.storage import get_connection

        created_at = now_iso()
        with get_connection() as conn:
            cur = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("测试 allow alias 归一化", "stopped", created_at, None, created_at, None),
            )
            task_id = int(cur.lastrowid)
            plan = {
                "titles": ["tool_call:web_fetch 抓取数据"],
                # 旧数据/模型别名：tool -> tool_call
                "allows": [["tool"]],
                "artifacts": [],
                "items": [{"id": 1, "brief": "抓取", "status": "pending"}],
            }
            state = {
                "message": "测试 allow alias 归一化",
                "workdir": os.getcwd(),
                "tools_hint": "(无)",
                "skills_hint": "(无)",
                "memories_hint": "(无)",
                "graph_hint": "(无)",
                "context": {"last_llm_response": None},
                "observations": [],
                "step_order": 1,
                "paused": None,
                "model": "gpt-4o-mini",
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

        fake_action = json.dumps(
            {
                "action": {
                    "type": "tool_call",
                    "payload": {"input": "ping"},
                }
            },
            ensure_ascii=False,
        )

        call_count = {"n": 0}

        def _fake_create_llm_call(_payload: dict):
            call_count["n"] += 1
            if call_count["n"] > 1:
                raise AssertionError("create_llm_call 被多次调用，说明 allow alias 未被归一化")
            return {"record": {"status": "success", "response": fake_action}}

        def _fake_execute_step_action(_task_id, _run_id, _step_row, context=None):
            # 避免真实执行 tool_call（需要工具注册/执行权限等），只验证编排链路
            return {"ok": True}, None

        app = create_app()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            with patch(
                "backend.src.agent.runner.react_loop.create_llm_call",
                side_effect=_fake_create_llm_call,
            ), patch(
                "backend.src.agent.runner.react_loop._execute_step_action",
                side_effect=_fake_execute_step_action,
            ):
                async with client.stream(
                    "POST",
                    "/api/agent/command/resume/stream",
                    json={"run_id": run_id, "message": "继续"},
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

    async def test_resume_noop_when_plan_already_done(self):
        """
        回归：stopped run 可能在“最后一步已完成”后异常退出，重启时被 stop-running 标记为 stopped。
        resume 此时不应重复执行最后一步（避免二次写文件/重复输出），而应直接进入收尾：
        - artifacts 校验
        - finalize_run_and_task_status
        - enqueue_postprocess_thread
        """
        from backend.src.api.utils import now_iso
        from backend.src.main import create_app
        from backend.src.storage import get_connection

        created_at = now_iso()
        with get_connection() as conn:
            cur = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("测试 stopped 已完成收尾", "stopped", created_at, None, created_at, created_at),
            )
            task_id = int(cur.lastrowid)
            plan = {
                "titles": ["输出结果 1", "输出结果 2"],
                "allows": [["task_output"], ["task_output"]],
                "artifacts": [],
                "items": [
                    {"id": 1, "brief": "输出1", "status": "done"},
                    {"id": 2, "brief": "输出2", "status": "done"},
                ],
            }
            state = {
                "message": "测试 stopped 已完成收尾",
                "workdir": os.getcwd(),
                "tools_hint": "(无)",
                "skills_hint": "(无)",
                "memories_hint": "(无)",
                "graph_hint": "(无)",
                "context": {"last_llm_response": None},
                "observations": [],
                "step_order": 2,
                "paused": None,
                "model": "gpt-4o-mini",
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

            # 模拟“所有步骤都已 done，但 run 状态仍是 stopped”
            detail = json.dumps({"type": "task_output", "payload": {"output_type": "text"}}, ensure_ascii=False)
            for order in (1, 2):
                conn.execute(
                    "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at, executor) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        task_id,
                        run_id,
                        f"输出结果 {order}",
                        "done",
                        detail,
                        "ok",
                        None,
                        1,
                        created_at,
                        created_at,
                        order,
                        created_at,
                        created_at,
                        None,
                    ),
                )

        app = create_app()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            with patch(
                "backend.src.agent.runner.react_loop.create_llm_call",
                side_effect=AssertionError("计划已完成，resume 不应再调用 create_llm_call"),
            ):
                async with client.stream(
                    "POST",
                    "/api/agent/command/resume/stream",
                    json={"run_id": run_id, "message": "继续"},
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        with get_connection() as conn:
            run_row = conn.execute("SELECT status FROM task_runs WHERE id = ?", (int(run_id),)).fetchone()
            step_count = conn.execute("SELECT COUNT(*) AS c FROM task_steps WHERE run_id = ?", (int(run_id),)).fetchone()
        self.assertIsNotNone(run_row)
        self.assertEqual(str(run_row["status"] or ""), "done")
        self.assertIsNotNone(step_count)
        self.assertEqual(int(step_count["c"] or 0), 2)


if __name__ == "__main__":
    unittest.main()
