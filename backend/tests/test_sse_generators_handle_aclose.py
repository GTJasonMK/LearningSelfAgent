import json
import os
import shutil
import tempfile
import time
import unittest
import asyncio


class TestSSEGeneratorsHandleAclose(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
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
        # 某些 SSE/后处理链路会在测试结束后短暂触发后台线程写库（sqlite 可能创建 journal/WAL 文件），
        # 与 TemporaryDirectory cleanup 产生竞态，导致 rmtree 最后一步 rmdir 抛 “Directory not empty”。
        for _ in range(10):
            try:
                self._tmpdir.cleanup()
                break
            except OSError:
                time.sleep(0.05)
        else:
            shutil.rmtree(self._tmpdir.name, ignore_errors=True)

    async def test_execute_task_stream_aclose_does_not_raise(self):
        """
        回归：SSE async generator 在收到 aclose/断连时不得继续 yield，
        否则会触发 “async generator ignored GeneratorExit”。
        """
        from backend.src.api.utils import now_iso
        from backend.src.storage import get_connection

        created_at = now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("测试任务", "queued", created_at, None, None, None),
            )
            task_id = int(cursor.lastrowid)
            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    None,
                    "输出结果",
                    "planned",
                    json.dumps({"type": "task_output", "payload": {"output_type": "text", "content": "hi"}}, ensure_ascii=False),
                    None,
                    None,
                    None,
                    None,
                    None,
                    1,
                    created_at,
                    created_at,
                ),
            )

        from backend.src.api.tasks.routes_task_execute import execute_task_stream

        resp = await execute_task_stream(task_id)
        aiter = resp.body_iterator

        first = await aiter.__anext__()
        self.assertIsInstance(first, str)
        self.assertTrue(first.startswith("data: "))

        # 不应抛 “async generator ignored GeneratorExit”
        await aiter.aclose()

    async def test_agent_evaluate_stream_aclose_does_not_raise(self):
        from backend.src.api.utils import now_iso
        from backend.src.storage import get_connection

        created_at = now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("测试任务", "done", created_at, None, created_at, created_at),
            )
            task_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_plan, agent_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    "done",
                    "test_run",
                    created_at,
                    created_at,
                    created_at,
                    created_at,
                    json.dumps({"titles": ["一步"], "allows": [["task_output"]], "artifacts": []}, ensure_ascii=False),
                    json.dumps({"message": "测试", "step_order": 1}, ensure_ascii=False),
                ),
            )
            run_id = int(cursor.lastrowid)

        from backend.src.api.agent.routes_agent_evaluate import agent_evaluate_stream
        from backend.src.api.schemas import AgentEvaluateStreamRequest

        resp = await agent_evaluate_stream(AgentEvaluateStreamRequest(run_id=run_id, message="x"))
        aiter = resp.body_iterator

        first = await aiter.__anext__()
        self.assertIsInstance(first, str)
        self.assertTrue(first.startswith("data: "))

        await aiter.aclose()

    async def test_llm_chat_stream_aclose_does_not_raise(self):
        """
        回归：/llm/chat/stream 在断连/主动 aclose 时不得在 finally 继续 yield done，
        否则可能触发 “async generator ignored GeneratorExit/CancelledError”。
        """
        import asyncio

        import backend.src.services.llm.llm_client as llm_client

        original_client = llm_client.LLMClient

        class FakeLLMClient:
            def __init__(self, provider=None):
                self._provider = provider

            async def aclose(self) -> None:
                return

            async def stream_chat(self, messages, model=None, parameters=None, timeout=120):
                # 先产出 1 个 chunk，便于测试在中途断连（aclose）时的行为。
                yield {"content": "hi"}
                # 保持 generator 处于“可继续输出”的状态，确保 aclose 会触发关闭路径。
                while True:
                    await asyncio.sleep(3600)
                    yield {"content": "never"}

        llm_client.LLMClient = FakeLLMClient
        try:
            from backend.src.api.knowledge.routes_prompts import stream_llm_chat
            from backend.src.api.schemas import LLMChatStreamRequest

            resp = await stream_llm_chat(LLMChatStreamRequest(message="x", model="test-model"))
            aiter = resp.body_iterator

            first = await aiter.__anext__()
            self.assertIsInstance(first, str)
            self.assertTrue(first.startswith("data: "))

            await aiter.aclose()
        finally:
            llm_client.LLMClient = original_client

    async def test_agent_command_stream_aclose_marks_stopped(self):
        """
        回归：/agent/command/stream 在断连/主动 aclose 时应收敛 run/task 为 stopped，
        且不得抛 “async generator ignored GeneratorExit/CancelledError”。
        """
        from backend.src.agent.runner.stream_new_run import stream_agent_command
        from backend.src.api.schemas import AgentCommandStreamRequest
        from backend.src.storage import get_connection

        resp = stream_agent_command(AgentCommandStreamRequest(message="hi"))
        aiter = resp.body_iterator

        first = await aiter.__anext__()
        self.assertIsInstance(first, str)
        self.assertTrue(first.startswith("data: "))

        first_line = first.split("\n", 1)[0]
        obj = json.loads(first_line[len("data: ") :])
        self.assertEqual(obj.get("type"), "run_created")
        task_id = int(obj.get("task_id") or 0)
        run_id = int(obj.get("run_id") or 0)
        self.assertGreater(task_id, 0)
        self.assertGreater(run_id, 0)

        await aiter.aclose()

        # stop_task_run_records 在后台线程执行：允许短暂等待
        for _ in range(40):
            with get_connection() as conn:
                run_row = conn.execute("SELECT status FROM task_runs WHERE id = ?", (run_id,)).fetchone()
                task_row = conn.execute("SELECT status FROM tasks WHERE id = ?", (task_id,)).fetchone()
            if run_row and task_row and run_row["status"] == "stopped" and task_row["status"] == "stopped":
                return
            await asyncio.sleep(0.05)
        self.fail("断连后状态未收敛为 stopped")

    async def test_agent_think_command_stream_aclose_marks_stopped(self):
        """
        回归：/agent/command/stream（think）在断连/主动 aclose 时应收敛 run/task 为 stopped，
        且不得抛 “async generator ignored GeneratorExit/CancelledError”。
        """
        from backend.src.agent.runner.stream_think_run import stream_agent_think_command
        from backend.src.api.schemas import AgentCommandStreamRequest
        from backend.src.storage import get_connection

        resp = stream_agent_think_command(AgentCommandStreamRequest(message="hi", mode="think"))
        aiter = resp.body_iterator

        first = await aiter.__anext__()
        self.assertIsInstance(first, str)
        self.assertTrue(first.startswith("data: "))

        first_line = first.split("\n", 1)[0]
        obj = json.loads(first_line[len("data: ") :])
        self.assertEqual(obj.get("type"), "run_created")
        task_id = int(obj.get("task_id") or 0)
        run_id = int(obj.get("run_id") or 0)
        self.assertGreater(task_id, 0)
        self.assertGreater(run_id, 0)

        await aiter.aclose()

        for _ in range(40):
            with get_connection() as conn:
                run_row = conn.execute("SELECT status FROM task_runs WHERE id = ?", (run_id,)).fetchone()
                task_row = conn.execute("SELECT status FROM tasks WHERE id = ?", (task_id,)).fetchone()
            if run_row and task_row and run_row["status"] == "stopped" and task_row["status"] == "stopped":
                return
            await asyncio.sleep(0.05)
        self.fail("断连后状态未收敛为 stopped")

    async def test_agent_command_resume_stream_aclose_marks_stopped(self):
        """
        回归：/agent/command/resume/stream 在断连时应收敛 run/task 为 stopped，
        且不得抛 “async generator ignored GeneratorExit/CancelledError”。
        """
        from backend.src.api.utils import now_iso
        from backend.src.agent.runner.stream_resume_run import stream_agent_command_resume
        from backend.src.api.schemas import AgentCommandResumeStreamRequest
        from backend.src.storage import get_connection

        created_at = now_iso()
        with get_connection() as conn:
            cur = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("测试 resume aclose", "waiting", created_at, None, created_at, None),
            )
            task_id = int(cur.lastrowid)
            plan = {
                "titles": ["输出结果"],
                "allows": [["task_output"]],
                "artifacts": [],
                "items": [{"id": 1, "brief": "输出", "status": "pending"}],
            }
            state = {
                "message": "测试 resume aclose",
                "workdir": os.getcwd(),
                "tools_hint": "(无)",
                "skills_hint": "(无)",
                "memories_hint": "(无)",
                "graph_hint": "(无)",
                "context": {"last_llm_response": None},
                "observations": [],
                "step_order": 1,
                "paused": None,
                "model": "test-model",
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

        resp = stream_agent_command_resume(AgentCommandResumeStreamRequest(run_id=run_id, message="继续"))
        aiter = resp.body_iterator

        first = await aiter.__anext__()
        self.assertIsInstance(first, str)
        self.assertTrue(first.startswith("data: "))

        await aiter.aclose()

        for _ in range(40):
            with get_connection() as conn:
                run_row = conn.execute("SELECT status FROM task_runs WHERE id = ?", (run_id,)).fetchone()
                task_row = conn.execute("SELECT status FROM tasks WHERE id = ?", (task_id,)).fetchone()
            if run_row and task_row and run_row["status"] == "stopped" and task_row["status"] == "stopped":
                return
            await asyncio.sleep(0.05)
        self.fail("resume 断连后状态未收敛为 stopped")


if __name__ == "__main__":
    unittest.main()
