import json
import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    import httpx  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    httpx = None


class TestAgentCommandStreamDoMode(unittest.IsolatedAsyncioTestCase):
    """
    端到端回归：覆盖 /api/agent/command/stream（do 模式）的编排链路能跑通。

    说明：
    - 为了避免真实 LLM 依赖，检索与规划阶段用 patch 返回固定结果；
    - 执行阶段保留真实 ReAct 循环，但 patch create_llm_call/_execute_step_action 驱动步骤收敛；
    - 同时 patch 后处理线程入队，避免后台线程写库导致临时目录清理竞态。
    """

    def setUp(self):
        if httpx is None:
            self.skipTest("httpx 未安装，跳过需要 ASGI 客户端的测试")

        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = Path(self._tmpdir.name) / "agent_test.db"
        self._prompt_root = Path(self._tmpdir.name) / "prompt"

        os.environ["AGENT_DB_PATH"] = str(self._db_path)
        os.environ["AGENT_PROMPT_ROOT"] = str(self._prompt_root)
        os.makedirs(self._prompt_root, exist_ok=True)

        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
        except Exception:
            pass

        # 部分链路会 enqueue 后台线程（本测试已 patch 关闭，但仍保留兜底重试）
        for _ in range(10):
            try:
                self._tmpdir.cleanup()
                return
            except OSError:
                time.sleep(0.05)
        shutil.rmtree(self._tmpdir.name, ignore_errors=True)

    async def test_agent_command_stream_do_mode_runs(self):
        from backend.src.agent.planning_phase import PlanPhaseResult
        from backend.src.main import create_app
        from backend.src.storage import get_connection

        # 规划结果：2 步（确保不会追加“确认满意度”，因为 max_steps=2）
        plan_titles = [
            "file_write:README.md 写入文档",
            "task_output 输出结果",
        ]
        plan_items = [
            {"id": 1, "brief": "写文档", "status": "pending"},
            {"id": 2, "brief": "输出", "status": "pending"},
        ]
        plan_allows = [["file_write"], ["task_output"]]

        def _fake_run_planning_phase(*_args, **_kwargs):
            if False:  # pragma: no cover - make this a generator
                yield ""
            return PlanPhaseResult(
                plan_titles=list(plan_titles),
                plan_briefs=["写文档", "输出"],
                plan_allows=[list(a) for a in plan_allows],
                plan_artifacts=[],
                plan_items=[dict(it) for it in plan_items],
                plan_llm_id=1,
            )

        llm_actions = [
            {"action": {"type": "file_write", "payload": {"path": "README.md", "content": "doc"}}},
            {"action": {"type": "task_output", "payload": {"output_type": "text", "content": "ok"}}},
        ]
        llm_calls: list[dict] = []

        def _fake_create_llm_call(payload: dict):
            llm_calls.append(dict(payload))
            resp = json.dumps(llm_actions[len(llm_calls) - 1], ensure_ascii=False)
            return {"record": {"status": "success", "response": resp}}

        def _fake_execute_step_action(_task_id, _run_id, _step_row, context=None):
            return {"ok": True}, None

        app = create_app()
        transport = httpx.ASGITransport(app=app)

        with patch(
            "backend.src.agent.runner.stream_new_run._select_relevant_graph_nodes",
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_new_run._filter_relevant_domains",
            return_value=["misc"],
        ), patch(
            "backend.src.agent.runner.stream_new_run._select_relevant_skills",
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_new_run._select_relevant_solutions",
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_new_run._collect_tools_from_solutions",
            return_value="(无)",
        ), patch(
            "backend.src.agent.runner.stream_new_run.run_planning_phase",
            side_effect=_fake_run_planning_phase,
        ), patch(
            "backend.src.agent.runner.stream_new_run.enqueue_postprocess_thread",
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
                    "/api/agent/command/stream",
                    json={
                        "message": "test do stream",
                        "mode": "do",
                        "max_steps": 2,
                        "model": "base-model",
                        "parameters": {"temperature": 0},
                    },
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        # 断言：run 被创建并结束；agent_state.mode 为 do
        with get_connection() as conn:
            row = conn.execute(
                "SELECT status, agent_state, agent_plan FROM task_runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertIsNotNone(row)
        state = json.loads(row["agent_state"] or "{}")
        plan = json.loads(row["agent_plan"] or "{}")
        self.assertEqual(str(state.get("mode") or ""), "do")
        self.assertEqual(plan.get("titles"), plan_titles)
        self.assertEqual(len(llm_calls), 2)


if __name__ == "__main__":
    unittest.main()

