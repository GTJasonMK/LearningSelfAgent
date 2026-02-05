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


class TestAgentCommandStreamThinkRetrievalPartialFailure(unittest.IsolatedAsyncioTestCase):
    """
    回归：Think 多模型检索阶段，单个 Planner 失败不应导致整体失败。

    覆盖点：
    - graph/domains/skills/solutions 的并行 gather 允许部分异常并降级为空结果；
    - 仍能进入规划与执行阶段并正常收敛为 done。
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

        for _ in range(10):
            try:
                self._tmpdir.cleanup()
                return
            except OSError:
                time.sleep(0.05)
        shutil.rmtree(self._tmpdir.name, ignore_errors=True)

    async def test_think_stream_ignores_planner_retrieval_failure(self):
        from backend.src.agent.retrieval import KnowledgeSufficiencyResult
        from backend.src.agent.think import ThinkPlanResult
        from backend.src.main import create_app
        from backend.src.storage import get_connection

        plan_titles = [
            "file_write:README.md 写文档",
            "task_output 输出结果",
        ]
        plan_briefs = ["写文档", "输出"]
        plan_allows = [["file_write"], ["task_output"]]

        def _fake_run_think_planning_sync(*_args, **_kwargs):
            return ThinkPlanResult(
                plan_titles=list(plan_titles),
                plan_briefs=list(plan_briefs),
                plan_allows=[list(a) for a in plan_allows],
                plan_artifacts=[],
                winning_planner_id="planner_a",
            )

        llm_actions = [
            {"action": {"type": "file_write", "payload": {"path": "README.md", "content": "doc"}}},
            {"action": {"type": "task_output", "payload": {"output_type": "text", "content": "ok"}}},
        ]
        models: list[str] = []

        def _fake_create_llm_call(payload: dict):
            models.append(str(payload.get("model") or ""))
            resp = json.dumps(llm_actions[len(models) - 1], ensure_ascii=False)
            return {"record": {"status": "success", "response": resp}}

        def _fake_execute_step_action(_task_id, _run_id, _step_row, context=None):
            return {"ok": True}, None

        def _fake_select_relevant_graph_nodes(*, message: str, model: str, parameters: dict):
            if str(model) == "bad-model":
                raise RuntimeError("planner graph failed")
            return []

        sufficiency = KnowledgeSufficiencyResult(
            sufficient=True,
            reason="ok",
            missing_knowledge="none",
            suggestion="proceed",
            skill_count=0,
            graph_count=0,
            memory_count=0,
        )

        app = create_app()
        transport = httpx.ASGITransport(app=app)

        think_config = {
            "agents": {
                "planner_a": "ok-model",
                "planner_b": "bad-model",
                "executor_doc": "doc-model",
                "executor_code": "code-model",
                "executor_test": "test-model",
                "evaluator": "eval-model",
            }
        }

        with patch(
            "backend.src.agent.runner.stream_think_run._select_relevant_graph_nodes",
            side_effect=_fake_select_relevant_graph_nodes,
        ), patch(
            "backend.src.agent.runner.stream_think_run._filter_relevant_domains",
            return_value=["misc"],
        ), patch(
            "backend.src.agent.runner.stream_think_run._select_relevant_skills",
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_think_run._assess_knowledge_sufficiency",
            return_value=sufficiency,
        ), patch(
            "backend.src.agent.runner.stream_think_run._select_relevant_solutions",
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_think_run._collect_tools_from_solutions",
            return_value="(无)",
        ), patch(
            "backend.src.agent.runner.stream_think_run.run_think_planning_sync",
            side_effect=_fake_run_think_planning_sync,
        ), patch(
            "backend.src.agent.runner.stream_think_run.enqueue_postprocess_thread",
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
                        "message": "test think retrieval partial failure",
                        "mode": "think",
                        "max_steps": 2,  # len(plan)=2，避免追加“确认满意度”
                        "model": "base-model",
                        "parameters": {"temperature": 0},
                        "think_config": think_config,
                    },
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        # 断言：检索阶段未导致整体失败，执行阶段按 executor 选模型
        self.assertEqual(models, ["doc-model", "code-model"])

        with get_connection() as conn:
            row = conn.execute(
                "SELECT status, agent_state FROM task_runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(str(row["status"] or ""), "done")
        state = json.loads(row["agent_state"] or "{}")
        self.assertEqual(str(state.get("mode") or ""), "think")


if __name__ == "__main__":
    unittest.main()
