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


class TestAgentCommandStreamThinkConfigFallbacks(unittest.IsolatedAsyncioTestCase):
    """
    回归：think_config 为“部分覆盖”时，Think 主链路应具备兜底能力而非直接不可运行。

    覆盖点：
    - think_config 未包含 planner_* 时，stream_think_run 应自动补齐默认 planners
    - think_config 未配置 evaluator 时，evaluator_model 默认沿用 base model（state.model）
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

    async def test_think_stream_fills_default_planners_when_missing(self):
        from backend.src.agent.retrieval import KnowledgeSufficiencyResult
        from backend.src.agent.think import ThinkPlanResult
        from backend.src.main import create_app

        def _fake_run_think_planning_sync(*_args, **kwargs):
            cfg = kwargs.get("config")
            self.assertIsNotNone(cfg)
            planners = getattr(cfg, "planners", None)
            self.assertTrue(planners, "未配置 planner_* 时应补齐默认 planners")
            self.assertEqual(getattr(cfg, "evaluator_model", ""), "base-model")
            return ThinkPlanResult(
                plan_titles=["task_output 输出结果"],
                plan_briefs=["输出"],
                plan_allows=[["task_output"]],
                plan_artifacts=[],
                winning_planner_id="planner_a",
            )

        llm_action = {"action": {"type": "task_output", "payload": {"output_type": "text", "content": "ok"}}}

        def _fake_create_llm_call(_payload: dict):
            return {"record": {"status": "success", "response": json.dumps(llm_action, ensure_ascii=False)}}

        def _fake_execute_step_action(_task_id, _run_id, _step_row, context=None):
            return {"ok": True}, None

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

        with patch(
            "backend.src.agent.runner.stream_think_run._select_relevant_graph_nodes",
            return_value=[],
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
                        "message": "test think_config fallbacks",
                        "mode": "think",
                        # max_steps=1：避免追加“确认满意度”，让用例聚焦于 think_config 兜底
                        "max_steps": 1,
                        "model": "base-model",
                        "parameters": {"temperature": 0},
                        # 未提供 planner_* 与 evaluator：应自动补齐 planners 且 evaluator 默认 base-model
                        "think_config": {"agents": {"executor_code": "code-model"}},
                    },
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()


if __name__ == "__main__":
    unittest.main()
