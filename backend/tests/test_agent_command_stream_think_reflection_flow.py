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


class TestAgentCommandStreamThinkReflectionFlow(unittest.IsolatedAsyncioTestCase):
    """
    端到端回归：覆盖 think runner 的“失败 → 反思 → 插入修复步骤 → 继续执行”编排分支。

    说明：
    - 本测试不走真实并行执行器，直接 patch stream_think_run.run_think_parallel_loop 返回可控结果：
      第一次失败（last_step_order=2），触发反思并插入 1 个修复步骤；第二次执行成功；
    - 断言 agent_state.reflection_records 写入、agent_plan.titles 被插入修复步骤、最终 run 状态收敛到 done。
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

    async def test_think_runner_reflection_inserts_fix_steps_and_continues(self):
        from backend.src.agent.retrieval import KnowledgeSufficiencyResult
        from backend.src.agent.think import ThinkPlanResult
        from backend.src.constants import RUN_STATUS_DONE, RUN_STATUS_FAILED
        from backend.src.agent.runner.think_parallel_loop import ThinkParallelLoopResult
        from backend.src.main import create_app
        from backend.src.storage import get_connection

        plan_titles = ["file_write:main.py 写代码", "shell_command:验证 运行测试", "task_output 输出结果"]
        plan_briefs = ["写代码", "验证", "输出"]
        plan_allows = [["file_write"], ["shell_command"], ["task_output"]]

        def _fake_run_think_planning_sync(*_args, **_kwargs):
            return ThinkPlanResult(
                plan_titles=list(plan_titles),
                plan_briefs=list(plan_briefs),
                plan_allows=[list(a) for a in plan_allows],
                plan_artifacts=[],
                winning_planner_id="planner_a",
            )

        # fake parallel exec：第一次失败在 step 2；反思插入 1 步修复后第二次成功（last_step_order=4）
        exec_calls = {"n": 0}

        def _fake_run_think_parallel_loop(*_args, **_kwargs):
            if False:  # pragma: no cover
                yield ""
            exec_calls["n"] += 1
            if exec_calls["n"] == 1:
                return ThinkParallelLoopResult(run_status=RUN_STATUS_FAILED, last_step_order=2)
            return ThinkParallelLoopResult(run_status=RUN_STATUS_DONE, last_step_order=4)

        # fake reflection：返回 1 个修复步骤（插入到失败步骤之后）
        class _FakeWinningAnalysis:
            def to_dict(self):
                return {"root_cause": "test", "confidence": 0.9}

        class _FakeReflectionResult:
            winning_analysis = _FakeWinningAnalysis()
            fix_steps = [{"title": "file_write:main.py 修复代码", "brief": "修复", "allow": ["file_write"]}]

        def _fake_run_reflection(*_args, **_kwargs):
            return _FakeReflectionResult()

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
            "backend.src.agent.runner.stream_think_run.run_think_parallel_loop",
            side_effect=_fake_run_think_parallel_loop,
        ), patch(
            "backend.src.agent.runner.stream_think_run.run_reflection",
            side_effect=_fake_run_reflection,
        ), patch(
            "backend.src.agent.runner.stream_think_run.enqueue_postprocess_thread",
            return_value=None,
        ), patch(
            "backend.src.agent.runner.stream_think_run.enqueue_review_on_feedback_waiting",
            return_value=None,
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream(
                    "POST",
                    "/api/agent/command/stream",
                    json={
                        "message": "test think reflection",
                        "mode": "think",
                        "max_steps": 3,  # 避免追加“确认满意度”尾巴，聚焦反思链路
                        "model": "base-model",
                        "parameters": {"temperature": 0},
                    },
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        self.assertEqual(exec_calls["n"], 2)

        with get_connection() as conn:
            row = conn.execute(
                "SELECT status, agent_state, agent_plan FROM task_runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertIsNotNone(row)

        state = json.loads(row["agent_state"] or "{}")
        plan = json.loads(row["agent_plan"] or "{}")

        # 最终应成功
        self.assertEqual(str(row["status"] or ""), RUN_STATUS_DONE)

        # 反思记录应写入
        records = state.get("reflection_records")
        self.assertIsInstance(records, list)
        self.assertGreaterEqual(len(records), 1)
        self.assertEqual(int(records[0].get("failed_step_order") or 0), 2)

        # plan_titles 应插入修复步骤（位于失败步骤之后）
        titles = plan.get("titles")
        self.assertIsInstance(titles, list)
        self.assertIn("file_write:main.py 修复代码", titles)
        self.assertEqual(titles[2], "file_write:main.py 修复代码")


if __name__ == "__main__":
    unittest.main()
