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


class TestAgentCommandStreamThinkDraftSolution(unittest.IsolatedAsyncioTestCase):
    """
    回归：Think 模式“有技能但无匹配方案”时会草拟 draft 方案，并且草稿方案会下发给所有 Planner（不被 '(无)' 覆盖）。

    覆盖点：
    - _select_relevant_solutions -> []
    - _draft_solution_from_skills 成功后创建 draft solution 并落盘
    - run_think_planning_sync 收到的 planner_hints[*].solutions_hint 包含草稿方案
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

    async def test_think_mode_draft_solution_dispatched_to_all_planners(self):
        from backend.src.agent.retrieval import DraftSolutionResult, KnowledgeSufficiencyResult
        from backend.src.agent.think import ThinkPlanResult
        from backend.src.main import create_app
        from backend.src.storage import get_connection

        original_skills = [{"id": 11, "name": "原始技能", "description": "d", "steps": ["a"], "domain_id": "misc"}]
        sufficiency = KnowledgeSufficiencyResult(
            sufficient=True,
            reason="ok",
            missing_knowledge="none",
            suggestion="proceed",
            skill_count=1,
            graph_count=0,
            memory_count=0,
        )

        draft_solution = DraftSolutionResult(
            success=True,
            name="草拟方案",
            description="desc",
            steps=[{"title": "task_output 输出结果", "allow": ["task_output"]}],
            artifacts=["README.md"],
            tool_names=["fake_tool"],
            error=None,
        )

        created_skill_ids: list[int] = []

        def _fake_create_skill(_params):
            created_skill_ids.append(789)
            return 789

        captured_planner_hints: list[dict] = []

        def _fake_run_think_planning_sync(*_args, **kwargs):
            captured_planner_hints.append(dict(kwargs.get("planner_hints") or {}))
            # 计划长度=2，配合 max_steps=2 避免追加“确认满意度”
            return ThinkPlanResult(
                plan_titles=["file_write:README.md 写入文档", "task_output 输出结果"],
                plan_briefs=["写文档", "输出"],
                plan_allows=[["file_write"], ["task_output"]],
                plan_artifacts=[],
                winning_planner_id="planner_a",
                alternative_plans=[],
                vote_records=[],
            )

        think_config = {
            "agents": {
                "planner_a": "planner-model",
                "planner_b": "planner-model",
                "planner_c": "planner-model",
                "executor_doc": "doc-model",
                "executor_code": "code-model",
                "executor_test": "test-model",
                "evaluator": "eval-model",
            }
        }

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
            return_value=list(original_skills),
        ), patch(
            "backend.src.agent.runner.stream_think_run._assess_knowledge_sufficiency",
            return_value=sufficiency,
        ), patch(
            "backend.src.agent.runner.stream_think_run._select_relevant_solutions",
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_think_run._draft_solution_from_skills",
            return_value=draft_solution,
        ), patch(
            "backend.src.agent.runner.stream_think_run.create_skill",
            side_effect=_fake_create_skill,
        ), patch(
            "backend.src.agent.runner.stream_think_run.publish_skill_file",
            return_value=("skills/misc/草拟方案.md", None),
        ), patch(
            "backend.src.agent.runner.stream_think_run._collect_tools_from_solutions",
            return_value="(无)",
        ), patch(
            "backend.src.agent.runner.stream_think_run.run_think_planning_sync",
            side_effect=_fake_run_think_planning_sync,
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream(
                    "POST",
                    "/api/agent/command/stream",
                    json={
                        "message": "test think draft solution",
                        "mode": "think",
                        "dry_run": True,
                        "max_steps": 2,
                        "model": "base-model",
                        "parameters": {"temperature": 0},
                        "think_config": think_config,
                    },
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        self.assertEqual(created_skill_ids, [789])
        self.assertTrue(captured_planner_hints)
        ph = captured_planner_hints[-1]
        # 所有 planner 的 solutions_hint 都应包含草稿方案（不被 '(无)' 覆盖）
        for pid in ["planner_a", "planner_b", "planner_c"]:
            slot = ph.get(pid) or {}
            self.assertIn("草拟方案", str(slot.get("solutions_hint") or ""))

        with get_connection() as conn:
            row = conn.execute(
                "SELECT status, agent_state FROM task_runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(str(row["status"] or ""), "done")
        state = json.loads(row["agent_state"] or "{}")
        solution_ids = state.get("solution_ids")
        self.assertIsInstance(solution_ids, list)
        self.assertIn(789, solution_ids)
        self.assertEqual(int(state.get("draft_solution_id") or 0), 789)


if __name__ == "__main__":
    unittest.main()
