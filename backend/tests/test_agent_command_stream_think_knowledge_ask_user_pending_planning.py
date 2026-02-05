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


class TestAgentCommandStreamThinkKnowledgeAskUserPendingPlanning(unittest.IsolatedAsyncioTestCase):
    """
    回归：think 模式知识充分性建议 ask_user 时：
    - new_run 进入 waiting（pending_planning=True，mode=think）
    - resume 后自动重新检索+重新规划（Think）并继续执行收敛为 done
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

    async def test_think_stream_waiting_then_resume_replans_and_finishes(self):
        from backend.src.agent.retrieval import KnowledgeSufficiencyResult
        from backend.src.agent.think import ThinkPlanResult
        from backend.src.agent.runner.think_parallel_loop import ThinkParallelLoopResult
        from backend.src.constants import RUN_STATUS_DONE, RUN_STATUS_WAITING
        from backend.src.main import create_app
        from backend.src.storage import get_connection

        sufficiency_ask = KnowledgeSufficiencyResult(
            sufficient=False,
            reason="需要你补充关键约束",
            missing_knowledge="domain_knowledge",
            suggestion="ask_user",
            skill_count=0,
            graph_count=0,
            memory_count=0,
        )

        sufficiency_ok = KnowledgeSufficiencyResult(
            sufficient=True,
            reason="ok",
            missing_knowledge="none",
            suggestion="proceed",
            skill_count=0,
            graph_count=0,
            memory_count=0,
        )

        def _fake_run_think_planning_sync(*_args, **_kwargs):
            # 计划长度=2，配合 max_steps=6 + append_task_feedback_step(no-op) 保持可控
            return ThinkPlanResult(
                plan_titles=["file_write:README.md 写入文档", "task_output 输出结果"],
                plan_briefs=["写文档", "输出"],
                plan_allows=[["file_write"], ["task_output"]],
                plan_artifacts=[],
                winning_planner_id="planner_a",
                alternative_plans=[],
                vote_records=[],
            )

        def _fake_run_think_parallel_loop(*_args, **_kwargs):
            if False:  # pragma: no cover
                yield ""
            return ThinkParallelLoopResult(run_status=RUN_STATUS_DONE, last_step_order=3)

        app = create_app()
        transport = httpx.ASGITransport(app=app)

        # 1) new_run：ask_user -> waiting
        with patch(
            "backend.src.agent.runner.execution_pipeline.enqueue_postprocess_thread",
            return_value=None,
        ), patch(
            "backend.src.agent.runner.stream_think_run._select_relevant_graph_nodes",
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_think_run._filter_relevant_domains",
            return_value=["misc"],
        ), patch(
            "backend.src.agent.runner.stream_think_run._select_relevant_skills",
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_think_run._select_relevant_solutions",
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_think_run._collect_tools_from_solutions",
            return_value="(无)",
        ), patch(
            "backend.src.agent.runner.stream_think_run._assess_knowledge_sufficiency",
            return_value=sufficiency_ask,
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream(
                    "POST",
                    "/api/agent/command/stream",
                    json={
                        "message": "test think ask_user",
                        "mode": "think",
                        "max_steps": 6,
                        "model": "base-model",
                        "parameters": {"temperature": 0},
                    },
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        with get_connection() as conn:
            run_row = conn.execute(
                "SELECT id, status, agent_state, agent_plan FROM task_runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertIsNotNone(run_row)
        run_id = int(run_row["id"])
        self.assertEqual(str(run_row["status"] or ""), RUN_STATUS_WAITING)

        state = json.loads(run_row["agent_state"] or "{}")
        plan = json.loads(run_row["agent_plan"] or "{}")
        self.assertEqual(str(state.get("mode") or ""), "think")
        self.assertTrue(bool(state.get("pending_planning")))
        self.assertIsInstance(state.get("paused"), dict)
        self.assertEqual(int(state.get("paused", {}).get("step_order") or 0), 1)
        self.assertEqual(len(plan.get("titles") or []), 1)

        # 2) resume：应重新检索 + Think 重新规划 + 继续执行并收敛 done
        with patch(
            "backend.src.agent.runner.stream_resume_run.enqueue_postprocess_thread",
            return_value=None,
        ), patch(
            "backend.src.agent.runner.stream_resume_run.append_task_feedback_step",
            return_value=False,
        ), patch(
            "backend.src.agent.runner.stream_resume_run._select_relevant_graph_nodes",
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_resume_run._filter_relevant_domains",
            return_value=["misc"],
        ), patch(
            "backend.src.agent.runner.stream_resume_run._select_relevant_skills",
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_resume_run._select_relevant_solutions",
            return_value=[{"id": 21, "name": "已有方案", "description": "d", "steps": []}],
        ), patch(
            "backend.src.agent.runner.stream_resume_run._collect_tools_from_solutions",
            return_value="(无)",
        ), patch(
            "backend.src.agent.runner.stream_resume_run._assess_knowledge_sufficiency",
            return_value=sufficiency_ok,
        ), patch(
            "backend.src.agent.runner.stream_resume_run.run_think_planning_sync",
            side_effect=_fake_run_think_planning_sync,
        ), patch(
            "backend.src.agent.runner.stream_resume_run.run_think_parallel_loop",
            side_effect=_fake_run_think_parallel_loop,
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream(
                    "POST",
                    "/api/agent/command/resume/stream",
                    json={"run_id": run_id, "message": "补充：目标产出为 README.md"},
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        with get_connection() as conn:
            row = conn.execute(
                "SELECT status, agent_state, agent_plan FROM task_runs WHERE id = ?",
                (int(run_id),),
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(str(row["status"] or ""), RUN_STATUS_DONE)

        state2 = json.loads(row["agent_state"] or "{}")
        self.assertEqual(str(state2.get("mode") or ""), "think")
        self.assertFalse(bool(state2.get("pending_planning")))

        plan2 = json.loads(row["agent_plan"] or "{}")
        titles2 = plan2.get("titles") or []
        self.assertGreaterEqual(len(titles2), 3)
        self.assertTrue(str(titles2[0] or "").startswith("user_prompt:"))


if __name__ == "__main__":
    unittest.main()

