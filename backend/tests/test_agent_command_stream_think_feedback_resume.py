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


class TestAgentCommandStreamThinkFeedbackResume(unittest.IsolatedAsyncioTestCase):
    """
    端到端回归：覆盖 think 模式“确认满意度”等待→resume→结束的编排闭环。

    覆盖点：
    - /api/agent/command/stream（think）执行完成后追加“确认满意度”并进入 waiting（paused 写入 agent_state）
    - /api/agent/command/resume/stream 注入用户回答后，继续处理反馈并把 run 收敛到 done
    - Think 执行阶段按 executor 角色选模型（step_llm_config_resolver 生效）
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

    async def test_think_stream_waits_for_feedback_and_resume_finishes(self):
        from backend.src.agent.retrieval import KnowledgeSufficiencyResult
        from backend.src.agent.think import ThinkPlanResult
        from backend.src.constants import AGENT_TASK_FEEDBACK_STEP_TITLE, RUN_STATUS_DONE, RUN_STATUS_WAITING
        from backend.src.main import create_app
        from backend.src.storage import get_connection

        # 规划输出 2 步：file_write + task_output；stream_think_run 会自动追加“确认满意度”
        plan_titles = ["file_write:README.md 写文档", "task_output 输出结果"]
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

        # 两步执行后应进入 waiting（确认满意度不需要 LLM）
        llm_actions = [
            {"action": {"type": "file_write", "payload": {"path": "README.md", "content": "doc"}}},
            {"action": {"type": "task_output", "payload": {"output_type": "text", "content": "draft"}}},
        ]
        models: list[str] = []

        def _fake_create_llm_call(payload: dict):
            models.append(str(payload.get("model") or ""))
            resp = json.dumps(llm_actions[len(models) - 1], ensure_ascii=False)
            return {"record": {"status": "success", "response": resp}}

        def _fake_execute_step_action(_task_id, _run_id, _step_row, context=None):
            return {"ok": True}, None

        # 评估门闩：返回 pass，避免插入修复步骤
        def _fake_ensure_review(*_args, **_kwargs):
            return 1

        def _fake_get_review(_review_id: int):
            return {"status": "pass", "summary": "ok", "issues": "[]", "next_actions": "[]"}

        sufficiency = KnowledgeSufficiencyResult(
            sufficient=True,
            reason="ok",
            missing_knowledge="none",
            suggestion="proceed",
            skill_count=0,
            graph_count=0,
            memory_count=0,
        )

        # 让 executor_doc/executor_code 使用不同模型，便于断言 resolver 是否生效
        think_config = {
            "agents": {
                "planner_a": "planner-model",
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
            "backend.src.agent.runner.stream_think_run.enqueue_review_on_feedback_waiting",
            return_value=None,
        ), patch(
            "backend.src.services.tasks.task_postprocess.ensure_agent_review_record",
            side_effect=_fake_ensure_review,
        ), patch(
            "backend.src.repositories.agent_reviews_repo.get_agent_review",
            side_effect=_fake_get_review,
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
                        "message": "test think feedback waiting",
                        "mode": "think",
                        "max_steps": 4,  # 2 步 + 确认满意度
                        "model": "base-model",
                        "parameters": {"temperature": 0},
                        "think_config": think_config,
                    },
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        # 断言：step_llm_config_resolver 按 executor 选模型（doc → code）
        self.assertEqual(models, ["doc-model", "code-model"])

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
        self.assertEqual(plan.get("titles"), [*plan_titles, AGENT_TASK_FEEDBACK_STEP_TITLE])
        self.assertIsInstance(state.get("paused"), dict)
        self.assertEqual(int(state.get("paused", {}).get("step_order") or 0), 3)
        self.assertTrue(bool(state.get("task_feedback_asked")))

        # resume：输入“满意”，应收敛到 done（不应再次调用 LLM）
        with patch(
            "backend.src.agent.runner.stream_resume_run.enqueue_postprocess_thread",
            return_value=None,
        ), patch(
            "backend.src.agent.runner.stream_resume_run.enqueue_review_on_feedback_waiting",
            return_value=None,
        ), patch(
            "backend.src.services.tasks.task_postprocess.ensure_agent_review_record",
            side_effect=_fake_ensure_review,
        ), patch(
            "backend.src.repositories.agent_reviews_repo.get_agent_review",
            side_effect=_fake_get_review,
        ), patch(
            "backend.src.services.tasks.task_postprocess.write_task_result_memory_if_missing",
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
                    json={"run_id": run_id, "message": "满意"},
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        self.assertEqual(models, ["doc-model", "code-model"])

        with get_connection() as conn:
            row = conn.execute("SELECT status FROM task_runs WHERE id = ?", (int(run_id),)).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(str(row["status"] or ""), RUN_STATUS_DONE)


if __name__ == "__main__":
    unittest.main()
