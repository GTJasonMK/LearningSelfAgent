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


class TestAgentCommandStreamDoPendingPlanningAskUserTwiceMessageAccumulates(unittest.IsolatedAsyncioTestCase):
    """
    回归：pending_planning 下出现“补充后仍不足 → 再次 ask_user”时：
    - 第 2 次 waiting 的 agent_state.message 不应丢失第一次用户补充
    - 第 2 次 resume 后应基于累计补充继续规划并完成
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

    async def test_do_pending_planning_ask_user_twice_message_accumulates(self):
        from backend.src.agent.planning_phase import PlanPhaseResult
        from backend.src.agent.retrieval import KnowledgeSufficiencyResult
        from backend.src.constants import RUN_STATUS_DONE, RUN_STATUS_WAITING
        from backend.src.main import create_app
        from backend.src.storage import get_connection

        base_message = "test ask_user twice"
        first_input = "第一轮补充：目标文件为 README.md"
        second_input = "第二轮补充：输出需要包含目录结构"

        # 1) new_run：ask_user -> waiting
        sufficiency_ask = KnowledgeSufficiencyResult(
            sufficient=False,
            reason="需要你补充关键约束",
            missing_knowledge="domain_knowledge",
            suggestion="ask_user",
            skill_count=0,
            graph_count=0,
            memory_count=0,
        )

        # 2) 第一次 resume：仍不足 -> 再次 ask_user -> waiting
        sufficiency_ask_again = KnowledgeSufficiencyResult(
            sufficient=False,
            reason="仍缺少输出格式约束",
            missing_knowledge="domain_knowledge",
            suggestion="ask_user",
            skill_count=0,
            graph_count=0,
            memory_count=0,
        )

        # 3) 第二次 resume：充分 -> proceed -> 完成
        sufficiency_ok = KnowledgeSufficiencyResult(
            sufficient=True,
            reason="ok",
            missing_knowledge="none",
            suggestion="proceed",
            skill_count=0,
            graph_count=0,
            memory_count=0,
        )

        plan_titles = ["file_write:README.md 写入文档", "task_output 输出结果"]
        plan_items = [
            {"id": 1, "brief": "写文档", "status": "pending"},
            {"id": 2, "brief": "输出", "status": "pending"},
        ]
        plan_allows = [["file_write"], ["task_output"]]

        def _fake_run_planning_phase(*_args, **_kwargs):
            if False:  # pragma: no cover
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
            "backend.src.agent.runner.execution_pipeline.enqueue_postprocess_thread",
            return_value=None,
        ), patch(
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
            "backend.src.agent.runner.stream_new_run._assess_knowledge_sufficiency",
            return_value=sufficiency_ask,
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream(
                    "POST",
                    "/api/agent/command/stream",
                    json={
                        "message": base_message,
                        "mode": "do",
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

        # 第一次 resume：仍然 ask_user（触发第 2 轮 waiting）
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
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_resume_run._collect_tools_from_solutions",
            return_value="(无)",
        ), patch(
            "backend.src.agent.runner.stream_resume_run._assess_knowledge_sufficiency",
            return_value=sufficiency_ask_again,
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream(
                    "POST",
                    "/api/agent/command/resume/stream",
                    json={"run_id": run_id, "message": first_input},
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        with get_connection() as conn:
            row_wait2 = conn.execute(
                "SELECT status, agent_state FROM task_runs WHERE id = ?",
                (int(run_id),),
            ).fetchone()
        self.assertIsNotNone(row_wait2)
        self.assertEqual(str(row_wait2["status"] or ""), RUN_STATUS_WAITING)
        state_wait2 = json.loads(row_wait2["agent_state"] or "{}")
        self.assertTrue(bool(state_wait2.get("pending_planning")))
        self.assertIsInstance(state_wait2.get("paused"), dict)
        # 第 2 轮 user_prompt 应在 step_order=2
        self.assertEqual(int(state_wait2.get("paused", {}).get("step_order") or 0), 2)
        # 关键断言：message 必须包含第一次补充，否则下一轮规划会丢失上下文
        self.assertIn(first_input, str(state_wait2.get("message") or ""))

        # 第二次 resume：充分 -> 规划+执行 -> done
        llm_calls.clear()
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
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_resume_run._collect_tools_from_solutions",
            return_value="(无)",
        ), patch(
            "backend.src.agent.runner.stream_resume_run._assess_knowledge_sufficiency",
            return_value=sufficiency_ok,
        ), patch(
            "backend.src.agent.runner.stream_resume_run.run_planning_phase",
            side_effect=_fake_run_planning_phase,
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
                    json={"run_id": run_id, "message": second_input},
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        with get_connection() as conn:
            row_done = conn.execute(
                "SELECT status, agent_state FROM task_runs WHERE id = ?",
                (int(run_id),),
            ).fetchone()
        self.assertIsNotNone(row_done)
        self.assertEqual(str(row_done["status"] or ""), RUN_STATUS_DONE)
        state_done = json.loads(row_done["agent_state"] or "{}")
        self.assertFalse(bool(state_done.get("pending_planning")))
        msg_done = str(state_done.get("message") or "")
        self.assertIn(first_input, msg_done)
        self.assertIn(second_input, msg_done)


if __name__ == "__main__":
    unittest.main()

