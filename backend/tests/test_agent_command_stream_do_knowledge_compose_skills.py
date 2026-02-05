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


class TestAgentCommandStreamDoKnowledgeComposeSkills(unittest.IsolatedAsyncioTestCase):
    """
    回归：do 模式知识充分性不足时触发 compose_skills 分支，仍能继续规划与执行收敛。

    覆盖点：
    - _assess_knowledge_sufficiency -> suggestion=compose_skills
    - _compose_skills 成功后创建 draft 技能并注入 skills_hint
    - 继续走 solutions/planning/react 并最终收敛为 done
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

    async def test_do_stream_compose_skills_branch_runs(self):
        from backend.src.agent.planning_phase import PlanPhaseResult
        from backend.src.agent.retrieval import ComposedSkillResult, KnowledgeSufficiencyResult
        from backend.src.main import create_app
        from backend.src.storage import get_connection

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

        original_skills = [{"id": 11, "name": "原始技能", "description": "d", "steps": ["a"], "domain_id": "misc"}]

        sufficiency = KnowledgeSufficiencyResult(
            sufficient=False,
            reason="test insufficient",
            missing_knowledge="skill",
            suggestion="compose_skills",
            skill_count=1,
            graph_count=0,
            memory_count=0,
        )

        composed = ComposedSkillResult(
            success=True,
            name="组合技能",
            description="desc",
            steps=["s1", "s2"],
            source_skill_ids=[11],
            domain_id="misc",
            error=None,
        )

        created_skill_ids: list[int] = []

        def _fake_create_skill(_params):
            created_skill_ids.append(123)
            return 123

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
            return_value=list(original_skills),
        ), patch(
            "backend.src.agent.runner.stream_new_run._assess_knowledge_sufficiency",
            return_value=sufficiency,
        ), patch(
            "backend.src.agent.runner.stream_new_run._compose_skills",
            return_value=composed,
        ), patch(
            "backend.src.agent.runner.stream_new_run.create_skill",
            side_effect=_fake_create_skill,
        ), patch(
            "backend.src.agent.runner.stream_new_run.publish_skill_file",
            return_value=("skills/misc/组合技能.md", None),
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
                        "message": "test do compose skills",
                        "mode": "do",
                        "max_steps": 2,  # len(plan)=2，避免追加“确认满意度”
                        "model": "base-model",
                        "parameters": {"temperature": 0},
                    },
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        self.assertEqual(created_skill_ids, [123])

        with get_connection() as conn:
            row = conn.execute("SELECT status, agent_state FROM task_runs ORDER BY id DESC LIMIT 1").fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(str(row["status"] or ""), "done")
        state = json.loads(row["agent_state"] or "{}")
        self.assertEqual(str(state.get("mode") or ""), "do")
        # skills 列表应包含新创建的 draft skill id
        skill_ids = state.get("skill_ids")
        self.assertIsInstance(skill_ids, list)
        self.assertIn(11, skill_ids)
        self.assertIn(123, skill_ids)


if __name__ == "__main__":
    unittest.main()

