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


class TestAgentPlanningMaxStepsUnlimited(unittest.IsolatedAsyncioTestCase):
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

    async def test_do_mode_ignores_small_max_steps_for_planning_phase(self):
        from backend.src.agent.planning_phase import PlanPhaseResult
        from backend.src.constants import AGENT_MAX_STEPS_UNLIMITED
        from backend.src.main import create_app

        captured = {"max_steps": None}

        def _fake_run_planning_phase(*_args, **kwargs):
            captured["max_steps"] = kwargs.get("max_steps")
            if False:  # pragma: no cover
                yield ""
            return PlanPhaseResult(
                plan_titles=["task_output 输出结果"],
                plan_briefs=["输出"],
                plan_allows=[["task_output"]],
                plan_artifacts=[],
                plan_items=[{"id": 1, "brief": "输出", "status": "pending"}],
                plan_llm_id=1,
            )

        llm_actions = [
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
                        "message": "test unlimited planning max steps",
                        "mode": "do",
                        "max_steps": 1,
                        "model": "base-model",
                        "parameters": {"temperature": 0},
                    },
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        self.assertEqual(int(captured["max_steps"]), int(AGENT_MAX_STEPS_UNLIMITED))


if __name__ == "__main__":
    unittest.main()
