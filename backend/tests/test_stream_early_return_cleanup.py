import json
import os
import shutil
import tempfile
import time
import unittest
import importlib.util
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


class TestStreamEarlyReturnCleanup(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        if importlib.util.find_spec("pydantic") is None:
            self.skipTest("pydantic 未安装，跳过流式清理测试")
        if importlib.util.find_spec("fastapi") is None:
            self.skipTest("fastapi 未安装，跳过流式清理测试")
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_test.db")
        os.environ["AGENT_DB_PATH"] = self._db_path
        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
        except Exception:
            pass
        for _ in range(10):
            try:
                self._tmpdir.cleanup()
                break
            except OSError:
                time.sleep(0.05)
        else:
            shutil.rmtree(self._tmpdir.name, ignore_errors=True)

    async def test_stream_new_run_releases_queue_ticket_on_pending_wait_return(self):
        from backend.src.agent.core.run_context import AgentRunContext
        from backend.src.agent.runner.stream_new_run import stream_agent_command
        from backend.src.api.schemas import AgentCommandStreamRequest

        run_ctx = AgentRunContext.from_agent_state(
            {},
            mode="do",
            message="m",
            model="test-model",
            parameters={"temperature": 0},
            max_steps=6,
            workdir=os.getcwd(),
        )
        started = SimpleNamespace(task_id=1, run_id=1, run_ctx=run_ctx, events=[])
        fake_ticket = SimpleNamespace(release=AsyncMock())

        async def _fake_iter_stream_task_events(*_args, **_kwargs):
            yield (
                "done",
                {
                    "graph_nodes": [],
                    "graph_hint": "(无)",
                    "domain_ids": ["misc"],
                    "skills": [],
                    "skills_hint": "(无)",
                    "solutions": [],
                },
            )

        async def _fake_iter_planning_enrich_events(*_args, **_kwargs):
            yield (
                "done",
                {
                    "skills": [],
                    "skills_hint": "(无)",
                    "solutions_for_prompt": [],
                    "draft_solution_id": None,
                    "solutions_hint": "(无)",
                    "tools_hint": "(无)",
                    "need_user_prompt": True,
                    "user_prompt_question": "请补充约束",
                },
            )

        async def _fake_iter_pending_planning_wait_events(*_args, **_kwargs):
            yield ("done", {"status": "waiting"})

        with patch(
            "backend.src.agent.runner.stream_new_run.start_new_mode_run",
            new=AsyncMock(return_value=started),
        ), patch(
            "backend.src.agent.runner.stream_new_run.acquire_stream_queue_ticket",
            new=AsyncMock(return_value=fake_ticket),
        ), patch(
            "backend.src.agent.runner.stream_new_run.iter_stream_task_events",
            side_effect=_fake_iter_stream_task_events,
        ), patch(
            "backend.src.agent.runner.stream_new_run.iter_planning_enrich_events",
            side_effect=_fake_iter_planning_enrich_events,
        ), patch(
            "backend.src.agent.runner.stream_new_run.iter_pending_planning_wait_events",
            side_effect=_fake_iter_pending_planning_wait_events,
        ):
            resp = stream_agent_command(AgentCommandStreamRequest(message="hi", mode="do"))
            chunks = [chunk async for chunk in resp.body_iterator]
            self.assertTrue(isinstance(chunks, list))

        self.assertEqual(1, int(fake_ticket.release.await_count))

    async def test_stream_think_run_releases_queue_ticket_on_pending_wait_return(self):
        from backend.src.agent.core.run_context import AgentRunContext
        from backend.src.agent.runner.stream_think_run import stream_agent_think_command
        from backend.src.api.schemas import AgentCommandStreamRequest

        run_ctx = AgentRunContext.from_agent_state(
            {},
            mode="think",
            message="m",
            model="test-model",
            parameters={"temperature": 0},
            max_steps=6,
            workdir=os.getcwd(),
        )
        started = SimpleNamespace(task_id=2, run_id=2, run_ctx=run_ctx, events=[])
        fake_ticket = SimpleNamespace(release=AsyncMock())

        async def _fake_iter_think_retrieval_merge_events(*_args, **_kwargs):
            yield (
                "done",
                {
                    "graph_nodes": [],
                    "graph_hint": "(无)",
                    "memories_hint": "(无)",
                    "domain_ids": ["misc"],
                    "skills": [],
                    "skills_hint": "(无)",
                    "solutions": [],
                    "solutions_hint": "(无)",
                    "tools_hint": "(无)",
                    "draft_solution_id": None,
                    "planner_hints": {},
                    "need_user_prompt": True,
                    "user_prompt_question": "请补充约束",
                },
            )

        async def _fake_iter_pending_planning_wait_events(*_args, **_kwargs):
            yield ("done", {"status": "waiting"})

        with patch(
            "backend.src.agent.runner.stream_think_run.start_new_mode_run",
            new=AsyncMock(return_value=started),
        ), patch(
            "backend.src.agent.runner.stream_think_run.acquire_stream_queue_ticket",
            new=AsyncMock(return_value=fake_ticket),
        ), patch(
            "backend.src.agent.runner.stream_think_run.iter_think_retrieval_merge_events",
            side_effect=_fake_iter_think_retrieval_merge_events,
        ), patch(
            "backend.src.agent.runner.stream_think_run.iter_pending_planning_wait_events",
            side_effect=_fake_iter_pending_planning_wait_events,
        ):
            resp = stream_agent_think_command(AgentCommandStreamRequest(message="hi", mode="think"))
            chunks = [chunk async for chunk in resp.body_iterator]
            self.assertTrue(isinstance(chunks, list))

        self.assertEqual(1, int(fake_ticket.release.await_count))

    async def test_stream_resume_run_releases_queue_ticket_on_pending_wait_return(self):
        from backend.src.agent.runner.stream_resume_run import stream_agent_command_resume
        from backend.src.api.schemas import AgentCommandResumeStreamRequest
        from backend.src.constants import RUN_STATUS_WAITING

        class _NoGetRow:
            """模拟 sqlite3.Row：支持下标访问，但不提供 dict.get。"""

            def __init__(self, data):
                self._data = dict(data or {})

            def __getitem__(self, key):
                return self._data[key]

            def __bool__(self):
                return True

        plan_payload = {
            "titles": ["user_prompt:补充信息"],
            "allows": [["user_prompt"]],
            "artifacts": [],
            "items": [{"id": 1, "brief": "补充", "status": "pending"}],
        }
        state_payload = {
            "mode": "do",
            "message": "m",
            "workdir": os.getcwd(),
            "model": "test-model",
            "parameters": {"temperature": 0},
            "max_steps": 6,
            "step_order": 1,
            "paused": {"step_order": 1, "question": "请补充"},
            "pending_planning": True,
            "tools_hint": "(无)",
            "skills_hint": "(无)",
            "solutions_hint": "(无)",
            "graph_hint": "(无)",
            "context": {},
            "observations": [],
        }
        run_row = {
            "id": 3,
            "task_id": 10,
            "status": RUN_STATUS_WAITING,
            "agent_plan": json.dumps(plan_payload, ensure_ascii=False),
            "agent_state": json.dumps(state_payload, ensure_ascii=False),
            "created_at": "",
        }
        fake_ticket = SimpleNamespace(release=AsyncMock())

        async def _fake_apply_resume_user_input(**kwargs):
            return int(kwargs.get("resume_step_order") or 1), dict(kwargs.get("state_obj") or {})

        async def _fake_iter_stream_task_events(*_args, **_kwargs):
            yield ("done", {"outcome": "waiting"})

        with patch(
            "backend.src.agent.runner.stream_resume_run.get_task_run",
            return_value=_NoGetRow(run_row),
        ), patch(
            "backend.src.agent.runner.stream_resume_run.get_task",
            return_value={"title": "resume test task"},
        ), patch(
            "backend.src.agent.runner.stream_resume_run.get_max_step_order_for_run_by_status",
            return_value=0,
        ), patch(
            "backend.src.agent.runner.stream_resume_run.get_last_non_planned_step_for_run",
            return_value=None,
        ), patch(
            "backend.src.agent.runner.stream_resume_run.acquire_stream_queue_ticket",
            new=AsyncMock(return_value=fake_ticket),
        ), patch(
            "backend.src.agent.runner.stream_resume_run.apply_resume_user_input",
            side_effect=_fake_apply_resume_user_input,
        ), patch(
            "backend.src.agent.runner.stream_resume_run.iter_stream_task_events",
            side_effect=_fake_iter_stream_task_events,
        ), patch(
            "backend.src.agent.runner.stream_resume_run._select_relevant_graph_nodes",
            return_value=[],
        ):
            resp = stream_agent_command_resume(AgentCommandResumeStreamRequest(run_id=3, message="继续"))
            chunks = [chunk async for chunk in resp.body_iterator]
            self.assertTrue(isinstance(chunks, list))

        self.assertEqual(1, int(fake_ticket.release.await_count))


if __name__ == "__main__":
    unittest.main()
