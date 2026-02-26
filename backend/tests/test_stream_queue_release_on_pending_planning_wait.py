import json
import importlib.util
import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

try:
    import httpx  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    httpx = None

HAS_FASTAPI = importlib.util.find_spec("fastapi") is not None


class _FakeQueueTicket:
    def __init__(self) -> None:
        self.release_calls = 0

    async def release(self) -> None:
        self.release_calls += 1


class TestStreamQueueReleaseOnPendingPlanningWait(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        if httpx is None:
            self.skipTest("httpx 未安装，跳过需要 ASGI 客户端的测试")
        if not HAS_FASTAPI:
            self.skipTest("fastapi 未安装，跳过需要 ASGI 客户端的测试")

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

    async def _run_new_do_waiting(self):
        from backend.src.agent.retrieval import KnowledgeSufficiencyResult
        from backend.src.main import create_app
        from backend.src.storage import get_connection

        sufficiency_ask = KnowledgeSufficiencyResult(
            sufficient=False,
            reason="需要补充约束",
            missing_knowledge="domain_knowledge",
            suggestion="ask_user",
            skill_count=0,
            graph_count=0,
            memory_count=0,
        )

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
                        "message": "test queue release do",
                        "mode": "do",
                        "max_steps": 6,
                        "model": "base-model",
                        "parameters": {"temperature": 0},
                    },
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        with get_connection() as conn:
            row = conn.execute(
                "SELECT id FROM task_runs ORDER BY id DESC LIMIT 1",
            ).fetchone()
        self.assertIsNotNone(row)
        return int(row["id"])

    async def test_new_run_do_waiting_releases_queue_ticket(self):
        from backend.src.agent.retrieval import KnowledgeSufficiencyResult
        from backend.src.main import create_app

        sufficiency_ask = KnowledgeSufficiencyResult(
            sufficient=False,
            reason="需要补充约束",
            missing_knowledge="domain_knowledge",
            suggestion="ask_user",
            skill_count=0,
            graph_count=0,
            memory_count=0,
        )
        fake_ticket = _FakeQueueTicket()

        app = create_app()
        transport = httpx.ASGITransport(app=app)
        with patch(
            "backend.src.agent.runner.execution_pipeline.enqueue_postprocess_thread",
            return_value=None,
        ), patch(
            "backend.src.agent.runner.stream_new_run.acquire_stream_queue_ticket",
            new=AsyncMock(return_value=fake_ticket),
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
                        "message": "test queue release do",
                        "mode": "do",
                        "max_steps": 6,
                        "model": "base-model",
                        "parameters": {"temperature": 0},
                    },
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        self.assertEqual(1, int(fake_ticket.release_calls))

    async def test_new_run_think_waiting_releases_queue_ticket(self):
        from backend.src.agent.retrieval import KnowledgeSufficiencyResult
        from backend.src.main import create_app

        sufficiency_ask = KnowledgeSufficiencyResult(
            sufficient=False,
            reason="需要补充约束",
            missing_knowledge="domain_knowledge",
            suggestion="ask_user",
            skill_count=0,
            graph_count=0,
            memory_count=0,
        )
        fake_ticket = _FakeQueueTicket()

        app = create_app()
        transport = httpx.ASGITransport(app=app)
        with patch(
            "backend.src.agent.runner.execution_pipeline.enqueue_postprocess_thread",
            return_value=None,
        ), patch(
            "backend.src.agent.runner.stream_think_run.acquire_stream_queue_ticket",
            new=AsyncMock(return_value=fake_ticket),
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
                        "message": "test queue release think",
                        "mode": "think",
                        "max_steps": 6,
                        "model": "base-model",
                        "parameters": {"temperature": 0},
                    },
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        self.assertEqual(1, int(fake_ticket.release_calls))

    async def test_resume_pending_planning_waiting_releases_queue_ticket(self):
        from backend.src.agent.retrieval import KnowledgeSufficiencyResult
        from backend.src.main import create_app

        run_id = await self._run_new_do_waiting()

        sufficiency_ask_again = KnowledgeSufficiencyResult(
            sufficient=False,
            reason="仍需要补充约束",
            missing_knowledge="domain_knowledge",
            suggestion="ask_user",
            skill_count=0,
            graph_count=0,
            memory_count=0,
        )
        fake_ticket = _FakeQueueTicket()

        app = create_app()
        transport = httpx.ASGITransport(app=app)
        with patch(
            "backend.src.agent.runner.stream_resume_run.enqueue_postprocess_thread",
            return_value=None,
        ), patch(
            "backend.src.agent.runner.stream_resume_run.acquire_stream_queue_ticket",
            new=AsyncMock(return_value=fake_ticket),
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
                    json={"run_id": int(run_id), "message": "补充一点信息"},
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        self.assertEqual(1, int(fake_ticket.release_calls))


if __name__ == "__main__":
    unittest.main()
