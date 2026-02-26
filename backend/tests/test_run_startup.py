import json
import unittest
from unittest.mock import AsyncMock, Mock, patch

from backend.src.agent.core.run_context import AgentRunContext
from backend.src.agent.runner.run_startup import start_new_mode_run


def _parse_sse_data_json(msg: str):
    text = str(msg or "")
    prefix = "data: "
    if not text.startswith(prefix):
        return None
    line = text[len(prefix):].splitlines()[0]
    try:
        return json.loads(line)
    except Exception:
        return None


class TestRunStartup(unittest.IsolatedAsyncioTestCase):
    async def test_start_new_mode_run_emits_boot_events_and_debug(self):
        run_ctx = AgentRunContext.from_agent_state({"mode": "do"})
        safe_debug = Mock()
        with patch(
            "backend.src.agent.runner.run_startup.bootstrap_new_mode_run",
            new_callable=AsyncMock,
        ) as mock_boot:
            mock_boot.return_value.task_id = 1
            mock_boot.return_value.run_id = 2
            mock_boot.return_value.run_ctx = run_ctx
            mock_boot.return_value.run_created_event = "run-created-event"
            mock_boot.return_value.stage_event = "stage-event"
            started = await start_new_mode_run(
                message="hello",
                mode="do",
                model="m1",
                parameters={"temperature": 0.2},
                max_steps=6,
                workdir="/tmp",
                stage_where_prefix="new_run",
                safe_write_debug=safe_debug,
                start_debug_message="agent.start",
                start_debug_data={"x": 1},
                start_delta="启动",
            )

        self.assertEqual(started.task_id, 1)
        self.assertEqual(started.run_id, 2)
        self.assertEqual(started.run_ctx, run_ctx)
        self.assertGreaterEqual(len(started.events), 4)
        self.assertEqual(started.events[0], "run-created-event")

        run_status_event = _parse_sse_data_json(started.events[1])
        self.assertIsNotNone(run_status_event)
        self.assertEqual(run_status_event.get("type"), "run_status")
        self.assertEqual(run_status_event.get("task_id"), 1)
        self.assertEqual(run_status_event.get("run_id"), 2)
        self.assertEqual(run_status_event.get("status"), "running")
        self.assertEqual(run_status_event.get("stage"), "retrieval")

        self.assertEqual(started.events[2], "stage-event")
        self.assertIn("启动", started.events[3])
        safe_debug.assert_called_once_with(1, 2, message="agent.start", data={"x": 1})
        mock_boot.assert_awaited_once()

    async def test_start_new_mode_run_without_optional_messages(self):
        run_ctx = AgentRunContext.from_agent_state({"mode": "think"})
        with patch(
            "backend.src.agent.runner.run_startup.bootstrap_new_mode_run",
            new_callable=AsyncMock,
        ) as mock_boot:
            mock_boot.return_value.task_id = 3
            mock_boot.return_value.run_id = 4
            mock_boot.return_value.run_ctx = run_ctx
            mock_boot.return_value.run_created_event = "run-created-event"
            mock_boot.return_value.stage_event = None
            started = await start_new_mode_run(
                message="hello",
                mode="think",
                model="m1",
                parameters={},
                max_steps=None,
                workdir="/tmp",
                stage_where_prefix="think_run",
            )

        self.assertEqual(len(started.events), 2)
        self.assertEqual(started.events[0], "run-created-event")
        run_status_event = _parse_sse_data_json(started.events[1])
        self.assertIsNotNone(run_status_event)
        self.assertEqual(run_status_event.get("type"), "run_status")
        self.assertEqual(run_status_event.get("task_id"), 3)
        self.assertEqual(run_status_event.get("run_id"), 4)
        self.assertEqual(run_status_event.get("status"), "running")
        self.assertEqual(run_status_event.get("stage"), "retrieval")


if __name__ == "__main__":
    unittest.main()
