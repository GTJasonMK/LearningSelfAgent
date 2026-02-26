import json
import unittest
from unittest.mock import patch


def _parse_sse_data_json(msg: str):
    text = str(msg or "")
    data_lines = [line for line in text.splitlines() if line.startswith("data: ")]
    if not data_lines:
        return None
    try:
        return json.loads(data_lines[0][len("data: ") :])
    except Exception:
        return None


class TestStreamModeLifecycleDoneTail(unittest.IsolatedAsyncioTestCase):
    async def _collect(self, agen):
        out = []
        async for chunk in agen:
            out.append(str(chunk))
        return out

    async def test_done_tail_fallbacks_status_and_emits_observability_error(self):
        from backend.src.agent.runner.stream_mode_lifecycle import (
            StreamModeLifecycle,
            iter_stream_done_tail,
        )

        lifecycle = StreamModeLifecycle(task_id=11, run_id=22, session_key="sess_x")
        lifecycle.stream_state.bind_run(task_id=11, run_id=22, session_key="sess_x", prime_status="running")

        with patch(
            "backend.src.agent.runner.stream_mode_lifecycle.get_task_run",
            return_value={"status": "failed"},
        ):
            chunks = await self._collect(iter_stream_done_tail(lifecycle=lifecycle, run_status=""))

        self.assertTrue(any("event: error" in c for c in chunks))
        self.assertTrue(any("event: done" in c for c in chunks))

        payloads = [_parse_sse_data_json(c) for c in chunks]
        payloads = [p for p in payloads if isinstance(p, dict)]

        anomaly = next((p for p in payloads if str(p.get("code") or "") == "stream_missing_terminal_status"), None)
        self.assertIsNotNone(anomaly)
        self.assertEqual(str(anomaly.get("resolved_status") or ""), "failed")

        run_status = next((p for p in payloads if str(p.get("type") or "") == "run_status"), None)
        self.assertIsNotNone(run_status)
        self.assertEqual(str(run_status.get("status") or ""), "failed")

    async def test_done_tail_does_not_emit_observability_error_when_status_present(self):
        from backend.src.agent.runner.stream_mode_lifecycle import (
            StreamModeLifecycle,
            iter_stream_done_tail,
        )

        lifecycle = StreamModeLifecycle(task_id=33, run_id=44, session_key="sess_y")
        lifecycle.stream_state.bind_run(task_id=33, run_id=44, session_key="sess_y", prime_status="running")

        with patch("backend.src.agent.runner.stream_mode_lifecycle.get_task_run") as mocked_get:
            chunks = await self._collect(iter_stream_done_tail(lifecycle=lifecycle, run_status="done"))

        mocked_get.assert_not_called()
        self.assertTrue(any("event: done" in c for c in chunks))
        self.assertFalse(any("stream_missing_terminal_status" in c for c in chunks))


if __name__ == "__main__":
    unittest.main()
