import json
import unittest

from backend.src.agent.runner.stream_convergence import completion_reason_for_status, resolve_terminal_meta
from backend.src.agent.runner.stream_entry_common import done_sse_event


def _parse_sse_data_json(msg: str):
    text = str(msg or "")
    data_lines = [line for line in text.splitlines() if line.startswith("data: ")]
    if not data_lines:
        return None
    try:
        return json.loads(data_lines[0][len("data: ") :])
    except Exception:
        return None


class TestStreamConvergenceContract(unittest.TestCase):
    def test_completion_reason_mapping(self):
        self.assertEqual(completion_reason_for_status("done"), "completed")
        self.assertEqual(completion_reason_for_status("waiting"), "waiting_input")
        self.assertEqual(completion_reason_for_status("failed"), "failed")
        self.assertEqual(completion_reason_for_status("stopped"), "stopped")
        self.assertEqual(completion_reason_for_status("unknown"), "unknown")

    def test_resolve_terminal_meta_with_non_runtime_source(self):
        meta = resolve_terminal_meta("failed", status_source="db")
        self.assertEqual(meta.run_status, "failed")
        self.assertEqual(meta.terminal_source, "db")
        self.assertEqual(meta.completion_reason, "failed_from_db")

    def test_done_sse_event_contains_terminal_fields(self):
        msg = done_sse_event(run_status="done")
        self.assertIn("event: done", msg)
        payload = _parse_sse_data_json(msg)
        self.assertIsNotNone(payload)
        self.assertEqual(str(payload.get("type") or ""), "stream_end")
        self.assertEqual(str(payload.get("run_status") or ""), "done")
        self.assertEqual(str(payload.get("completion_reason") or ""), "completed")
        self.assertEqual(str(payload.get("terminal_source") or ""), "runtime")

    def test_done_sse_event_fallbacks_failed_when_status_missing(self):
        msg = done_sse_event(run_status="")
        payload = _parse_sse_data_json(msg)
        self.assertIsNotNone(payload)
        self.assertEqual(str(payload.get("run_status") or ""), "failed")
        self.assertEqual(str(payload.get("completion_reason") or ""), "failed")
        self.assertEqual(str(payload.get("terminal_source") or ""), "runtime")


if __name__ == "__main__":
    unittest.main()

