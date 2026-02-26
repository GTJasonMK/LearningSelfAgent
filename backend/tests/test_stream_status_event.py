import json
import unittest

from backend.src.agent.runner.stream_status_event import (
    build_run_status_sse,
    is_legal_stream_run_status_transition,
    normalize_stream_run_status,
)


def _parse_sse_data_json(msg: str):
    text = str(msg or "")
    prefix = "data: "
    if not text.startswith(prefix):
        return None
    payload_line = text[len(prefix):].splitlines()[0]
    try:
        return json.loads(payload_line)
    except Exception:
        return None


class TestStreamStatusEvent(unittest.TestCase):
    def test_normalize_stream_run_status_accepts_valid_values(self):
        self.assertEqual(normalize_stream_run_status("running"), "running")
        self.assertEqual(normalize_stream_run_status("WAITING"), "waiting")
        self.assertEqual(normalize_stream_run_status("Done"), "done")
        self.assertEqual(normalize_stream_run_status(" failed "), "failed")
        self.assertEqual(normalize_stream_run_status("stopped"), "stopped")

    def test_normalize_stream_run_status_rejects_invalid_values(self):
        self.assertEqual(normalize_stream_run_status(""), "")
        self.assertEqual(normalize_stream_run_status(None), "")
        self.assertEqual(normalize_stream_run_status("cancelled"), "")
        self.assertEqual(normalize_stream_run_status("unknown"), "")

    def test_build_run_status_sse_includes_required_fields(self):
        msg = build_run_status_sse(
            status="running",
            task_id=11,
            run_id=22,
            stage="retrieval",
            session_key="sess_demo",
        )
        obj = _parse_sse_data_json(msg)
        self.assertIsNotNone(obj)
        self.assertEqual(obj.get("type"), "run_status")
        self.assertEqual(obj.get("task_id"), 11)
        self.assertEqual(obj.get("run_id"), 22)
        self.assertEqual(obj.get("status"), "running")
        self.assertEqual(obj.get("stage"), "retrieval")
        self.assertEqual(obj.get("session_key"), "sess_demo")

    def test_build_run_status_sse_omits_blank_stage(self):
        msg = build_run_status_sse(status="bad-status", task_id=None, run_id=2, stage=" ")
        obj = _parse_sse_data_json(msg)
        self.assertIsNotNone(obj)
        self.assertEqual(obj.get("type"), "run_status")
        self.assertEqual(obj.get("task_id"), None)
        self.assertEqual(obj.get("run_id"), 2)
        self.assertEqual(obj.get("status"), "")
        self.assertNotIn("stage", obj)

    def test_legal_transition_rejects_terminal_back_to_running(self):
        self.assertTrue(is_legal_stream_run_status_transition("running", "done"))
        self.assertTrue(is_legal_stream_run_status_transition("waiting", "running"))
        self.assertFalse(is_legal_stream_run_status_transition("done", "running"))
        self.assertFalse(is_legal_stream_run_status_transition("failed", "waiting"))
        self.assertFalse(is_legal_stream_run_status_transition("stopped", "running"))


if __name__ == "__main__":
    unittest.main()
