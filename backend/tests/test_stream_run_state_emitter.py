import json
import unittest

from backend.src.agent.runner.stream_entry_common import StreamRunStateEmitter


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


class TestStreamRunStateEmitter(unittest.TestCase):
    def test_emit_run_status_deduplicates(self):
        emitter = StreamRunStateEmitter()
        emitter.bind_run(task_id=1, run_id=2)
        first = emitter.emit_run_status("running")
        second = emitter.emit_run_status("running")
        self.assertIsNotNone(first)
        self.assertIsNone(second)
        obj = _parse_sse_data_json(first)
        self.assertIsNotNone(obj)
        self.assertEqual(obj.get("type"), "run_status")
        self.assertEqual(obj.get("task_id"), 1)
        self.assertEqual(obj.get("run_id"), 2)
        self.assertEqual(obj.get("status"), "running")

    def test_missing_visible_result_emits_only_when_needed(self):
        emitter = StreamRunStateEmitter()
        emitter.bind_run(task_id=3, run_id=4)
        missing = emitter.build_missing_visible_result_if_needed("done")
        self.assertIsNotNone(missing)
        self.assertIn("【结果】", str(missing))

        emitter2 = StreamRunStateEmitter()
        emitter2.bind_run(task_id=5, run_id=6)
        emitter2.emit("data: {\"delta\": \"【结果】\\nok\"}\n\n")
        self.assertIsNone(emitter2.build_missing_visible_result_if_needed("done"))

    def test_emit_run_status_requires_bound_run(self):
        emitter = StreamRunStateEmitter()
        self.assertIsNone(emitter.emit_run_status("done"))

    def test_emit_run_status_rejects_terminal_back_transition(self):
        emitter = StreamRunStateEmitter()
        emitter.bind_run(task_id=7, run_id=8)
        done_msg = emitter.emit_run_status("done")
        self.assertIsNotNone(done_msg)
        illegal_msg = emitter.emit_run_status("running")
        self.assertIsNone(illegal_msg)


if __name__ == "__main__":
    unittest.main()
