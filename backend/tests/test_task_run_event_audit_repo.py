import json
import os
import tempfile
import unittest


class TestTaskRunEventAuditRepo(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        os.environ["AGENT_DB_PATH"] = os.path.join(self._tmpdir.name, "agent_test.db")
        os.environ["AGENT_PROMPT_ROOT"] = os.path.join(self._tmpdir.name, "prompt")
        os.environ["AGENT_RUN_EVENT_AUDIT_DIR"] = os.path.join(self._tmpdir.name, "audit")

        import backend.src.storage as storage

        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
            os.environ.pop("AGENT_RUN_EVENT_AUDIT_DIR", None)
            os.environ.pop("AGENT_RUN_EVENT_AUDIT_ENABLED", None)
        except Exception:
            pass
        self._tmpdir.cleanup()

    def test_stream_emitter_writes_jsonl_audit(self):
        from backend.src.agent.runner.stream_entry_common import StreamRunStateEmitter

        emitter = StreamRunStateEmitter()
        emitter.bind_run(task_id=11, run_id=22, session_key="sess_audit")
        msg = emitter.emit_run_status("running")
        self.assertTrue(isinstance(msg, str))

        path = os.path.join(self._tmpdir.name, "audit", "run_22.jsonl")
        self.assertTrue(os.path.exists(path))
        with open(path, "r", encoding="utf-8") as fh:
            lines = [line.strip() for line in fh.readlines() if line.strip()]
        self.assertEqual(len(lines), 1)
        row = json.loads(lines[0])
        self.assertEqual(int(row.get("task_id") or 0), 11)
        self.assertEqual(int(row.get("run_id") or 0), 22)
        self.assertEqual(str(row.get("event_type") or ""), "run_status")
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        self.assertEqual(str(payload.get("type") or ""), "run_status")

    def test_audit_can_be_disabled(self):
        from backend.src.repositories.task_run_event_audit_repo import append_task_run_event_audit

        os.environ["AGENT_RUN_EVENT_AUDIT_ENABLED"] = "0"
        out = append_task_run_event_audit(
            task_id=1,
            run_id=2,
            event_id="e1",
            event_type="run_status",
            payload={"type": "run_status"},
        )
        self.assertIsNone(out)
        path = os.path.join(self._tmpdir.name, "audit", "run_2.jsonl")
        self.assertFalse(os.path.exists(path))


if __name__ == "__main__":
    unittest.main()

