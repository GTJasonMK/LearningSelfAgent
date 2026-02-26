import os
import tempfile
import unittest


class TestStreamEventReplayLog(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        os.environ["AGENT_DB_PATH"] = os.path.join(self._tmpdir.name, "agent_test.db")
        os.environ["AGENT_PROMPT_ROOT"] = os.path.join(self._tmpdir.name, "prompt")

        import backend.src.storage as storage

        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
        except Exception:
            pass
        self._tmpdir.cleanup()

    def test_emitter_writes_typed_events_into_run_log(self):
        from backend.src.agent.runner.stream_entry_common import StreamRunStateEmitter
        from backend.src.repositories.task_run_events_repo import list_task_run_events

        emitter = StreamRunStateEmitter()
        emitter.bind_run(task_id=11, run_id=22, session_key="sess_test")
        msg = emitter.emit_run_status("running")
        self.assertTrue(isinstance(msg, str))

        rows = list_task_run_events(run_id=22, limit=20)
        self.assertEqual(len(rows), 1)
        self.assertEqual(str(rows[0]["event_type"] or ""), "run_status")


if __name__ == "__main__":
    unittest.main()

