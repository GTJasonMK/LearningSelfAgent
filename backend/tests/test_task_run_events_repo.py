import os
import tempfile
import unittest


class TestTaskRunEventsRepo(unittest.TestCase):
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

    def test_create_and_list_with_after_event_id(self):
        from backend.src.common.utils import now_iso
        from backend.src.repositories.task_run_events_repo import create_task_run_event, list_task_run_events
        from backend.src.services.tasks.task_run_lifecycle import create_task_and_run_records_for_agent

        task_id, run_id = create_task_and_run_records_for_agent(message="m", created_at=now_iso())
        create_task_run_event(
            task_id=int(task_id),
            run_id=int(run_id),
            session_key="sess_x",
            event_id="sess_x:1:1:run_status",
            event_type="run_status",
            payload={"type": "run_status", "event_id": "sess_x:1:1:run_status"},
        )
        create_task_run_event(
            task_id=int(task_id),
            run_id=int(run_id),
            session_key="sess_x",
            event_id="sess_x:1:2:need_input",
            event_type="need_input",
            payload={"type": "need_input", "event_id": "sess_x:1:2:need_input"},
        )
        # duplicate -> ignore
        duplicate_id = create_task_run_event(
            task_id=int(task_id),
            run_id=int(run_id),
            session_key="sess_x",
            event_id="sess_x:1:2:need_input",
            event_type="need_input",
            payload={"type": "need_input", "event_id": "sess_x:1:2:need_input"},
        )
        self.assertIsNone(duplicate_id)

        all_rows = list_task_run_events(run_id=int(run_id), limit=20)
        self.assertEqual(len(all_rows), 2)

        delta_rows = list_task_run_events(
            run_id=int(run_id),
            after_event_id="sess_x:1:1:run_status",
            limit=20,
        )
        self.assertEqual(len(delta_rows), 1)
        self.assertEqual(str(delta_rows[0]["event_type"] or ""), "need_input")

    def test_list_with_missing_after_event_id_returns_latest_window(self):
        from backend.src.common.utils import now_iso
        from backend.src.repositories.task_run_events_repo import create_task_run_event, list_task_run_events
        from backend.src.services.tasks.task_run_lifecycle import create_task_and_run_records_for_agent

        task_id, run_id = create_task_and_run_records_for_agent(message="m", created_at=now_iso())
        for i in range(1, 6):
            event_id = f"sess_x:1:{i}:run_status"
            create_task_run_event(
                task_id=int(task_id),
                run_id=int(run_id),
                session_key="sess_x",
                event_id=event_id,
                event_type="run_status",
                payload={"type": "run_status", "event_id": event_id, "status": "running"},
            )

        rows = list_task_run_events(
            run_id=int(run_id),
            after_event_id="sess_x:1:999:run_status",
            limit=2,
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(str(rows[0]["event_id"] or ""), "sess_x:1:4:run_status")
        self.assertEqual(str(rows[1]["event_id"] or ""), "sess_x:1:5:run_status")


if __name__ == "__main__":
    unittest.main()
