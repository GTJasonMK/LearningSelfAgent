import unittest
from unittest.mock import patch

from backend.src.agent.core.checkpoint_store import persist_checkpoint, persist_checkpoint_async


class TestCheckpointStore(unittest.TestCase):
    def test_persist_checkpoint_passes_status_and_clear_finished(self):
        with patch("backend.src.agent.core.checkpoint_store.update_task_run") as mock_update:
            err = persist_checkpoint(
                run_id=7,
                status="running",
                clear_finished_at=True,
                agent_plan={"titles": ["a"]},
                agent_state={"mode": "do"},
                retries=1,
            )

        self.assertIsNone(err)
        kwargs = mock_update.call_args.kwargs
        self.assertEqual(kwargs.get("run_id"), 7)
        self.assertEqual(kwargs.get("status"), "running")
        self.assertEqual(kwargs.get("clear_finished_at"), True)
        self.assertEqual(kwargs.get("agent_state"), {"mode": "do"})
        self.assertEqual(kwargs.get("agent_plan"), {"titles": ["a"]})

    def test_persist_checkpoint_retry_and_debug_trace(self):
        debug_logs = []

        def _debug(task_id, run_id, *, message, data=None, level="debug"):
            debug_logs.append(
                {
                    "task_id": task_id,
                    "run_id": run_id,
                    "message": message,
                    "data": dict(data or {}),
                    "level": level,
                }
            )

        with patch(
            "backend.src.agent.core.checkpoint_store.update_task_run",
            side_effect=RuntimeError("db_locked"),
        ) as mock_update:
            err = persist_checkpoint(
                run_id=9,
                agent_state={},
                retries=2,
                retry_backoff_seconds=0,
                task_id=11,
                safe_write_debug=_debug,
                where="unit_test",
            )

        self.assertIn("db_locked", str(err))
        self.assertEqual(mock_update.call_count, 2)
        self.assertEqual(len(debug_logs), 2)
        self.assertEqual(debug_logs[0]["message"], "agent.checkpoint.persist_retry")
        self.assertEqual(debug_logs[0]["data"].get("where"), "unit_test")

class TestCheckpointStoreAsync(unittest.IsolatedAsyncioTestCase):
    async def test_persist_checkpoint_async_forwards_parameters(self):
        with patch(
            "backend.src.agent.core.checkpoint_store.persist_checkpoint",
            return_value=None,
        ) as mock_persist:
            err = await persist_checkpoint_async(
                run_id=3,
                status="waiting",
                clear_finished_at=True,
                agent_plan={"items": []},
                agent_state={"mode": "think"},
                retries=1,
                where="async_case",
            )

        self.assertIsNone(err)
        kwargs = mock_persist.call_args.kwargs
        self.assertEqual(kwargs.get("run_id"), 3)
        self.assertEqual(kwargs.get("status"), "waiting")
        self.assertEqual(kwargs.get("clear_finished_at"), True)
        self.assertEqual(kwargs.get("where"), "async_case")
        self.assertEqual(kwargs.get("agent_state"), {"mode": "think"})


if __name__ == "__main__":
    unittest.main()
