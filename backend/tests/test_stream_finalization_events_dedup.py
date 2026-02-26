import unittest
from unittest.mock import patch

from backend.src.agent.runner.stream_entry_common import iter_finalization_events
from backend.src.agent.runner.stream_status_event import build_run_status_sse
from backend.src.services.llm.llm_client import sse_json


class StreamFinalizationEventsDedupTests(unittest.IsolatedAsyncioTestCase):
    async def _collect(self, agen):
        out = []
        async for item in agen:
            out.append(item)
        return out

    async def test_dedup_status_when_msg_and_return_match(self):
        async def fake_run_finalization_sequence(**kwargs):
            emit = kwargs["yield_func"]
            emit(sse_json({"delta": "phase:finalize"}))
            emit(build_run_status_sse(status="waiting", task_id=1, run_id=2))
            return "waiting"

        with patch(
            "backend.src.agent.runner.stream_entry_common.run_finalization_sequence",
            new=fake_run_finalization_sequence,
        ):
            events = await self._collect(
                iter_finalization_events(
                    task_id=1,
                    run_id=2,
                    run_status="waiting",
                    agent_state={},
                    plan_items=[],
                    plan_artifacts=[],
                    message="x",
                    workdir=".",
                )
            )

        self.assertEqual(events[0][0], "msg")
        self.assertIn("phase:finalize", events[0][1])
        self.assertEqual(events[1], ("status", "waiting"))
        self.assertEqual(sum(1 for event_type, _ in events if event_type == "status"), 1)

    async def test_emit_status_from_return_when_no_status_msg(self):
        async def fake_run_finalization_sequence(**kwargs):
            emit = kwargs["yield_func"]
            emit(sse_json({"delta": "phase:finalize"}))
            return "done"

        with patch(
            "backend.src.agent.runner.stream_entry_common.run_finalization_sequence",
            new=fake_run_finalization_sequence,
        ):
            events = await self._collect(
                iter_finalization_events(
                    task_id=3,
                    run_id=4,
                    run_status="done",
                    agent_state={},
                    plan_items=[],
                    plan_artifacts=[],
                    message="x",
                    workdir=".",
                )
            )

        self.assertEqual(events[0][0], "msg")
        self.assertIn("phase:finalize", events[0][1])
        self.assertEqual(events[1], ("status", "done"))


if __name__ == "__main__":
    unittest.main()
