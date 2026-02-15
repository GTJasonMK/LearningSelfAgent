import unittest

from backend.src.agent.runner.stream_task_events import iter_stream_task_events


class TestStreamTaskEvents(unittest.IsolatedAsyncioTestCase):
    async def test_iter_stream_task_events_emits_msgs_then_done(self):
        async def _worker(emit):
            emit("m1")
            emit("m2")
            return "ok"

        events = []
        async for event_type, payload in iter_stream_task_events(task_builder=_worker):
            events.append((event_type, payload))

        self.assertEqual(
            events,
            [
                ("msg", "m1"),
                ("msg", "m2"),
                ("done", "ok"),
            ],
        )

    async def test_iter_stream_task_events_propagates_error(self):
        async def _worker(_emit):
            raise RuntimeError("boom")

        with self.assertRaises(RuntimeError):
            async for _ in iter_stream_task_events(task_builder=_worker):
                pass


if __name__ == "__main__":
    unittest.main()
