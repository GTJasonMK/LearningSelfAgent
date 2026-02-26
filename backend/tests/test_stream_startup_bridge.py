import unittest
from types import SimpleNamespace


class _FakeLifecycle:
    def __init__(self):
        self.bind_calls = []
        self.emitted = []

    async def bind_started_run(self, **kwargs):
        self.bind_calls.append(dict(kwargs))

    def emit(self, chunk: str) -> str:
        value = f"emit:{chunk}"
        self.emitted.append(value)
        return value


class TestStreamStartupBridge(unittest.IsolatedAsyncioTestCase):
    async def test_bootstrap_stream_mode_lifecycle_binds_and_emits(self):
        from backend.src.agent.runner.stream_startup_bridge import bootstrap_stream_mode_lifecycle

        fake_lifecycle = _FakeLifecycle()
        run_ctx = object()

        async def _start_mode_run(**kwargs):
            self.assertEqual("hello", kwargs.get("message"))
            return SimpleNamespace(
                task_id=11,
                run_id=22,
                run_ctx=run_ctx,
                events=["evt-a", "evt-b"],
            )

        async def _acquire_ticket(**_kwargs):
            return None

        result = await bootstrap_stream_mode_lifecycle(
            lifecycle=fake_lifecycle,  # type: ignore[arg-type]
            start_mode_run_func=_start_mode_run,
            start_mode_run_kwargs={"message": "hello"},
            acquire_queue_ticket_func=_acquire_ticket,
        )

        self.assertEqual(11, result.task_id)
        self.assertEqual(22, result.run_id)
        self.assertIs(run_ctx, result.run_ctx)
        self.assertEqual(["emit:evt-a", "emit:evt-b"], result.emitted_events)
        self.assertEqual(1, len(fake_lifecycle.bind_calls))
        self.assertEqual(11, int(fake_lifecycle.bind_calls[0]["task_id"]))
        self.assertEqual(22, int(fake_lifecycle.bind_calls[0]["run_id"]))


if __name__ == "__main__":
    unittest.main()
