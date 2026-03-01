import time
import unittest


class TestStreamPumpStopSignal(unittest.IsolatedAsyncioTestCase):
    async def test_pump_stops_when_stop_status_provider_returns_terminal_status(self):
        from backend.src.agent.runner.stream_pump import pump_sync_generator

        started = time.monotonic()

        def _inner():
            yield "msg-1"
            # 模拟内层同步生成器卡在长阻塞步骤。
            time.sleep(3.0)
            return "done"

        def _stop_status_provider():
            # 启动后很快报告外部已收敛到 stopped。
            if (time.monotonic() - started) > 0.12:
                return "stopped"
            return ""

        seen_stop = ""
        async for kind, payload in pump_sync_generator(
            inner=_inner(),
            label="stop_signal_test",
            poll_interval_seconds=0.02,
            idle_timeout_seconds=10.0,
            stop_status_provider=_stop_status_provider,
        ):
            if kind == "stop":
                seen_stop = str(payload or "")
                break

        self.assertEqual(seen_stop, "stopped")


if __name__ == "__main__":
    unittest.main()
