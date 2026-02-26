import json
import time
import unittest


class TestStreamPumpHeartbeat(unittest.IsolatedAsyncioTestCase):
    async def test_pump_emits_heartbeat_during_idle_gap(self):
        from backend.src.agent.runner.stream_pump import pump_sync_generator
        from backend.src.services.llm.llm_client import sse_json

        def _inner():
            yield sse_json({"delta": "hello"})
            time.sleep(0.18)
            return "done"

        seen_heartbeat = False
        async for kind, payload in pump_sync_generator(
            inner=_inner(),
            label="heartbeat_test",
            poll_interval_seconds=0.02,
            idle_timeout_seconds=2.0,
            heartbeat_builder=lambda: sse_json({"type": "run_heartbeat", "run_id": 1, "task_id": 1}),
            heartbeat_min_interval_seconds=0.05,
            heartbeat_trigger_debounce_seconds=0.02,
        ):
            if kind != "msg":
                continue
            text = str(payload or "")
            data_lines = [line for line in text.splitlines() if line.startswith("data: ")]
            if not data_lines:
                continue
            try:
                obj = json.loads(data_lines[0][len("data: ") :])
            except Exception:
                continue
            if str(obj.get("type") or "") == "run_heartbeat":
                seen_heartbeat = True
                break

        self.assertTrue(seen_heartbeat)


if __name__ == "__main__":
    unittest.main()

