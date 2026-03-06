import json
import time
import unittest

from backend.src.agent.runner.react_step_executor import (
    build_step_progress_payload,
    run_blocking_call_with_progress,
)


class TestBlockingProgress(unittest.TestCase):
    def _decode_sse_payload(self, chunk: str) -> dict:
        prefix = "data: "
        self.assertTrue(chunk.startswith(prefix))
        return json.loads(chunk[len(prefix):].strip())

    def test_blocking_call_emits_periodic_progress(self):
        def _slow_call():
            time.sleep(0.08)
            return "ok"

        gen = run_blocking_call_with_progress(
            func=_slow_call,
            start_payload=build_step_progress_payload(
                task_id=1,
                run_id=2,
                step_order=3,
                title="file_write:data/out.csv 写入结果",
                phase="action_generation",
                status="start",
                message="开始生成动作",
            ),
            progress_payload_builder=lambda elapsed_ms, tick: build_step_progress_payload(
                task_id=1,
                run_id=2,
                step_order=3,
                title="file_write:data/out.csv 写入结果",
                phase="action_generation",
                status="running",
                message="动作生成中",
                elapsed_ms=elapsed_ms,
                tick=tick,
            ),
            interval_seconds=0.02,
        )

        events = []
        try:
            while True:
                events.append(next(gen))
        except StopIteration as exc:
            result = exc.value

        self.assertEqual(result, "ok")
        payloads = [self._decode_sse_payload(item) for item in events]
        self.assertGreaterEqual(len(payloads), 2)
        self.assertEqual(payloads[0].get("type"), "step_progress")
        self.assertEqual(payloads[0].get("status"), "start")
        self.assertTrue(any(p.get("status") == "running" for p in payloads[1:]))
        self.assertTrue(any(int(p.get("tick") or 0) >= 1 for p in payloads[1:]))

    def test_blocking_call_drains_custom_events(self):
        emitted = []

        def _slow_call():
            time.sleep(0.03)
            return "ok"

        def _drain_events():
            if emitted:
                items = list(emitted)
                emitted.clear()
                return items
            return []

        emitted.append({"type": "search_progress", "stage": "search_query", "query": "黄金 价格"})
        gen = run_blocking_call_with_progress(
            func=_slow_call,
            interval_seconds=0.01,
            drain_events=_drain_events,
        )

        events = []
        try:
            while True:
                events.append(next(gen))
        except StopIteration as exc:
            result = exc.value

        self.assertEqual(result, "ok")
        payloads = [self._decode_sse_payload(item) for item in events]
        self.assertTrue(any(p.get("type") == "search_progress" for p in payloads))


if __name__ == "__main__":
    unittest.main()
