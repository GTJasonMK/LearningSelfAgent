import json
import unittest
from unittest.mock import patch


def _parse_sse_data_json(msg: str):
    if not isinstance(msg, str) or not msg:
        return None
    data_lines: list[str] = []
    for line in msg.splitlines():
        if line.startswith("data:"):
            data_lines.append(line[len("data:") :].lstrip())
    if not data_lines:
        return None
    try:
        return json.loads("\n".join(data_lines))
    except Exception:
        return None


class TestStreamPumpPlanThrottle(unittest.IsolatedAsyncioTestCase):
    async def test_pump_coalesces_plan_messages_and_flushes_before_need_input(self):
        from backend.src.agent.runner.stream_pump import pump_sync_generator
        from backend.src.services.llm.llm_client import sse_json

        def inner():
            yield sse_json({"type": "plan", "task_id": 1, "items": [{"id": 1, "status": "running"}]})
            yield sse_json({"type": "plan", "task_id": 1, "items": [{"id": 1, "status": "done"}]})
            yield sse_json({"type": "plan", "task_id": 1, "items": [{"id": 1, "status": "waiting"}]})
            yield sse_json({"delta": "hi\n"})
            yield sse_json({"type": "need_input", "task_id": 1, "run_id": 1, "question": "q"})
            return "ok"

        # 把节流窗口拉大：确保中间的 plan 更新会被合并（只保留最后一次）
        with patch("backend.src.agent.runner.stream_pump.AGENT_SSE_PLAN_MIN_INTERVAL_SECONDS", 999):
            msgs: list[str] = []
            async for kind, payload in pump_sync_generator(
                inner=inner(),
                label="test",
                poll_interval_seconds=1,
                idle_timeout_seconds=10,
            ):
                if kind == "msg":
                    msgs.append(str(payload or ""))
                if kind in {"done", "err"}:
                    break

        plan_statuses: list[str] = []
        need_input_seen = False
        delta_idx = -1
        waiting_plan_idx = -1
        for i, m in enumerate(msgs):
            obj = _parse_sse_data_json(m)
            if not isinstance(obj, dict):
                continue
            if obj.get("delta") == "hi\n":
                delta_idx = i
            if obj.get("type") == "need_input":
                need_input_seen = True
            if obj.get("type") == "plan":
                items = obj.get("items") if isinstance(obj.get("items"), list) else []
                status = ""
                if items and isinstance(items[0], dict):
                    status = str(items[0].get("status") or "")
                plan_statuses.append(status)
                if status == "waiting":
                    waiting_plan_idx = i

        self.assertTrue(need_input_seen)
        # 期望：running（首次）+ waiting（最新）；中间 done 被合并
        self.assertEqual(plan_statuses, ["running", "waiting"])
        # 期望：delta 在 waiting plan flush 之前
        self.assertTrue(delta_idx >= 0)
        self.assertTrue(waiting_plan_idx >= 0)
        self.assertLess(delta_idx, waiting_plan_idx)

