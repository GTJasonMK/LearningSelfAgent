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


class TestStreamPumpPlanDeltaThrottle(unittest.IsolatedAsyncioTestCase):
    async def test_pump_coalesces_plan_delta_changes_and_flushes_before_need_input(self):
        from backend.src.agent.runner.stream_pump import pump_sync_generator
        from backend.src.services.llm.llm_client import sse_json

        def inner():
            # 三次增量更新：同一 step 的状态变化 + 另一个 step 的 running
            yield sse_json(
                {"type": "plan_delta", "task_id": 1, "changes": [{"id": 1, "step_order": 1, "status": "running"}]}
            )
            yield sse_json(
                {"type": "plan_delta", "task_id": 1, "changes": [{"id": 1, "step_order": 1, "status": "done"}]}
            )
            yield sse_json(
                {"type": "plan_delta", "task_id": 1, "changes": [{"id": 2, "step_order": 2, "status": "running"}]}
            )
            yield sse_json({"type": "need_input", "task_id": 1, "run_id": 1, "question": "q"})
            return "ok"

        # 把节流窗口拉大：确保中间的 plan_delta 会被合并后再输出（并在 need_input 前强制 flush）
        with patch("backend.src.agent.runner.stream_pump.AGENT_SSE_PLAN_MIN_INTERVAL_SECONDS", 999):
            msgs: list[str] = []
            async for kind, payload in pump_sync_generator(
                inner=inner(),
                label="test_plan_delta",
                poll_interval_seconds=1,
                idle_timeout_seconds=10,
            ):
                if kind == "msg":
                    msgs.append(str(payload or ""))
                if kind in {"done", "err"}:
                    break

        seen_need_input = False
        plan_delta_objs: list[dict] = []
        plan_delta_last_idx = -1
        need_input_idx = -1
        for i, m in enumerate(msgs):
            obj = _parse_sse_data_json(m)
            if not isinstance(obj, dict):
                continue
            if obj.get("type") == "need_input":
                seen_need_input = True
                if need_input_idx == -1:
                    need_input_idx = i
            if obj.get("type") == "plan_delta":
                plan_delta_objs.append(obj)
                plan_delta_last_idx = i

        self.assertTrue(seen_need_input)
        # 期望：至少合并一次（running 可能先即时输出；其余变更在 need_input 前合并 flush）
        self.assertEqual(len(plan_delta_objs), 2)
        self.assertTrue(plan_delta_last_idx >= 0)
        self.assertTrue(need_input_idx >= 0)
        self.assertLess(plan_delta_last_idx, need_input_idx)

        # 期望：最后一次 plan_delta 含最新状态（step1=done, step2=running）
        changes = plan_delta_objs[-1].get("changes")
        self.assertTrue(isinstance(changes, list))
        by_order = {int(c.get("step_order")): str(c.get("status")) for c in changes if isinstance(c, dict)}
        self.assertEqual(by_order.get(1), "done")
        self.assertEqual(by_order.get(2), "running")


if __name__ == "__main__":
    unittest.main()
