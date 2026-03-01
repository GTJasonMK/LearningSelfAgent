import json
import unittest

from backend.src.agent.core.plan_structure import PlanStructure
from backend.src.agent.runner.react_error_handler import handle_step_failure
from backend.src.constants import RUN_STATUS_FAILED


def _drain_generator(gen):
    chunks = []
    try:
        while True:
            chunks.append(next(gen))
    except StopIteration as stop:
        return chunks, stop.value


def _parse_sse_chunk(chunk_text):
    event_name = ""
    data_lines = []
    for line in str(chunk_text or "").splitlines():
        if line.startswith("event:"):
            event_name = str(line[6:]).strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
    payload = {}
    if data_lines:
        payload = json.loads("\n".join(data_lines))
    return event_name, payload


def _build_plan_struct():
    return PlanStructure.from_agent_plan_payload(
        {
            "titles": ["step1", "step2"],
            "items": [
                {"allow": ["file_write"], "status": "done"},
                {"allow": ["tool_call"], "status": "running"},
            ],
        }
    )


class TestReactStepErrorEventSemantics(unittest.TestCase):
    def test_step_failure_emits_non_terminal_step_error_event(self):
        plan_struct = _build_plan_struct()
        debug_logs = []
        failed_steps = []

        def _safe_write_debug(**kwargs):
            debug_logs.append(dict(kwargs))

        def _mark_task_step_failed(**kwargs):
            failed_steps.append(dict(kwargs))

        def _unexpected_replan(**kwargs):
            raise AssertionError("run_replan_and_merge should not be called in this test")

        gen = handle_step_failure(
            task_id=1,
            run_id=1,
            step_id=2,
            step_order=2,
            idx=1,
            title="tool_call:web_fetch",
            message="测试",
            workdir=".",
            model="gpt",
            react_params={},
            tools_hint="",
            skills_hint="",
            memories_hint="",
            graph_hint="",
            action_type="tool_call",
            step_error="[code=rate_limited] upstream limited",
            plan_struct=plan_struct,
            agent_state={},
            context={"latest_parse_input_text": "x"},
            observations=[],
            max_steps_limit=1,
            run_replan_and_merge=_unexpected_replan,
            safe_write_debug=_safe_write_debug,
            mark_task_step_failed=_mark_task_step_failed,
            finished_at="2026-01-01T00:00:00Z",
        )

        chunks, ret = _drain_generator(gen)
        self.assertEqual(ret, (RUN_STATUS_FAILED, None))
        self.assertTrue(failed_steps)
        self.assertTrue(debug_logs)

        parsed = [_parse_sse_chunk(chunk) for chunk in chunks]
        step_error_events = [item for item in parsed if item[1].get("type") == "step_error"]
        self.assertTrue(step_error_events, "missing step_error event")
        event_name, payload = step_error_events[-1]
        self.assertEqual(event_name, "")
        self.assertEqual(payload.get("code"), "rate_limited")
        self.assertTrue(payload.get("recoverable"))
        self.assertFalse(payload.get("non_retriable_failure"))

    def test_step_failure_non_retriable_marked_recoverable_false(self):
        plan_struct = _build_plan_struct()

        def _noop(**kwargs):
            return None

        def _unexpected_replan(**kwargs):
            raise AssertionError("run_replan_and_merge should not be called in this test")

        gen = handle_step_failure(
            task_id=1,
            run_id=1,
            step_id=2,
            step_order=2,
            idx=1,
            title="tool_call:web_fetch",
            message="测试",
            workdir=".",
            model="gpt",
            react_params={},
            tools_hint="",
            skills_hint="",
            memories_hint="",
            graph_hint="",
            action_type="tool_call",
            step_error="[code=invalid_action_payload] bad payload",
            plan_struct=plan_struct,
            agent_state={},
            context={},
            observations=[],
            max_steps_limit=1,
            run_replan_and_merge=_unexpected_replan,
            safe_write_debug=_noop,
            mark_task_step_failed=_noop,
            finished_at="2026-01-01T00:00:00Z",
        )

        chunks, ret = _drain_generator(gen)
        self.assertEqual(ret, (RUN_STATUS_FAILED, None))

        parsed = [_parse_sse_chunk(chunk) for chunk in chunks]
        step_error_events = [item for item in parsed if item[1].get("type") == "step_error"]
        self.assertTrue(step_error_events, "missing step_error event")
        _event_name, payload = step_error_events[-1]
        self.assertEqual(payload.get("code"), "invalid_action_payload")
        self.assertFalse(payload.get("recoverable"))
        self.assertTrue(payload.get("non_retriable_failure"))


if __name__ == "__main__":
    unittest.main()
