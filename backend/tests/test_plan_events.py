import json
import unittest


class TestPlanEvents(unittest.TestCase):
    def _parse_sse(self, chunk: str) -> dict:
        prefix = "data: "
        self.assertTrue(str(chunk).startswith(prefix))
        return json.loads(str(chunk)[len(prefix):].strip())

    def test_sse_plan_hides_pending_feedback_tail(self):
        from backend.src.agent.runner.plan_events import sse_plan

        payload = self._parse_sse(
            sse_plan(
                task_id=1,
                run_id=2,
                plan_items=[
                    {"id": 1, "title": "task_output:输出结果", "brief": "输出结果", "status": "done", "kind": "task_output"},
                    {"id": 2, "title": "确认满意度", "brief": "确认满意度", "status": "pending", "kind": "task_feedback"},
                ],
            )
        )

        items = payload.get("items") or []
        self.assertEqual(len(items), 1)
        self.assertEqual(str(items[0].get("title") or ""), "task_output:输出结果")

    def test_sse_plan_keeps_feedback_once_it_is_waiting(self):
        from backend.src.agent.runner.plan_events import sse_plan

        payload = self._parse_sse(
            sse_plan(
                task_id=1,
                run_id=2,
                plan_items=[
                    {"id": 1, "title": "task_output:输出结果", "brief": "输出结果", "status": "done", "kind": "task_output"},
                    {"id": 2, "title": "确认满意度", "brief": "确认满意度", "status": "waiting", "kind": "task_feedback"},
                ],
            )
        )

        items = payload.get("items") or []
        self.assertEqual(len(items), 2)
        self.assertEqual(str(items[-1].get("status") or ""), "waiting")
        self.assertEqual(str(items[-1].get("title") or ""), "确认满意度")


if __name__ == "__main__":
    unittest.main()
