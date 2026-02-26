import unittest
from unittest.mock import patch

from backend.src.services.tasks.postprocess.backfill import (
    backfill_missing_agent_reviews,
    backfill_waiting_feedback_agent_reviews,
)


class TestTaskPostprocessBackfill(unittest.TestCase):
    def test_backfill_missing_agent_reviews(self):
        rows = [
            {"id": 11, "task_id": 101},
            {"id": 12, "task_id": 102},
        ]

        with patch(
            "backend.src.services.tasks.postprocess.backfill.list_agent_runs_missing_reviews",
            return_value=rows,
        ):
            out = backfill_missing_agent_reviews(
                ensure_agent_review_record_fn=lambda **kwargs: kwargs["run_id"] + 1000,
                limit=10,
            )

        self.assertTrue(out.get("ok"))
        self.assertEqual(2, out.get("count"))
        self.assertEqual(1011, out["items"][0]["review_id"])

    def test_backfill_waiting_feedback_agent_reviews_filters_step(self):
        rows = [
            {"id": 21, "task_id": 201, "agent_state": '{"paused":{"step_title":"确认满意度"}}'},
            {"id": 22, "task_id": 202, "agent_state": '{"paused":{"step_title":"用户补充信息"}}'},
        ]

        with patch(
            "backend.src.services.tasks.postprocess.backfill.list_agent_runs_missing_reviews",
            return_value=rows,
        ):
            out = backfill_waiting_feedback_agent_reviews(
                ensure_agent_review_record_fn=lambda **kwargs: kwargs["run_id"] + 2000,
                limit=10,
            )

        self.assertTrue(out.get("ok"))
        self.assertEqual(1, out.get("count"))
        self.assertEqual(21, out["items"][0]["run_id"])
        self.assertEqual(2021, out["items"][0]["review_id"])


if __name__ == "__main__":
    unittest.main()
