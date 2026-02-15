import unittest
from unittest.mock import patch

from backend.src.actions.handlers.task_output import execute_task_output


class TestTaskOutputEvidenceEnforcement(unittest.TestCase):
    def test_enforce_evidence_appends_step_tool_artifact_refs(self):
        rows = [
            {
                "id": 1,
                "status": "done",
                "title": "tool_call:抓取黄金数据",
                "detail": '{"type":"tool_call"}',
                "result": '{"id":12,"tool_id":2}',
            },
            {
                "id": 2,
                "status": "done",
                "title": "file_write:写入CSV",
                "detail": '{"type":"file_write"}',
                "result": '{"path":"backend/.agent/workspace/gold_prices.csv","bytes":100}',
            },
        ]

        with patch("backend.src.actions.handlers.task_output.list_task_steps_for_run", return_value=rows), patch(
            "backend.src.actions.handlers.task_output._create_task_output_record",
            side_effect=lambda task_id, payload: ({"content": payload.get("content")}, None),
        ):
            output, error_message = execute_task_output(
                task_id=1,
                run_id=1,
                payload={"output_type": "text", "content": "执行完成"},
                context={
                    "enforce_task_output_evidence": True,
                    "enforce_csv_artifact_quality": False,
                },
                step_row={"id": 99},
            )

        self.assertIsNone(error_message)
        self.assertIn("[证据引用]", output["content"])
        self.assertIn("step#1:tool_call:抓取黄金数据", output["content"])
        self.assertIn("tool_call_records: #12", output["content"])
        self.assertIn("gold_prices.csv", output["content"])

    def test_enforce_evidence_without_refs_downgrades_to_draft(self):
        with patch("backend.src.actions.handlers.task_output.list_task_steps_for_run", return_value=[]), patch(
            "backend.src.actions.handlers.task_output._create_task_output_record",
            side_effect=lambda task_id, payload: ({"content": payload.get("content")}, None),
        ):
            output, error_message = execute_task_output(
                task_id=1,
                run_id=1,
                payload={"output_type": "text", "content": "已成功收集数据"},
                context={
                    "enforce_task_output_evidence": True,
                    "enforce_csv_artifact_quality": False,
                },
                step_row={"id": 100},
            )

        self.assertIsNone(error_message)
        self.assertIn("[证据引用]", output["content"])
        self.assertIn("无（建议补齐 step/tool/artifact 证据", output["content"])

    def test_not_enforced_keeps_original_content(self):
        with patch(
            "backend.src.actions.handlers.task_output._create_task_output_record",
            side_effect=lambda task_id, payload: ({"content": payload.get("content")}, None),
        ):
            output, error_message = execute_task_output(
                task_id=1,
                run_id=1,
                payload={"output_type": "text", "content": "ok"},
                context={"enforce_task_output_evidence": False},
                step_row={"id": 101},
            )

        self.assertIsNone(error_message)
        self.assertEqual(output["content"], "ok")


if __name__ == "__main__":
    unittest.main()
