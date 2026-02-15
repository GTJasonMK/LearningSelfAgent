import os
import tempfile
import unittest
from unittest.mock import patch

from backend.src.actions.handlers.task_output import execute_task_output


class TestTaskOutputCsvQualityGate(unittest.TestCase):
    def test_warn_placeholder_csv_output_when_hard_fail_disabled(self):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = os.path.join(tmp, "gold_prices.csv")
            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write("日期,价格(元/克)\n")
                for day in range(1, 12):
                    handle.write(f"2026-01-{day:02d},暂无数据\n")

            rows = [
                {
                    "id": 1,
                    "status": "done",
                    "title": "file_write:gold_prices.csv",
                    "detail": '{"type":"file_write","payload":{"path":"gold_prices.csv"}}',
                    "result": '{"path":"' + csv_path.replace("\\", "\\\\") + '","bytes":200}',
                }
            ]

            with patch("backend.src.actions.handlers.task_output.list_task_steps_for_run", return_value=rows), patch(
                "backend.src.actions.handlers.task_output._create_task_output_record",
                side_effect=lambda task_id, payload: ({"content": payload.get("content")}, None),
            ):
                output, error_message = execute_task_output(
                    task_id=1,
                    run_id=1,
                    payload={"output_type": "text", "content": "已完成"},
                    context={
                        "enforce_task_output_evidence": True,
                        "enforce_csv_artifact_quality": True,
                        "enforce_csv_artifact_quality_hard_fail": False,
                    },
                    step_row={"id": 10},
                )

        self.assertIsNone(error_message)
        self.assertIsInstance(output, dict)
        text = str(output.get("content") or "")
        self.assertIn("CSV 产物质量校验未通过", text)

    def test_fail_placeholder_csv_output_when_hard_fail_enabled(self):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = os.path.join(tmp, "gold_prices.csv")
            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write("日期,价格(元/克)\n")
                for day in range(1, 6):
                    handle.write(f"2026-01-{day:02d},暂无数据\n")

            rows = [
                {
                    "id": 1,
                    "status": "done",
                    "title": "file_write:gold_prices.csv",
                    "detail": '{"type":"file_write","payload":{"path":"gold_prices.csv"}}',
                    "result": '{"path":"' + csv_path.replace("\\", "\\\\") + '","bytes":120}',
                }
            ]

            with patch("backend.src.actions.handlers.task_output.list_task_steps_for_run", return_value=rows), patch(
                "backend.src.actions.handlers.task_output._create_task_output_record",
                side_effect=lambda task_id, payload: ({"content": payload.get("content")}, None),
            ):
                output, error_message = execute_task_output(
                    task_id=1,
                    run_id=1,
                    payload={"output_type": "text", "content": "已完成"},
                    context={
                        "enforce_task_output_evidence": True,
                        "enforce_csv_artifact_quality": True,
                        "enforce_csv_artifact_quality_hard_fail": True,
                    },
                    step_row={"id": 10},
                )

        self.assertIsNone(output)
        self.assertIsInstance(error_message, str)
        self.assertIn("csv_artifact_quality_failed", error_message)

    def test_pass_valid_numeric_csv_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = os.path.join(tmp, "gold_prices.csv")
            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write("日期,价格(元/克)\n")
                for day in range(1, 16):
                    handle.write(f"2026-01-{day:02d},{680 + day * 0.5:.2f}\n")

            rows = [
                {
                    "id": 1,
                    "status": "done",
                    "title": "file_write:gold_prices.csv",
                    "detail": '{"type":"file_write","payload":{"path":"gold_prices.csv"}}',
                    "result": '{"path":"' + csv_path.replace("\\", "\\\\") + '","bytes":300}',
                }
            ]

            with patch("backend.src.actions.handlers.task_output.list_task_steps_for_run", return_value=rows), patch(
                "backend.src.actions.handlers.task_output._create_task_output_record",
                side_effect=lambda task_id, payload: ({"content": payload.get("content")}, None),
            ):
                output, error_message = execute_task_output(
                    task_id=1,
                    run_id=1,
                    payload={"output_type": "text", "content": "已完成"},
                    context={
                        "enforce_task_output_evidence": True,
                        "enforce_csv_artifact_quality": True,
                    },
                    step_row={"id": 11},
                )

        self.assertIsNone(error_message)
        self.assertIsInstance(output, dict)
        self.assertIn("证据引用", str(output.get("content") or ""))

if __name__ == "__main__":
    unittest.main()
