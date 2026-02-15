import unittest
from unittest.mock import patch


class TestFileWriteAuthenticityGuard(unittest.TestCase):
    def test_warns_business_csv_when_simulated_marker_detected(self):
        from backend.src.actions.handlers.file_write import execute_file_write

        with patch(
            "backend.src.actions.handlers.file_write.write_text_file",
            return_value={"path": "/tmp/business.csv", "bytes": 16},
        ) as mocked_write:
            result, error_message = execute_file_write(
                {
                    "path": "artifacts/business.csv",
                    "content": "id,value\n1,100\n",
                },
                context={
                    "enforce_csv_artifact_quality": True,
                    "latest_parse_input_text": "以下为模拟数据，仅用于演示",
                },
            )

        self.assertIsNone(error_message)
        self.assertIsInstance(result, dict)
        self.assertIn("warnings", result)
        self.assertIn("模拟数据", str(result.get("warnings")))
        mocked_write.assert_called_once()

    def test_allows_business_csv_when_no_simulated_marker(self):
        from backend.src.actions.handlers.file_write import execute_file_write

        with patch(
            "backend.src.actions.handlers.file_write.write_text_file",
            return_value={"path": "/tmp/business.csv", "bytes": 16},
        ) as mocked_write:
            result, error_message = execute_file_write(
                {
                    "path": "artifacts/business.csv",
                    "content": "id,value\n1,100\n",
                },
                context={
                    "enforce_csv_artifact_quality": True,
                    "latest_parse_input_text": "抓取完成：来源站点返回 20 行记录",
                },
            )

        self.assertIsNone(error_message)
        self.assertIsInstance(result, dict)
        self.assertEqual(str(result.get("path") or ""), "/tmp/business.csv")
        mocked_write.assert_called_once()

    def test_non_csv_file_not_blocked_by_marker(self):
        from backend.src.actions.handlers.file_write import execute_file_write

        with patch(
            "backend.src.actions.handlers.file_write.write_text_file",
            return_value={"path": "/tmp/notes.md", "bytes": 10},
        ) as mocked_write:
            result, error_message = execute_file_write(
                {
                    "path": "notes.md",
                    "content": "mock record",
                },
                context={"latest_parse_input_text": "模拟数据"},
            )

        self.assertIsNone(error_message)
        self.assertIsInstance(result, dict)
        mocked_write.assert_called_once()


if __name__ == "__main__":
    unittest.main()
