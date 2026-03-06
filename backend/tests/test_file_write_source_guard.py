import os
import tempfile
import unittest


class TestFileWriteSourceGuard(unittest.TestCase):
    def test_reject_csv_write_when_recent_source_is_not_csv(self):
        from backend.src.actions.handlers.file_write import execute_file_write

        with tempfile.TemporaryDirectory() as tmp:
            prev = os.getcwd()
            try:
                os.chdir(tmp)
                result, error = execute_file_write(
                    {"path": "gold_prices.csv", "content": "date,price\n2026-01-01,500\n"},
                    context={"latest_parse_input_text": "这是网页正文，不是 CSV"},
                )
            finally:
                os.chdir(prev)

        self.assertIsNone(result)
        self.assertIn("business_data_source_not_csv", str(error or ""))

    def test_reject_header_only_csv_even_when_recent_source_matches(self):
        from backend.src.actions.handlers.file_write import execute_file_write

        csv_text = "date,price\n"
        with tempfile.TemporaryDirectory() as tmp:
            prev = os.getcwd()
            try:
                os.chdir(tmp)
                result, error = execute_file_write(
                    {"path": "gold_prices.csv", "content": csv_text},
                    context={"latest_parse_input_text": csv_text},
                )
            finally:
                os.chdir(prev)

        self.assertIsNone(result)
        self.assertIn("csv_artifact_quality_failed", str(error or ""))


    def test_allow_csv_write_when_content_matches_recent_csv_source(self):
        from backend.src.actions.handlers.file_write import execute_file_write

        csv_text = "date,price\n2026-01-01,500\n2026-01-02,501\n"
        with tempfile.TemporaryDirectory() as tmp:
            prev = os.getcwd()
            try:
                os.chdir(tmp)
                result, error = execute_file_write(
                    {"path": "gold_prices.csv", "content": csv_text},
                    context={"latest_parse_input_text": csv_text},
                )
            finally:
                os.chdir(prev)

        self.assertIsNone(error)
        self.assertIsInstance(result, dict)
        self.assertTrue(str(result.get("path") or "").endswith("gold_prices.csv"))


if __name__ == "__main__":
    unittest.main()
