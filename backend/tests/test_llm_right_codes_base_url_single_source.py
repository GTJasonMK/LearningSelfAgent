import unittest
from pathlib import Path


class TestLlmRightCodesBaseUrlSingleSource(unittest.TestCase):
    def test_right_codes_base_url_literal_should_only_live_in_constant_module(self):
        needle = "https://right.codes/codex/v1"
        root = Path("backend/src")
        files = sorted(root.rglob("*.py"))
        hits = []

        for file_path in files:
            text = file_path.read_text(encoding="utf-8")
            if needle in text:
                hits.append(str(file_path))

        self.assertEqual(
            ["backend/src/constants/llm_config.py"],
            hits,
            msg=f"right.codes 默认基址不应散落硬编码，当前命中: {hits}",
        )


if __name__ == "__main__":
    unittest.main()
