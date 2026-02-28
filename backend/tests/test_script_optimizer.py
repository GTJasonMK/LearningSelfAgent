import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


class TestScriptOptimizer(unittest.TestCase):
    def test_run_optimizer_analyze_returns_structured_result(self):
        import scripts.script_optimizer as script_optimizer

        with tempfile.TemporaryDirectory() as tmp:
            file_path = Path(tmp) / "demo.py"
            file_path.write_text("x = 1\nprint(x)\n", encoding="utf-8")

            llm_json = (
                '{'
                '"status":"ok",'
                '"summary":"存在可读性优化点",'
                '"issues":[{"type":"style","severity":"low","line":1,"message":"变量名过短"}],'
                '"suggestions":[{"title":"命名优化","detail":"将 x 改为 value"}]'
                '}'
            )
            payload = {
                "target_paths": [str(file_path)],
                "mode": "analyze",
                "languages": ["python"],
            }

            with patch("scripts.script_optimizer.call_openai", return_value=(llm_json, None, None)):
                result = script_optimizer.run_optimizer(payload)

        self.assertEqual("ok", result.get("status"))
        self.assertEqual(1, len(result.get("files") or []))
        file_item = (result.get("files") or [])[0]
        self.assertEqual("ok", file_item.get("status"))
        self.assertTrue((file_item.get("issues") or []))
        self.assertTrue((file_item.get("suggestions") or []))
        self.assertEqual([], result.get("errors") or [])

    def test_run_optimizer_apply_patch_writes_backup_and_file(self):
        import scripts.script_optimizer as script_optimizer

        with tempfile.TemporaryDirectory() as tmp:
            file_path = Path(tmp) / "demo.py"
            before = "value = 1\nprint(value)\n"
            after = "value = 1\nprint(f'value={value}')\n"
            file_path.write_text(before, encoding="utf-8")

            llm_json = (
                '{'
                '"status":"ok",'
                '"summary":"输出格式优化",'
                '"issues":[{"type":"style","severity":"low","line":2,"message":"可读性一般"}],'
                '"suggestions":[{"title":"格式化输出","detail":"使用 f-string"}],'
                '"optimized_code":"value = 1\\nprint(f\'value={value}\')\\n"'
                '}'
            )
            payload = {
                "target_paths": [str(file_path)],
                "mode": "apply_patch",
                "languages": ["python"],
            }

            with patch("scripts.script_optimizer.call_openai", return_value=(llm_json, None, None)):
                result = script_optimizer.run_optimizer(payload)

            new_text = file_path.read_text(encoding="utf-8")
            backups = list(file_path.parent.glob(f"{file_path.name}.bak-*"))

        self.assertEqual("ok", result.get("status"))
        self.assertEqual(after, new_text)
        self.assertEqual(1, len(backups))
        self.assertEqual(1, len(result.get("applied") or []))
        applied = (result.get("applied") or [])[0]
        self.assertTrue(str(applied.get("backup_path") or "").endswith(backups[0].name))
        self.assertGreater(int(applied.get("changed_lines") or 0), 0)

    def test_run_optimizer_missing_file_returns_failed(self):
        import scripts.script_optimizer as script_optimizer

        with tempfile.TemporaryDirectory() as tmp:
            missing_path = Path(tmp) / "missing.py"
            payload = {"target_paths": [str(missing_path)], "mode": "analyze"}
            result = script_optimizer.run_optimizer(payload)

        self.assertEqual("failed", result.get("status"))
        errors = result.get("errors") or []
        self.assertTrue(errors)
        self.assertEqual("file_not_found", errors[0].get("code"))


if __name__ == "__main__":
    unittest.main()
