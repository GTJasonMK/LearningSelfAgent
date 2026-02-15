import os
import tempfile
import unittest


class TestShellCommandPythonSubstitution(unittest.TestCase):
    def test_python_command_uses_sys_executable(self):
        """
        回归：命令为 ["python", ...] 时应自动替换为 sys.executable，
        避免 Windows 环境下 python 不在 PATH 导致 WinError 2。
        """
        from backend.src.services.execution.shell_command import run_shell_command

        with tempfile.TemporaryDirectory() as tmp:
            result, err = run_shell_command(
                {"command": ["python", "-c", "print(123)"], "workdir": tmp, "timeout_ms": 10000}
            )
            self.assertIsNone(err)
            self.assertIsInstance(result, dict)
            self.assertTrue(result.get("ok"))
            self.assertIn("123", str(result.get("stdout") or ""))

    def test_python_c_fixes_semicolon_before_with(self):
        """
        回归：python -c 代码落盘后，若存在 `; with open(...)` 这类复合语句，
        需要把分号替换为换行，否则会触发 SyntaxError。
        """
        from backend.src.services.execution.shell_command import run_shell_command

        with tempfile.TemporaryDirectory() as tmp:
            out_path = os.path.join(tmp, "out.txt").replace("\\", "/")
            code = (
                "import os; "
                f"path=r'{out_path}'; "
                "with open(path, 'w', encoding='utf-8') as f: f.write('ok'); "
                "print('ok')"
            )
            result, err = run_shell_command(
                {"command": ["python", "-c", code], "workdir": tmp, "timeout_ms": 10000}
            )
            self.assertIsNone(err)
            self.assertIsInstance(result, dict)
            self.assertTrue(result.get("ok"))
            self.assertIn("ok", str(result.get("stdout") or ""))
            with open(os.path.join(tmp, "out.txt"), "r", encoding="utf-8") as handle:
                self.assertEqual(handle.read(), "ok")


if __name__ == "__main__":
    unittest.main()
