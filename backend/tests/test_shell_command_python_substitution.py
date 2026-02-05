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


if __name__ == "__main__":
    unittest.main()

