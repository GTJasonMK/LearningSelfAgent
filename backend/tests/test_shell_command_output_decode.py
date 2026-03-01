import unittest
from types import SimpleNamespace
from unittest.mock import patch


class TestShellCommandOutputDecode(unittest.TestCase):
    def test_non_utf8_subprocess_output_is_decoded_without_exception(self):
        from backend.src.services.execution.shell_command import run_shell_command

        with patch(
            "backend.src.services.execution.shell_command.has_exec_permission",
            return_value=True,
        ), patch(
            "backend.src.services.execution.shell_command.subprocess.run",
            return_value=SimpleNamespace(stdout=b"\x81abc", stderr=b"\xff", returncode=1),
        ):
            result, error_message = run_shell_command(
                {
                    "command": "echo test",
                    "workdir": "/tmp",
                    "timeout_ms": 5000,
                    "stdin": "hello",
                }
            )

        self.assertIsNone(error_message)
        self.assertIsInstance(result, dict)
        self.assertIsInstance(result.get("stdout"), str)
        self.assertIsInstance(result.get("stderr"), str)
        self.assertIn("abc", str(result.get("stdout")))
        self.assertFalse(bool(result.get("ok")))


if __name__ == "__main__":
    unittest.main()
