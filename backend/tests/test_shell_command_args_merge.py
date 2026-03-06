import sys
import unittest
from unittest.mock import patch


class TestShellCommandArgsMerge(unittest.TestCase):
    def test_run_shell_command_merges_payload_args_when_script_field_missing(self):
        from backend.src.services.execution.shell_command import run_shell_command

        with patch("backend.src.services.execution.shell_command.has_exec_permission", return_value=True):
            result, error = run_shell_command(
                {
                    "command": sys.executable,
                    "args": ["-c", "import sys; print(sys.argv[1])", "hello"],
                    "workdir": ".",
                    "timeout_ms": 5000,
                }
            )

        self.assertIsNone(error)
        self.assertTrue(isinstance(result, dict))
        self.assertEqual(result.get("returncode"), 0)
        self.assertEqual(str(result.get("stdout") or "").strip(), "hello")


if __name__ == "__main__":
    unittest.main()
