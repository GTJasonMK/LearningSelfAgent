import unittest
from types import SimpleNamespace
from unittest.mock import patch


class TestShellCommandWindowsBuiltinPathNormalize(unittest.TestCase):
    def test_dir_builtin_normalizes_relative_path_slashes(self):
        from backend.src.services.execution.shell_command import run_shell_command

        with patch(
            "backend.src.services.execution.shell_command.os.name",
            "nt",
        ), patch(
            "backend.src.services.execution.shell_command.has_exec_permission",
            return_value=True,
        ), patch(
            "backend.src.services.execution.shell_command.subprocess.run",
            return_value=SimpleNamespace(stdout="ok", stderr="", returncode=0),
        ) as mocked_run:
            result, error_message = run_shell_command(
                {
                    "command": 'dir /b "backend/.agent/workspace/fetch_gold_price.py"',
                    "workdir": "E:\\code\\LearningSelfAgent",
                    "timeout_ms": 10000,
                }
            )

        self.assertIsNone(error_message)
        self.assertTrue(result.get("ok"))
        self.assertTrue(mocked_run.called)

        command_args = mocked_run.call_args.args[0]
        self.assertEqual(command_args[0], "cmd.exe")
        self.assertEqual(command_args[1], "/c")
        self.assertIn("dir /b", command_args[2].lower())
        self.assertIn("backend\\.agent\\workspace\\fetch_gold_price.py", command_args[2])


if __name__ == "__main__":
    unittest.main()
