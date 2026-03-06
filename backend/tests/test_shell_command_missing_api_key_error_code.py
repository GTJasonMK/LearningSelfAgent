import unittest
from unittest.mock import patch


class TestShellCommandMissingApiKeyErrorCode(unittest.TestCase):
    def test_missing_api_key_is_coded(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command
        from backend.src.common.task_error_codes import extract_task_error_code

        fake_result = {
            "ok": False,
            "returncode": 1,
            "stdout": "",
            "stderr": (
                "RuntimeError: FX API unsuccessful: {'success': False, "
                "'error': {'code': 101, 'type': 'missing_access_key', "
                "'info': 'You have not supplied an API Access Key.'}}"
            ),
        }

        with patch(
            "backend.src.actions.handlers.shell_command.run_shell_command",
            return_value=(fake_result, None),
        ):
            with self.assertRaises(ValueError) as ctx:
                execute_shell_command(
                    task_id=1,
                    run_id=1,
                    step_row={"id": 1},
                    payload={
                        "command": ["python", "x.py"],
                        "workdir": ".",
                        "timeout_ms": 1000,
                    },
                    context={
                        "enforce_shell_script_dependency": False,
                        "disallow_complex_python_c": False,
                        "auto_rewrite_complex_python_c": False,
                    },
                )

        err = str(ctx.exception)
        self.assertEqual(extract_task_error_code(err), "missing_api_key")
        self.assertIn("API Key", err)


if __name__ == "__main__":
    unittest.main()
