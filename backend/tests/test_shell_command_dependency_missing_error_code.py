import unittest
from unittest.mock import patch


class TestShellCommandDependencyMissingErrorCode(unittest.TestCase):
    def test_module_not_found_error_is_coded(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command
        from backend.src.common.task_error_codes import extract_task_error_code

        fake_result = {
            "ok": False,
            "returncode": 1,
            "stdout": "",
            "stderr": "Traceback (most recent call last):\n  File \"x.py\", line 1, in <module>\n    import requests\nModuleNotFoundError: No module named 'requests'\n",
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
                    payload={"command": ["python", "x.py"], "workdir": ".", "timeout_ms": 1000},
                    context={
                        "enforce_shell_script_dependency": False,
                        "disallow_complex_python_c": False,
                        "auto_rewrite_complex_python_c": False,
                    },
                )

        err = str(ctx.exception)
        self.assertEqual(extract_task_error_code(err), "dependency_missing")
        self.assertIn("requests", err)

    def test_dns_resolution_error_is_coded(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command
        from backend.src.common.task_error_codes import extract_task_error_code

        fake_result = {
            "ok": False,
            "returncode": 6,
            "stdout": "",
            "stderr": "curl: (6) Could not resolve host: api.frankfurter.app",
        }

        with patch(
            "backend.src.actions.handlers.shell_command.run_shell_command",
            return_value=(fake_result, None),
        ):
            with self.assertRaises(ValueError) as ctx:
                execute_shell_command(
                    task_id=1,
                    run_id=1,
                    step_row={"id": 2},
                    payload={"command": ["curl", "https://api.frankfurter.app/latest"], "workdir": ".", "timeout_ms": 1000},
                    context={
                        "enforce_shell_script_dependency": False,
                        "disallow_complex_python_c": False,
                        "auto_rewrite_complex_python_c": False,
                    },
                )

        err = str(ctx.exception)
        self.assertEqual(extract_task_error_code(err), "dns_resolution_failed")

    def test_script_arg_contract_mismatch_is_coded(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command
        from backend.src.common.task_error_codes import extract_task_error_code

        fake_result = {
            "ok": False,
            "returncode": 1,
            "stdout": "",
            "stderr": (
                "Traceback (most recent call last):\n"
                "  File \"gold_3mo_fetch.py\", line 194, in main\n"
                "    start = dt.date.fromisoformat(argv[3])\n"
                "ValueError: Invalid isoformat string: '--fx'\n"
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
                    step_row={"id": 3},
                    payload={
                        "command": [
                            "python",
                            "backend/.agent/workspace/gold_3mo_fetch.py",
                            "--source",
                            "stooq_direct",
                            "--fx",
                            "frankfurter",
                        ],
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
        self.assertEqual(extract_task_error_code(err), "script_arg_contract_mismatch")

    def test_shell_operator_chain_is_rejected_with_code(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command
        from backend.src.common.task_error_codes import extract_task_error_code

        with self.assertRaises(ValueError) as ctx:
            execute_shell_command(
                task_id=1,
                run_id=1,
                step_row={"id": 4},
                payload={
                    "command": "python backend/.agent/workspace/fetch.py --out data.csv && python -c \"print('ok')\"",
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
        self.assertEqual(extract_task_error_code(err), "shell_operators_not_supported")
        self.assertIn("&&", err)


if __name__ == "__main__":
    unittest.main()
