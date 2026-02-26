import os
import tempfile
import unittest
from unittest.mock import patch


class TestShellCommandValidationUrlRetry(unittest.TestCase):
    def test_validation_script_missing_url_auto_retry_with_context_url(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command

        with tempfile.TemporaryDirectory() as tmp:
            script_path = os.path.join(tmp, "web_fetch.py")
            with open(script_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "import sys\n"
                    "if len(sys.argv) < 2:\n"
                    "    print('ERROR: No URL provided', file=sys.stderr)\n"
                    "    raise SystemExit(1)\n"
                    "print(sys.argv[1])\n"
                )

            rows = [
                {
                    "id": 1,
                    "status": "done",
                    "detail": '{"type":"file_write","payload":{"path":"web_fetch.py"}}',
                    "result": '{"path":"' + script_path.replace("\\", "\\\\") + '","bytes":120}',
                }
            ]

            with patch(
                "backend.src.actions.handlers.shell_command.list_task_steps_for_run",
                return_value=rows,
            ):
                result, error_message = execute_shell_command(
                    task_id=1,
                    run_id=1,
                    step_row={"id": 2, "title": "shell_command:验证web_fetch脚本"},
                    payload={"command": ["python3", script_path], "workdir": tmp, "timeout_ms": 10000},
                    context={
                        "enforce_shell_script_dependency": True,
                        "disallow_complex_python_c": False,
                        "latest_external_url": "https://data.example.org/sample.csv",
                    },
                )

        self.assertIsNone(error_message)
        self.assertTrue(result.get("ok"))
        self.assertIn("https://data.example.org/sample.csv", str(result.get("stdout") or ""))
        auto_retry = result.get("auto_retry") if isinstance(result, dict) else None
        self.assertIsInstance(auto_retry, dict)
        self.assertEqual(str(auto_retry.get("trigger") or ""), "missing_url")
        self.assertEqual(str(auto_retry.get("fallback_url") or ""), "https://data.example.org/sample.csv")
        self.assertIn("No URL provided", str(auto_retry.get("initial_stderr") or ""))

    def test_validation_script_missing_url_without_context_fails_fast(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command

        with tempfile.TemporaryDirectory() as tmp:
            script_path = os.path.join(tmp, "web_fetch.py")
            with open(script_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "import sys\n"
                    "if len(sys.argv) < 2:\n"
                    "    print('ERROR: No URL provided', file=sys.stderr)\n"
                    "    raise SystemExit(1)\n"
                    "print(sys.argv[1])\n"
                )

            rows = [
                {
                    "id": 1,
                    "status": "done",
                    "detail": '{"type":"file_write","payload":{"path":"web_fetch.py"}}',
                    "result": '{"path":"' + script_path.replace("\\", "\\\\") + '","bytes":120}',
                }
            ]

            with patch(
                "backend.src.actions.handlers.shell_command.list_task_steps_for_run",
                return_value=rows,
            ):
                with self.assertRaises(ValueError) as ctx:
                    execute_shell_command(
                        task_id=1,
                        run_id=1,
                        step_row={"id": 2, "title": "shell_command:验证web_fetch脚本"},
                        payload={"command": ["python3", script_path], "workdir": tmp, "timeout_ms": 10000},
                        context={
                            "enforce_shell_script_dependency": True,
                            "disallow_complex_python_c": False,
                        },
                    )

        self.assertIn("[code=missing_url_input]", str(ctx.exception))

    def test_script_missing_input_auto_retry_with_latest_parse_input(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command

        with tempfile.TemporaryDirectory() as tmp:
            script_path = os.path.join(tmp, "convert.py")
            with open(script_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "import sys\n"
                    "text = sys.stdin.read().strip()\n"
                    "if not text:\n"
                    "    print('错误：未提供输入数据', file=sys.stderr)\n"
                    "    raise SystemExit(1)\n"
                    "print('ok:' + text.splitlines()[0])\n"
                )

            rows = [
                {
                    "id": 1,
                    "status": "done",
                    "detail": '{"type":"file_write","payload":{"path":"convert.py"}}',
                    "result": '{"path":"' + script_path.replace("\\", "\\\\") + '","bytes":120}',
                }
            ]

            with patch(
                "backend.src.actions.handlers.shell_command.list_task_steps_for_run",
                return_value=rows,
            ):
                result, error_message = execute_shell_command(
                    task_id=1,
                    run_id=1,
                    step_row={"id": 2, "title": "shell_command:执行转换脚本"},
                    payload={"command": ["python3", script_path], "workdir": tmp, "timeout_ms": 10000},
                    context={
                        "enforce_shell_script_dependency": True,
                        "disallow_complex_python_c": False,
                        "latest_parse_input_text": "Date,Close\n2026-02-14,673.2\n",
                    },
                )

        self.assertIsNone(error_message)
        self.assertTrue(result.get("ok"))
        self.assertIn("ok:Date,Close", str(result.get("stdout") or ""))
        auto_retry = result.get("auto_retry") if isinstance(result, dict) else None
        if isinstance(auto_retry, dict):
            self.assertEqual(str(auto_retry.get("trigger") or ""), "missing_input_data")
            self.assertTrue(bool(auto_retry.get("retry_stdin_attached")))
            self.assertGreater(int(auto_retry.get("stdin_chars") or 0), 0)
        else:
            self.assertTrue(bool(result.get("auto_stdin_attached")))
            self.assertGreater(int(result.get("auto_stdin_chars") or 0), 0)

    def test_auto_attach_context_stdin_for_script_without_args(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command

        with tempfile.TemporaryDirectory() as tmp:
            script_path = os.path.join(tmp, "stdin_only.py")
            with open(script_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "import sys\n"
                    "text = sys.stdin.read().strip()\n"
                    "if not text:\n"
                    "    print('missing stdin', file=sys.stderr)\n"
                    "    raise SystemExit(1)\n"
                    "print(text.splitlines()[0])\n"
                )

            rows = [
                {
                    "id": 1,
                    "status": "done",
                    "detail": '{"type":"file_write","payload":{"path":"stdin_only.py"}}',
                    "result": '{"path":"' + script_path.replace("\\", "\\\\") + '","bytes":120}',
                }
            ]

            with patch(
                "backend.src.actions.handlers.shell_command.list_task_steps_for_run",
                return_value=rows,
            ):
                result, error_message = execute_shell_command(
                    task_id=1,
                    run_id=1,
                    step_row={"id": 3, "title": "shell_command:执行解析脚本"},
                    payload={"command": ["python3", script_path], "workdir": tmp, "timeout_ms": 10000},
                    context={
                        "enforce_shell_script_dependency": True,
                        "disallow_complex_python_c": False,
                        "latest_parse_input_text": "Date,Close\n2026-02-14,673.2\n",
                    },
                )

        self.assertIsNone(error_message)
        self.assertTrue(result.get("ok"))
        self.assertIn("Date,Close", str(result.get("stdout") or ""))
        self.assertTrue(bool(result.get("auto_stdin_attached")))
        self.assertGreater(int(result.get("auto_stdin_chars") or 0), 0)


if __name__ == "__main__":
    unittest.main()
