import os
import tempfile
import unittest
from unittest.mock import patch


class TestShellCommandValidationUrlRetry(unittest.TestCase):
    def test_validation_missing_required_columns_auto_retry_with_alias_mapping(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command

        with tempfile.TemporaryDirectory() as tmp:
            script_path = os.path.join(tmp, "validate_gold_csv.py")
            with open(script_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "import sys\n"
                    "print('validate')\n"
                )

            csv_path = os.path.join(tmp, "data", "gold.csv")
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)
            with open(csv_path, "w", encoding="utf-8", newline="") as handle:
                handle.write("date,price\n2026-02-01,680.2\n")

            call_state = {"count": 0}

            def _fake_run_shell(p):
                call_state["count"] += 1
                if call_state["count"] == 1:
                    return (
                        {
                            "stdout": "",
                            "stderr": "ERROR: Missing required columns: timestamp, price_cny_per_g",
                            "returncode": 1,
                            "ok": False,
                        },
                        None,
                    )
                command = list((p or {}).get("command") or [])
                csv_tokens = [str(item) for item in command if str(item).lower().endswith(".csv")]
                if not csv_tokens:
                    return {"stdout": "", "stderr": "missing csv arg", "returncode": 1, "ok": False}, None
                mapped_path = csv_tokens[0]
                if not os.path.isabs(mapped_path):
                    mapped_path = os.path.abspath(os.path.join(tmp, mapped_path))
                with open(mapped_path, "r", encoding="utf-8") as f:
                    header = str(f.readline() or "").strip().lower()
                if "timestamp" not in header or "price_cny_per_g" not in header:
                    return {"stdout": "", "stderr": "mapping not applied", "returncode": 1, "ok": False}, None
                return {"stdout": "ok", "stderr": "", "returncode": 0, "ok": True}, None

            with patch(
                "backend.src.actions.handlers.shell_command.has_exec_permission",
                return_value=True,
            ), patch(
                "backend.src.actions.handlers.shell_command.run_shell_command",
                side_effect=_fake_run_shell,
            ):
                result, error_message = execute_shell_command(
                    task_id=1,
                    run_id=1,
                    step_row={"id": 2, "title": "shell_command:校验CSV"},
                    payload={
                        "command": ["python3", script_path, csv_path],
                        "workdir": tmp,
                        "timeout_ms": 10000,
                    },
                    context={
                        "enforce_shell_script_dependency": False,
                        "disallow_complex_python_c": False,
                    },
                )

        self.assertIsNone(error_message)
        self.assertIsInstance(result, dict)
        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(int(call_state.get("count") or 0), 2)
        auto_retry = result.get("auto_retry") if isinstance(result, dict) else None
        self.assertIsInstance(auto_retry, dict)
        self.assertEqual(str(auto_retry.get("trigger") or ""), "missing_required_columns")
        self.assertIn("timestamp", list(auto_retry.get("required_columns") or []))
        self.assertIn("price_cny_per_g", list(auto_retry.get("required_columns") or []))

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
        self.assertIsNone(auto_retry)

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

        self.assertIn("[code=script_args_missing]", str(ctx.exception))

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



    def test_script_missing_input_file_auto_retry_with_materialized_context_file(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command

        with tempfile.TemporaryDirectory() as tmp:
            script_path = os.path.join(tmp, "parse_html.py")
            with open(script_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "import sys\n"
                    "path = sys.argv[1]\n"
                    "import os\n"
                    "if not os.path.exists(path):\n"
                    "    print(f'Error: File {path} not found', file=sys.stderr)\n"
                    "    raise SystemExit(1)\n"
                    "with open(path, 'r', encoding='utf-8') as f:\n"
                    "    text = f.read().strip()\n"
                    "print(text.splitlines()[0])\n"
                )

            result, error_message = execute_shell_command(
                task_id=1,
                run_id=1,
                step_row={"id": 2, "title": "shell_command:执行解析脚本自测"},
                payload={
                    "command": ["python3", script_path, "recent_sample.html"],
                    "workdir": tmp,
                    "timeout_ms": 10000,
                },
                context={
                    "enforce_shell_script_dependency": False,
                    "disallow_complex_python_c": False,
                    "latest_parse_input_text": "<html><body>sample</body></html>",
                },
            )

            materialized_path = os.path.join(tmp, "recent_sample.html")
            self.assertTrue(os.path.exists(materialized_path))
            with open(materialized_path, "r", encoding="utf-8") as handle:
                self.assertIn("sample", handle.read())

        self.assertIsNone(error_message)
        self.assertTrue(bool(result.get("ok")))
        auto_retry = result.get("auto_retry") if isinstance(result, dict) else None
        self.assertIsInstance(auto_retry, dict)
        self.assertEqual(str(auto_retry.get("trigger") or ""), "missing_input_file")
        self.assertTrue(str(auto_retry.get("materialized_input_path") or "").endswith("recent_sample.html"))
if __name__ == "__main__":
    unittest.main()
