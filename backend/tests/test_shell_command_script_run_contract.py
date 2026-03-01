import os
import tempfile
import unittest
import json
from unittest.mock import patch


class TestShellCommandScriptRunContract(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_test.db")
        os.environ["AGENT_DB_PATH"] = self._db_path
        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
        except Exception:
            pass
        self._tmpdir.cleanup()

    def test_script_run_preflight_detects_missing_required_args(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command
        from backend.src.common.task_error_codes import extract_task_error_code

        payload = {
            "script": "backend/.agent/workspace/parse_gold_price.py",
            "args": ["--input", "data/in.json"],
            "required_args": ["--input", "--output"],
            "workdir": self._tmpdir.name,
        }

        with patch(
            "backend.src.actions.handlers.shell_command.has_exec_permission",
            return_value=True,
        ):
            with self.assertRaises(ValueError) as ctx:
                execute_shell_command(
                    task_id=1,
                    run_id=1,
                    step_row={"id": 1, "title": "script_run:parse"},
                    payload=payload,
                    context={"enforce_shell_script_dependency": False},
                )

        self.assertEqual(extract_task_error_code(str(ctx.exception)), "script_args_missing")

    def test_script_run_discovers_required_args_from_argparse_source(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command
        from backend.src.common.task_error_codes import extract_task_error_code

        script_path = os.path.join(self._tmpdir.name, "need_args.py")
        with open(script_path, "w", encoding="utf-8") as handle:
            handle.write(
                "import argparse\n"
                "p=argparse.ArgumentParser()\n"
                "p.add_argument('--input', required=True)\n"
                "p.add_argument('--output', required=True)\n"
                "p.parse_args()\n"
            )

        payload = {
            "command": [".venv/bin/python", script_path],
            "workdir": self._tmpdir.name,
            "discover_required_args": True,
        }

        with patch(
            "backend.src.actions.handlers.shell_command.has_exec_permission",
            return_value=True,
        ):
            with self.assertRaises(ValueError) as ctx:
                execute_shell_command(
                    task_id=1,
                    run_id=1,
                    step_row={"id": 1, "title": "shell_command:run"},
                    payload=payload,
                    context={"enforce_shell_script_dependency": False},
                )

        self.assertEqual(extract_task_error_code(str(ctx.exception)), "script_args_missing")

    def test_script_run_parse_json_output_and_emit_context(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command

        payload = {
            "script": "backend/.agent/workspace/fetch.py",
            "args": ["--input", "x", "--output", "y"],
            "required_args": ["--input", "--output"],
            "parse_json_output": True,
            "emit_as": "latest_fetch_json",
            "workdir": self._tmpdir.name,
        }
        context = {"enforce_shell_script_dependency": False}

        with patch(
            "backend.src.actions.handlers.shell_command.has_exec_permission",
            return_value=True,
        ), patch(
            "backend.src.actions.handlers.shell_command.run_shell_command",
            return_value=(
                {
                    "stdout": '{"ok": true, "rows": 3}',
                    "stderr": "",
                    "returncode": 0,
                    "ok": True,
                },
                None,
            ),
        ):
            result, error = execute_shell_command(
                task_id=1,
                run_id=1,
                step_row={"id": 1, "title": "script_run:fetch"},
                payload=payload,
                context=context,
            )

        self.assertIsNone(error)
        self.assertIsInstance(result, dict)
        self.assertEqual((result.get("parsed_output") or {}).get("rows"), 3)
        self.assertEqual((context.get("latest_fetch_json") or {}).get("ok"), True)
        self.assertIn("script_contract", result)

    def test_script_run_missing_expected_output_returns_coded_error(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command
        from backend.src.common.task_error_codes import extract_task_error_code

        payload = {
            "script": "backend/.agent/workspace/build.py",
            "args": ["--input", "a.json", "--output", "data/out.csv"],
            "required_args": ["--input", "--output"],
            "expected_outputs": ["data/out.csv"],
            "workdir": self._tmpdir.name,
        }

        with patch(
            "backend.src.actions.handlers.shell_command.has_exec_permission",
            return_value=True,
        ), patch(
            "backend.src.actions.handlers.shell_command.run_shell_command",
            return_value=(
                {"stdout": "ok", "stderr": "", "returncode": 0, "ok": True},
                None,
            ),
        ):
            with self.assertRaises(ValueError) as ctx:
                execute_shell_command(
                    task_id=1,
                    run_id=1,
                    step_row={"id": 1, "title": "script_run:build"},
                    payload=payload,
                    context={"enforce_shell_script_dependency": False},
                )

        self.assertEqual(extract_task_error_code(str(ctx.exception)), "missing_expected_artifact")

    def test_script_run_preflight_can_autofill_required_args_from_existing_pairs(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command

        script_path = os.path.join(self._tmpdir.name, "parse_gold.py")
        with open(script_path, "w", encoding="utf-8") as handle:
            handle.write(
                "import argparse\n"
                "p=argparse.ArgumentParser()\n"
                "p.add_argument('--lbma-json', required=True)\n"
                "p.add_argument('--fx-json', required=True)\n"
                "p.add_argument('--out-csv', required=True)\n"
                "p.parse_args()\n"
            )

        payload = {
            "command": [
                ".venv/bin/python",
                script_path,
                "--input-xauusd",
                "data/xauusd.csv",
                "--input-usdcny",
                "data/usdcny.csv",
                "--output",
                "data/out.csv",
            ],
            "workdir": self._tmpdir.name,
            "discover_required_args": True,
        }

        observed = {"payload": None}

        def _fake_run_shell(p):
            observed["payload"] = dict(p or {})
            return {"stdout": "ok", "stderr": "", "returncode": 0, "ok": True}, None

        with patch(
            "backend.src.actions.handlers.shell_command.has_exec_permission",
            return_value=True,
        ), patch(
            "backend.src.actions.handlers.shell_command.run_shell_command",
            side_effect=_fake_run_shell,
        ):
            result, error = execute_shell_command(
                task_id=1,
                run_id=1,
                step_row={"id": 1, "title": "script_run:parse"},
                payload=payload,
                context={"enforce_shell_script_dependency": False},
            )

        self.assertIsNone(error)
        self.assertIsInstance(result, dict)
        cmd = (observed.get("payload") or {}).get("command") or []
        self.assertIn("--lbma-json", cmd)
        self.assertIn("--fx-json", cmd)
        self.assertIn("--out-csv", cmd)
        self.assertNotIn("--input-xauusd", cmd)
        self.assertNotIn("--input-usdcny", cmd)

    def test_script_run_preflight_can_autofill_required_args_via_llm_binding(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command

        script_path = os.path.join(self._tmpdir.name, "parse_gold_llm.py")
        with open(script_path, "w", encoding="utf-8") as handle:
            handle.write(
                "import argparse\n"
                "p=argparse.ArgumentParser()\n"
                "p.add_argument('--lbma-json', required=True)\n"
                "p.add_argument('--fx-json', required=True)\n"
                "p.add_argument('--out-csv', required=True)\n"
                "p.parse_args()\n"
            )

        payload = {
            "command": [
                ".venv/bin/python",
                script_path,
            ],
            "workdir": self._tmpdir.name,
            "discover_required_args": True,
        }

        observed = {"payload": None}

        def _fake_run_shell(p):
            observed["payload"] = dict(p or {})
            return {"stdout": "ok", "stderr": "", "returncode": 0, "ok": True}, None

        llm_response = {
            "args": [
                "--lbma-json",
                "data/xauusd.csv",
                "--fx-json",
                "data/usdcny.csv",
                "--out-csv",
                "data/out.csv",
            ],
            "reason": "映射输入与输出参数",
            "confidence": 0.93,
        }
        with patch(
            "backend.src.actions.handlers.shell_command.has_exec_permission",
            return_value=True,
        ), patch(
            "backend.src.services.llm.llm_calls.create_llm_call",
            return_value={"record": {"id": 9, "status": "success", "response": json.dumps(llm_response, ensure_ascii=False)}},
        ) as llm_mock, patch(
            "backend.src.actions.handlers.shell_command.run_shell_command",
            side_effect=_fake_run_shell,
        ):
            result, error = execute_shell_command(
                task_id=1,
                run_id=1,
                step_row={"id": 1, "title": "script_run:parse"},
                payload=payload,
                context={
                    "enforce_shell_script_dependency": False,
                    "enable_llm_script_arg_binding": True,
                },
            )

        llm_mock.assert_called_once()
        self.assertIsNone(error)
        self.assertIsInstance(result, dict)
        cmd = (observed.get("payload") or {}).get("command") or []
        self.assertIn("--lbma-json", cmd)
        self.assertIn("--fx-json", cmd)
        self.assertIn("--out-csv", cmd)
        llm_meta = (result.get("script_contract") or {}).get("llm_autofill") or {}
        self.assertEqual(llm_meta.get("status"), "applied")
        self.assertEqual(llm_meta.get("record_id"), 9)

    def test_script_run_preflight_llm_binding_invalid_response_keeps_structured_error(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command
        from backend.src.common.task_error_codes import extract_task_error_code

        script_path = os.path.join(self._tmpdir.name, "parse_gold_llm_invalid.py")
        with open(script_path, "w", encoding="utf-8") as handle:
            handle.write(
                "import argparse\n"
                "p=argparse.ArgumentParser()\n"
                "p.add_argument('--lbma-json', required=True)\n"
                "p.add_argument('--fx-json', required=True)\n"
                "p.add_argument('--out-csv', required=True)\n"
                "p.parse_args()\n"
            )

        payload = {
            "command": [
                ".venv/bin/python",
                script_path,
                "--input-xauusd",
                "data/xauusd.csv",
                "--input-usdcny",
                "data/usdcny.csv",
            ],
            "workdir": self._tmpdir.name,
            "discover_required_args": True,
        }

        with patch(
            "backend.src.actions.handlers.shell_command.has_exec_permission",
            return_value=True,
        ), patch(
            "backend.src.services.llm.llm_calls.create_llm_call",
            return_value={"record": {"id": 11, "status": "success", "response": "not-json"}},
        ), patch(
            "backend.src.actions.handlers.shell_command.run_shell_command",
        ) as run_mock:
            with self.assertRaises(ValueError) as ctx:
                execute_shell_command(
                    task_id=1,
                    run_id=1,
                    step_row={"id": 1, "title": "script_run:parse"},
                    payload=payload,
                    context={
                        "enforce_shell_script_dependency": False,
                        "enable_llm_script_arg_binding": True,
                    },
                )

        run_mock.assert_not_called()
        self.assertEqual(extract_task_error_code(str(ctx.exception)), "script_args_missing")


if __name__ == "__main__":
    unittest.main()
