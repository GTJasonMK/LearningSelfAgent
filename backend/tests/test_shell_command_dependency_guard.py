import os
import tempfile
import unittest
from unittest.mock import patch


class TestShellCommandDependencyGuard(unittest.TestCase):
    def test_block_when_script_file_missing(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command

        with tempfile.TemporaryDirectory() as tmp:
            missing_script = os.path.join(tmp, "missing_script.py")
            with self.assertRaises(ValueError) as ctx:
                execute_shell_command(
                    task_id=1,
                    run_id=1,
                    step_row={"id": 2},
                    payload={"command": ["python3", missing_script], "workdir": tmp, "timeout_ms": 10000},
                    context={
                        "enforce_shell_script_dependency": True,
                        "disallow_complex_python_c": False,
                    },
                )

        self.assertIn("脚本不存在", str(ctx.exception))

    def test_block_when_script_not_bound_by_file_write(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command

        with tempfile.TemporaryDirectory() as tmp:
            script_path = os.path.join(tmp, "tool_script.py")
            with open(script_path, "w", encoding="utf-8") as handle:
                handle.write("print('ok')\n")

            with patch(
                "backend.src.actions.handlers.shell_command.list_task_steps_for_run",
                return_value=[],
            ):
                with self.assertRaises(ValueError) as ctx:
                    execute_shell_command(
                        task_id=1,
                        run_id=1,
                        step_row={"id": 3},
                        payload={"command": ["python3", script_path], "workdir": tmp, "timeout_ms": 10000},
                        context={
                            "enforce_shell_script_dependency": True,
                            "disallow_complex_python_c": False,
                        },
                    )

        self.assertIn("脚本依赖未绑定", str(ctx.exception))

    def test_pass_when_script_bound_by_done_file_write(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command

        with tempfile.TemporaryDirectory() as tmp:
            script_path = os.path.join(tmp, "tool_script.py")
            with open(script_path, "w", encoding="utf-8") as handle:
                handle.write("print('ok')\n")

            rows = [
                {
                    "id": 1,
                    "status": "done",
                    "detail": '{"type":"file_write","payload":{"path":"tool_script.py"}}',
                    "result": '{"path":"' + script_path.replace("\\", "\\\\") + '","bytes":12}',
                }
            ]

            with patch(
                "backend.src.actions.handlers.shell_command.list_task_steps_for_run",
                return_value=rows,
            ):
                result, error_message = execute_shell_command(
                    task_id=1,
                    run_id=1,
                    step_row={"id": 4},
                    payload={"command": ["python3", script_path], "workdir": tmp, "timeout_ms": 10000},
                    context={
                        "enforce_shell_script_dependency": True,
                        "disallow_complex_python_c": False,
                    },
                )

        self.assertIsNone(error_message)
        self.assertTrue(result.get("ok"))
        self.assertIn("ok", str(result.get("stdout") or ""))


if __name__ == "__main__":
    unittest.main()
