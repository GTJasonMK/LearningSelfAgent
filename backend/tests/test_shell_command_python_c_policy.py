import os
import tempfile
import unittest


class TestShellCommandPythonCPolicy(unittest.TestCase):
    def test_block_complex_python_c_when_strict_enabled(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command

        complex_code = "import pathlib\npath='out.txt'\nwith open(path,'w',encoding='utf-8') as f:\n    f.write('ok')\nprint('ok')"

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError) as ctx:
                execute_shell_command(
                    task_id=1,
                    run_id=1,
                    step_row={"id": 1},
                    payload={"command": ["python3", "-c", complex_code], "workdir": tmp, "timeout_ms": 10000},
                    context={
                        "disallow_complex_python_c": True,
                        "auto_rewrite_complex_python_c": False,
                        "enforce_shell_script_dependency": False,
                    },
                )

        self.assertIn("复杂 python -c", str(ctx.exception))

    def test_auto_rewrite_complex_python_c_when_enabled(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command

        complex_code = "import pathlib\npath='out.txt'\nwith open(path,'w',encoding='utf-8') as f:\n    f.write('ok')\nprint('ok')"

        with tempfile.TemporaryDirectory() as tmp:
            context = {
                "disallow_complex_python_c": True,
                "auto_rewrite_complex_python_c": True,
                "enforce_shell_script_dependency": True,
                "agent_workspace_rel": "workspace",
            }
            result, error_message = execute_shell_command(
                task_id=2,
                run_id=3,
                step_row={"id": 5},
                payload={"command": ["python3", "-c", complex_code], "workdir": tmp, "timeout_ms": 10000},
                context=context,
            )
            output_path = os.path.join(tmp, "out.txt")
            exists_in_tmp = os.path.exists(output_path)
            auto_script = str(context.get("shell_auto_rewrite_last_script") or "")

        self.assertIsNone(error_message)
        self.assertTrue(result.get("ok"))
        self.assertIn("ok", str(result.get("stdout") or ""))
        self.assertTrue(exists_in_tmp)
        self.assertTrue(auto_script.endswith(".py"))

    def test_allow_complex_python_c_when_strict_disabled(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command

        complex_code = "import pathlib\npath='out.txt'\nwith open(path,'w',encoding='utf-8') as f:\n    f.write('ok')\nprint('ok')"

        with tempfile.TemporaryDirectory() as tmp:
            result, error_message = execute_shell_command(
                task_id=1,
                run_id=1,
                step_row={"id": 2},
                payload={"command": ["python3", "-c", complex_code], "workdir": tmp, "timeout_ms": 10000},
                context={
                    "disallow_complex_python_c": False,
                    "enforce_shell_script_dependency": False,
                },
            )
            output_path = os.path.join(tmp, "out.txt")
            exists_in_tmp = os.path.exists(output_path)

        self.assertIsNone(error_message)
        self.assertTrue(result.get("ok"))
        self.assertIn("ok", str(result.get("stdout") or ""))
        self.assertTrue(exists_in_tmp)

    def test_auto_rewrite_rejects_risky_inline_control_flow(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command

        code = (
            "import os; path='out.txt'; "
            "if True: with open(path, 'w', encoding='utf-8') as f: f.write('ok'); "
            "if os.path.exists(path): print('exists'); for i in range(2): print(i)"
        )

        with tempfile.TemporaryDirectory() as tmp:
            context = {
                "disallow_complex_python_c": True,
                "auto_rewrite_complex_python_c": True,
                "enforce_shell_script_dependency": True,
                "agent_workspace_rel": "workspace",
            }
            with self.assertRaises(ValueError) as ctx:
                execute_shell_command(
                    task_id=3,
                    run_id=4,
                    step_row={"id": 6},
                    payload={"command": ["python3", "-c", code], "workdir": tmp, "timeout_ms": 10000},
                    context=context,
                )

        self.assertIn("高风险单行控制流 python -c", str(ctx.exception))



if __name__ == "__main__":
    unittest.main()
