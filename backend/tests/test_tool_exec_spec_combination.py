import unittest
from unittest.mock import patch


class TestToolExecSpecCombination(unittest.TestCase):
    def test_command_string_plus_args_appends_args_instead_of_executing_args(self):
        # 回归：模型常输出 exec.command(字符串) + exec.args(仅包含输入参数)。
        # 期望行为：将 args 追加到 command 解析后的 token 后面，而不是把 args 当作“可执行文件”直接运行。
        from backend.src.actions.handlers.tool_call import _execute_tool_with_exec_spec

        calls = []

        def fake_run_shell_command(payload: dict):
            calls.append(payload)
            return {"stdout": "ok\n", "stderr": "", "returncode": 0, "ok": True}, None

        exec_spec = {
            "type": "shell",
            "command": 'python -c "print(123)"',
            "args": ["GC=F", "3mo", "1d"],
            "timeout_ms": 1000,
            "workdir": "/tmp",
        }

        with patch("backend.src.actions.handlers.tool_call.run_shell_command", fake_run_shell_command):
            out, err = _execute_tool_with_exec_spec(exec_spec, "GC=F 3mo 1d")

        self.assertIsNone(err)
        self.assertEqual(out, "ok")
        self.assertTrue(calls, "run_shell_command should be called once")

        cmd = calls[0].get("command")
        self.assertIsInstance(cmd, list)
        # 确保不是误把 args 当作命令执行（否则 cmd 会变成 ['GC=F', ...] 或类似）
        self.assertEqual(cmd[0], "python")
        self.assertIn("-c", cmd)
        self.assertEqual(cmd[-3:], ["GC=F", "3mo", "1d"])

    def test_python_command_plus_py_script_args_keeps_python_as_executable(self):
        # 回归：exec.command="python" + exec.args=["/path/to/script.py", "..."] 时，
        # args[0] 虽然是路径，但不是可执行文件；应追加到 python 后面执行。
        from backend.src.actions.handlers.tool_call import _execute_tool_with_exec_spec

        calls = []

        def fake_run_shell_command(payload: dict):
            calls.append(payload)
            return {"stdout": "ok\n", "stderr": "", "returncode": 0, "ok": True}, None

        exec_spec = {
            "type": "shell",
            "command": "python",
            "args": [r"E:\\code\\LearningSelfAgent\\test\\fetch_gold_price.py", "GC=F"],
            "timeout_ms": 1000,
            "workdir": r"E:\\code\\LearningSelfAgent",
        }

        with patch("backend.src.actions.handlers.tool_call.run_shell_command", fake_run_shell_command):
            out, err = _execute_tool_with_exec_spec(exec_spec, "GC=F")

        self.assertIsNone(err)
        self.assertEqual(out, "ok")
        cmd = calls[0].get("command")
        self.assertIsInstance(cmd, list)
        self.assertEqual(cmd[0], "python")
        self.assertEqual(cmd[1], r"E:\\code\\LearningSelfAgent\\test\\fetch_gold_price.py")
        self.assertEqual(cmd[2], "GC=F")
