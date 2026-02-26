import unittest
from unittest.mock import patch


class TestToolCallErrorCodes(unittest.TestCase):
    def test_tool_call_tls_handshake_error_is_coded(self):
        from backend.src.actions.handlers.tool_call import execute_tool_call
        from backend.src.common.task_error_codes import extract_task_error_code

        step_row = {"id": 1, "title": "tool_call:web_fetch 抓取页面"}
        payload = {
            "tool_name": "web_fetch",
            "tool_metadata": {"exec": {"type": "shell", "command": "curl -fsSL https://example.com", "workdir": "/tmp"}},
            "input": "https://example.com",
            "output": "",
        }

        with patch("backend.src.actions.handlers.tool_call.is_tool_enabled", return_value=True), patch(
            "backend.src.actions.handlers.tool_call.get_tool_by_name",
            return_value={"id": 1, "name": "web_fetch", "metadata": "{}"},
        ), patch(
            "backend.src.actions.handlers.tool_call._enforce_tool_exec_script_dependency",
            return_value=None,
        ), patch(
            "backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec",
            return_value=(None, "工具执行失败: curl: (35) schannel: failed to receive handshake, SSL/TLS connection failed"),
        ):
            with self.assertRaises(ValueError) as ctx:
                execute_tool_call(task_id=1, run_id=1, step_row=step_row, payload=payload)

        message = str(ctx.exception)
        self.assertEqual(extract_task_error_code(message), "tls_handshake_failed")
        self.assertIn("SSL/TLS", message)


if __name__ == "__main__":
    unittest.main()

