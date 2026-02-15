import unittest
from unittest.mock import patch


class TestToolCallWarnings(unittest.TestCase):
    def test_tool_call_empty_output_returns_warning_not_error(self):
        from backend.src.actions.handlers.tool_call import execute_tool_call

        step_row = {"id": 1, "title": "tool_call:demo"}
        payload = {
            "tool_name": "demo_tool",
            "tool_description": "demo",
            "tool_version": "0.1.0",
            "tool_metadata": {
                "exec": {"type": "shell", "command": "echo ok", "workdir": "/tmp"},
            },
            "input": "ping",
            "output": "",
        }

        with patch("backend.src.actions.handlers.tool_call.is_tool_enabled", return_value=True), patch(
            "backend.src.actions.handlers.tool_call.get_tool_by_name", return_value=None
        ), patch(
            "backend.src.actions.handlers.tool_call._enforce_tool_exec_script_dependency", return_value=None
        ), patch(
            "backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec",
            return_value=("", None),
        ), patch(
            "backend.src.actions.handlers.tool_call._create_tool_record",
            return_value={"record": {"tool_id": 1, "tool_name": "demo_tool", "input": "ping", "output": ""}},
        ):
            record, error = execute_tool_call(task_id=1, run_id=1, step_row=step_row, payload=payload)

        self.assertIsNone(error)
        self.assertIsInstance(record, dict)
        self.assertIn("warnings", record)
        self.assertIn("输出为空", str(record.get("warnings")))

    def test_web_fetch_block_marker_returns_error(self):
        from backend.src.actions.handlers.tool_call import execute_tool_call

        step_row = {"id": 1, "title": "tool_call:web_fetch 抓取页面"}
        payload = {
            "tool_name": "web_fetch",
            "tool_metadata": {
                "exec": {"type": "shell", "command": "echo ok", "workdir": "/tmp"},
            },
            "input": "https://example.com",
            "output": "",
        }

        with patch("backend.src.actions.handlers.tool_call.is_tool_enabled", return_value=True), patch(
            "backend.src.actions.handlers.tool_call.get_tool_by_name",
            return_value={"id": 1, "name": "web_fetch", "metadata": "{}"},
        ), patch(
            "backend.src.actions.handlers.tool_call._enforce_tool_exec_script_dependency", return_value=None
        ), patch(
            "backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec",
            return_value=("Edge: Too Many Requests", None),
        ), patch(
            "backend.src.actions.handlers.tool_call._create_tool_record",
            return_value={"record": {"tool_id": 1, "tool_name": "web_fetch", "input": "https://example.com", "output": "Edge: Too Many Requests"}},
        ):
            record, error = execute_tool_call(task_id=1, run_id=1, step_row=step_row, payload=payload)

        self.assertIsInstance(record, dict)
        self.assertIsNotNone(error)
        self.assertIn("web_fetch", str(error))
        self.assertIn("too_many_requests", str(error))


if __name__ == "__main__":
    unittest.main()
