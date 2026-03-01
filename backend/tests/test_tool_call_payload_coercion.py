import unittest
from unittest.mock import patch


class TestToolCallPayloadCoercion(unittest.TestCase):
    def test_empty_optional_ids_are_coerced_and_run_context_is_preserved(self):
        from backend.src.actions.handlers.tool_call import execute_tool_call

        payload = {
            "tool_name": "web_fetch",
            "tool_id": "",
            "task_id": "",
            "run_id": "",
            "skill_id": "",
            "input": "https://example.com",
            "output": "",
            "tool_metadata": {
                "exec": {
                    "type": "shell",
                    "command": "curl -fsSL https://example.com",
                    "workdir": "/tmp",
                }
            },
        }

        with patch("backend.src.actions.handlers.tool_call.is_tool_enabled", return_value=True), patch(
            "backend.src.actions.handlers.tool_call.get_tool_by_name",
            return_value={"id": 1, "name": "web_fetch", "metadata": "{}"},
        ), patch(
            "backend.src.actions.handlers.tool_call._enforce_tool_exec_script_dependency",
            return_value=None,
        ), patch(
            "backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec",
            return_value=('{"ok":true}', None),
        ), patch(
            "backend.src.actions.handlers.tool_call._create_tool_record",
            return_value={"record": {"id": 11, "tool_id": 1}},
        ) as create_record_mock:
            record, error = execute_tool_call(
                task_id=12,
                run_id=34,
                step_row={"id": 2, "title": "tool_call:web_fetch 抓取"},
                payload=payload,
            )

        self.assertIsNone(error)
        self.assertEqual(record["tool_id"], 1)
        called_payload = create_record_mock.call_args[0][0]
        self.assertIsNone(called_payload.get("tool_id"))
        self.assertEqual(called_payload.get("task_id"), 12)
        self.assertEqual(called_payload.get("run_id"), 34)
        self.assertIsNone(called_payload.get("skill_id"))

    def test_invalid_optional_id_raises_coded_error(self):
        from backend.src.actions.handlers.tool_call import execute_tool_call
        from backend.src.common.task_error_codes import extract_task_error_code

        with self.assertRaises(ValueError) as ctx:
            execute_tool_call(
                task_id=1,
                run_id=1,
                step_row={"id": 1, "title": "tool_call:web_fetch"},
                payload={
                    "tool_name": "web_fetch",
                    "tool_id": "abc",
                    "input": "https://example.com",
                    "output": "",
                },
            )

        self.assertEqual(extract_task_error_code(str(ctx.exception)), "invalid_action_payload")

    def test_empty_reuse_status_is_coerced_to_none(self):
        from backend.src.actions.handlers.tool_call import execute_tool_call

        payload = {
            "tool_name": "web_fetch",
            "tool_id": "",
            "task_id": "",
            "run_id": "",
            "skill_id": "",
            "reuse": "",
            "reuse_status": "",
            "reuse_notes": "",
            "tool_version": "",
            "tool_description": "",
            "input": "https://example.com",
            "output": "",
            "tool_metadata": {
                "exec": {
                    "type": "shell",
                    "command": "curl -fsSL https://example.com",
                    "workdir": "/tmp",
                }
            },
        }

        with patch("backend.src.actions.handlers.tool_call.is_tool_enabled", return_value=True), patch(
            "backend.src.actions.handlers.tool_call.get_tool_by_name",
            return_value={"id": 1, "name": "web_fetch", "metadata": "{}"},
        ), patch(
            "backend.src.actions.handlers.tool_call._enforce_tool_exec_script_dependency",
            return_value=None,
        ), patch(
            "backend.src.actions.handlers.tool_call._execute_tool_with_exec_spec",
            return_value=("ok", None),
        ), patch(
            "backend.src.actions.handlers.tool_call._create_tool_record",
            return_value={"record": {"id": 12, "tool_id": 1}},
        ) as create_record_mock:
            record, error = execute_tool_call(
                task_id=12,
                run_id=34,
                step_row={"id": 2, "title": "tool_call:web_fetch 抓取"},
                payload=payload,
            )

        self.assertIsNone(error)
        self.assertEqual(record["tool_id"], 1)
        called_payload = create_record_mock.call_args[0][0]
        self.assertIsNone(called_payload.get("reuse_status"))
        self.assertIsNone(called_payload.get("reuse_notes"))
        self.assertIsNone(called_payload.get("tool_version"))
        self.assertIsNone(called_payload.get("tool_description"))


if __name__ == "__main__":
    unittest.main()
