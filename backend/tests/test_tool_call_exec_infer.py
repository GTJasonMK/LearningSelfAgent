import os
import tempfile
import unittest
from unittest.mock import patch


class TestToolCallExecInfer(unittest.TestCase):
    def test_infer_exec_from_workspace_script_when_exec_missing(self):
        from backend.src.actions.handlers.tool_call import execute_tool_call

        with tempfile.TemporaryDirectory() as tmp:
            workspace = os.path.join(tmp, "backend", ".agent", "workspace")
            os.makedirs(workspace, exist_ok=True)
            script_path = os.path.join(workspace, "demo_tool.py")
            with open(script_path, "w", encoding="utf-8") as handle:
                handle.write("print('inferred_ok')\n")

            payload = {
                "tool_name": "demo_tool",
                "input": "self_test",
                "output": "",
                "tool_metadata": {"exec": {"workdir": tmp}},
            }
            rows = [
                {
                    "id": 1,
                    "status": "done",
                    "detail": '{"type":"file_write","payload":{"path":"backend/.agent/workspace/demo_tool.py"}}',
                    "result": '{"path":"' + script_path.replace('\\', '\\\\') + '","bytes":32}',
                }
            ]
            with patch(
                "backend.src.actions.handlers.tool_call.list_task_steps_for_run",
                return_value=rows,
            ):
                record, err = execute_tool_call(
                    task_id=11,
                    run_id=22,
                    step_row={"id": 7, "title": "tool_call:自测 demo_tool"},
                    payload=payload,
                )

        self.assertIsNone(err)
        self.assertIsInstance(record, dict)
        self.assertIn("inferred_ok", str(record.get("output") or ""))


if __name__ == "__main__":
    unittest.main()
