import os
import tempfile
import unittest
from unittest.mock import patch


class TestToolCallScriptDependency(unittest.TestCase):
    def test_tool_call_blocks_when_script_missing(self):
        from backend.src.actions.handlers.tool_call import execute_tool_call

        payload = {
            "tool_name": "gold_price_fetcher",
            "input": "self_test",
            "output": "",
            "tool_metadata": {
                "exec": {
                    "command": "python backend/.agent/workspace/missing_fetch.py {input}",
                    "workdir": "/tmp",
                    "timeout_ms": 5000,
                }
            },
        }

        with patch(
            "backend.src.actions.handlers.tool_call.list_task_steps_for_run",
            return_value=[],
        ):
            with self.assertRaises(ValueError) as ctx:
                execute_tool_call(
                    task_id=1,
                    run_id=1,
                    step_row={"id": 1, "title": "tool_call:创建工具"},
                    payload=payload,
                )

        self.assertIn("脚本不存在", str(ctx.exception))

    def test_tool_call_blocks_when_script_not_bound_by_file_write(self):
        from backend.src.actions.handlers.tool_call import execute_tool_call

        with tempfile.TemporaryDirectory() as tmp:
            workspace = os.path.join(tmp, "backend", ".agent", "workspace")
            os.makedirs(workspace, exist_ok=True)
            script_path = os.path.join(workspace, "fetch_gold.py")
            with open(script_path, "w", encoding="utf-8") as handle:
                handle.write("print('ok')\n")

            payload = {
                "tool_name": "gold_price_fetcher",
                "input": "self_test",
                "output": "",
                "tool_metadata": {
                    "exec": {
                        "command": "python backend/.agent/workspace/fetch_gold.py {input}",
                        "workdir": tmp,
                        "timeout_ms": 5000,
                    }
                },
            }

            rows = [
                {
                    "id": 1,
                    "status": "done",
                    "detail": '{"type":"file_write","payload":{"path":"backend/.agent/workspace/other.py"}}',
                    "result": '{"path":"' + os.path.join(workspace, "other.py").replace("\\", "\\\\") + '","bytes":12}',
                }
            ]

            with patch(
                "backend.src.actions.handlers.tool_call.list_task_steps_for_run",
                return_value=rows,
            ):
                with self.assertRaises(ValueError) as ctx:
                    execute_tool_call(
                        task_id=1,
                        run_id=1,
                        step_row={"id": 2, "title": "tool_call:执行工具"},
                        payload=payload,
                    )

        self.assertIn("脚本依赖未绑定", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
