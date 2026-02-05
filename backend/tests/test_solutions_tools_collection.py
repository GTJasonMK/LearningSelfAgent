import os
import tempfile
import unittest
from pathlib import Path


class TestSolutionsToolsCollection(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp.name) / "agent_test.db"
        os.environ["AGENT_DB_PATH"] = str(db_path)
        os.environ["AGENT_PROMPT_ROOT"] = str(Path(self._tmp.name) / "prompt")

        import backend.src.storage as storage

        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
            self._tmp.cleanup()
        except Exception:
            pass

    def test_collect_tools_from_solutions_prioritizes_solution_tools(self):
        from backend.src.agent.support import _collect_tools_from_solutions
        from backend.src.repositories.tools_repo import ToolCreateParams, create_tool

        _ = create_tool(
            ToolCreateParams(
                name="web_fetch",
                description="fetch",
                version="0.1.0",
                metadata={"exec": {"type": "shell", "command": "echo hi"}},
            )
        )
        _ = create_tool(
            ToolCreateParams(
                name="demo_tool",
                description="demo",
                version="0.1.0",
                metadata={"exec": {"type": "shell", "command": "echo hi"}},
            )
        )

        solutions = [
            {
                "id": 1,
                "name": "sol",
                "description": "",
                "steps": [
                    "tool_call:web_fetch 抓取页面",
                    "tool_call:demo_tool 处理数据",
                ],
            }
        ]

        hint = _collect_tools_from_solutions(solutions, limit=2)
        lines = [ln.strip() for ln in (hint or "").splitlines() if ln.strip()]
        self.assertEqual(len(lines), 2)
        self.assertIn("web_fetch", lines[0])
        self.assertIn("demo_tool", lines[1])


if __name__ == "__main__":
    unittest.main()

