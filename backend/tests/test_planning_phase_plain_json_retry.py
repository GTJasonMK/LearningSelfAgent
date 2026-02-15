import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


class TestPlanningPhasePlainJsonRetry(unittest.TestCase):
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

    def test_plan_retries_when_response_not_plain_json(self):
        from backend.src.agent.planning_phase import run_planning_phase

        fenced_plan = (
            "```json\n"
            "{\"plan\":[{\"title\":\"task_output 输出结果\",\"brief\":\"输出\",\"allow\":[\"task_output\"]}],\"artifacts\":[]}"
            "\n```"
        )
        plain_plan = (
            "{\"plan\":[{\"title\":\"task_output 输出结果\",\"brief\":\"输出\",\"allow\":[\"task_output\"]}],\"artifacts\":[]}"
        )

        with patch(
            "backend.src.agent.planning_phase.call_llm_for_text_with_id",
            side_effect=[(fenced_plan, None, 11), (plain_plan, None, 12)],
        ) as mocked:
            gen = run_planning_phase(
                task_id=1,
                run_id=1,
                message="输出一个结果",
                workdir=os.getcwd(),
                model="deepseek-chat",
                parameters={"temperature": 0.2},
                max_steps=5,
                tools_hint="(无)",
                skills_hint="(无)",
                solutions_hint="(无)",
                memories_hint="(无)",
                graph_hint="(无)",
            )
            try:
                while True:
                    next(gen)
            except StopIteration as exc:
                result = exc.value

        self.assertEqual(mocked.call_count, 2)
        self.assertEqual(result.plan_llm_id, 12)
        self.assertEqual(result.plan_titles, ["task_output 输出结果"])
        self.assertEqual(result.plan_allows, [["task_output"]])


if __name__ == "__main__":
    unittest.main()
