import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import patch


class TestSolutionRetrievalBySkillTags(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_test.db")

        os.environ["AGENT_DB_PATH"] = self._db_path
        os.environ["AGENT_PROMPT_ROOT"] = os.path.join(self._tmpdir.name, "prompt")
        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
        except Exception:
            pass
        self._tmpdir.cleanup()

    def _now_iso(self):
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def test_select_relevant_solutions_prefers_skill_tag_matches_and_falls_back_when_llm_pick_fails(self):
        from backend.src.storage import get_connection
        from backend.src.agent.retrieval import _select_relevant_solutions

        now = self._now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO skills_items (name, created_at, description, tags, steps, skill_type, status, domain_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "demo_solution",
                    now,
                    "demo",
                    json.dumps(["skill:111"], ensure_ascii=False),
                    json.dumps(["tool_call:web_fetch 抓取"], ensure_ascii=False),
                    "solution",
                    "approved",
                    "data",
                ),
            )
            solution_id = int(cursor.lastrowid)

        with patch(
            "backend.src.agent.retrieval.call_openai",
            return_value=("", None, "boom"),
        ):
            solutions = _select_relevant_solutions(
                message="随便问问",
                skills=[{"id": 111, "name": "some_skill"}],
                model="gpt-4o-mini",
                parameters={"temperature": 0},
                domain_ids=["data"],
                max_solutions=3,
                debug={},
            )

        self.assertTrue(solutions, "应返回至少一个方案")
        ids = [int(s.get("id")) for s in solutions if s.get("id") is not None]
        self.assertIn(solution_id, ids)


if __name__ == "__main__":
    unittest.main()

