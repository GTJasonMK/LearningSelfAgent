import os
import tempfile
import unittest
from pathlib import Path


class TestSkillsSyncPreservesPhase2Fields(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_test.db")
        self._skills_dir = Path(self._tmpdir.name) / "skills"
        self._skills_dir.mkdir(parents=True, exist_ok=True)

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

    def test_sync_preserves_domain_and_skill_type_and_status(self):
        from backend.src.services.skills.skills_sync import sync_skills_from_files
        from backend.src.storage import get_connection

        solution_dir = self._skills_dir / "solution"
        solution_dir.mkdir(parents=True, exist_ok=True)
        file_path = solution_dir / "demo.md"

        file_path.write_text(
            "---\n"
            "name: demo_solution\n"
            "description: demo\n"
            "category: solution\n"
            "domain_id: finance.stock\n"
            "skill_type: solution\n"
            "status: approved\n"
            "source_task_id: 123\n"
            "source_run_id: 456\n"
            "tags: [skill:1, tool:2]\n"
            "steps:\n"
            "  - tool_call:web_fetch 抓取\n"
            "  - task_output 输出结果\n"
            "---\n"
            "\n"
            "# demo_solution\n"
            "\n"
            "body\n",
            encoding="utf-8",
        )

        result = sync_skills_from_files(base_dir=self._skills_dir, prune=False)
        self.assertEqual(result.get("inserted"), 1)

        with get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM skills_items WHERE source_path = ? LIMIT 1",
                ("solution/demo.md",),
            ).fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(row["skill_type"], "solution")
        self.assertEqual(row["status"], "approved")
        self.assertEqual(row["domain_id"], "finance.stock")
        self.assertEqual(int(row["source_task_id"]), 123)
        self.assertEqual(int(row["source_run_id"]), 456)


if __name__ == "__main__":
    unittest.main()

