import json
import os
import tempfile
import unittest


class TestMaintenanceKnowledgeDedupeSkills(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_dedupe_skills_test.db")
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

    def _get_skill_row(self, skill_id: int) -> dict:
        from backend.src.storage import get_connection

        with get_connection() as conn:
            row = conn.execute("SELECT * FROM skills_items WHERE id = ?", (int(skill_id),)).fetchone()
        return dict(row) if row else {}

    def test_dedupe_by_scope_merges_and_marks_duplicates(self):
        from backend.src.repositories.skills_repo import SkillCreateParams, create_skill
        from backend.src.api.schemas import MaintenanceKnowledgeDedupeSkillsRequest
        from backend.src.api.system.routes_maintenance import maintenance_knowledge_dedupe_skills
        from backend.src.storage import get_connection

        scope = "tool:1"
        older_id = create_skill(
            SkillCreateParams(
                name="ToolSkill",
                scope=scope,
                tags=["domain:misc", "tool:1"],
                version="0.1.0",
                status="approved",
                skill_type="methodology",
            )
        )
        newer_id = create_skill(
            SkillCreateParams(
                name="ToolSkill",
                scope=scope,
                tags=["domain:misc", "extra:1"],
                version="0.1.0",
                status="approved",
                skill_type="methodology",
            )
        )

        preview = maintenance_knowledge_dedupe_skills(MaintenanceKnowledgeDedupeSkillsRequest(dry_run=True))
        self.assertTrue(preview.get("ok"))
        self.assertTrue(preview.get("dry_run"))

        # dry_run 不改库
        self.assertEqual(self._get_skill_row(older_id).get("status"), "approved")
        self.assertEqual(self._get_skill_row(newer_id).get("status"), "approved")

        applied = maintenance_knowledge_dedupe_skills(MaintenanceKnowledgeDedupeSkillsRequest(dry_run=False))
        self.assertTrue(applied.get("ok"))
        self.assertFalse(applied.get("dry_run"))
        self.assertTrue(applied.get("merged") >= 1)

        # 当前实现：canonical 取 id 最大的一条
        canonical = self._get_skill_row(newer_id)
        duplicate = self._get_skill_row(older_id)
        self.assertEqual(canonical.get("status"), "approved")
        self.assertEqual(canonical.get("version"), "0.1.1")
        self.assertEqual(duplicate.get("status"), "deprecated")

        merged_tags = json.loads(canonical.get("tags") or "[]")
        self.assertIn("tool:1", merged_tags)
        self.assertIn("extra:1", merged_tags)

        # 版本记录应存在（bump version 触发快照）
        with get_connection() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM skill_version_records WHERE skill_id = ?",
                (int(newer_id),),
            ).fetchone()
        self.assertTrue(int(row["c"] or 0) >= 1)


if __name__ == "__main__":
    unittest.main()

