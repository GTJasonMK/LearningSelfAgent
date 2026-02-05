import json
import os
import tempfile
import unittest


class TestMaintenanceKnowledgeValidateTags(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_validate_tags_test.db")
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

    def _get_tags(self, skill_id: int):
        from backend.src.storage import get_connection

        with get_connection() as conn:
            row = conn.execute("SELECT tags FROM skills_items WHERE id = ?", (int(skill_id),)).fetchone()
        if not row:
            return []
        try:
            return json.loads(row["tags"] or "[]")
        except Exception:
            return []

    def test_validate_tags_dry_run_and_fix(self):
        from backend.src.repositories.skills_repo import SkillCreateParams, create_skill
        from backend.src.api.schemas import MaintenanceKnowledgeValidateTagsRequest
        from backend.src.api.system.routes_maintenance import maintenance_knowledge_validate_tags

        skill_id = create_skill(
            SkillCreateParams(
                name="SkillWithBadTags",
                tags=[" Task:001 ", "mode:THINK", "task:0", "foo:bar", "bad key:1", ""],
                version="0.1.0",
                status="approved",
            )
        )

        before = self._get_tags(skill_id)
        self.assertIn(" Task:001 ", before)

        preview = maintenance_knowledge_validate_tags(
            MaintenanceKnowledgeValidateTagsRequest(dry_run=True, fix=True, strict_keys=False)
        )
        self.assertTrue(preview.get("ok"))
        self.assertTrue(preview.get("dry_run"))
        self.assertTrue(preview.get("changed") >= 1)

        # dry_run 不改库
        after_preview = self._get_tags(skill_id)
        self.assertEqual(after_preview, before)

        applied = maintenance_knowledge_validate_tags(
            MaintenanceKnowledgeValidateTagsRequest(dry_run=False, fix=True, strict_keys=False)
        )
        self.assertTrue(applied.get("ok"))
        self.assertFalse(applied.get("dry_run"))

        after = self._get_tags(skill_id)
        # 规范化：key 小写 + int 归一化 + mode 归一化；非法 task:0 丢弃；bad key 丢弃
        self.assertIn("task:1", after)
        self.assertIn("mode:think", after)
        self.assertIn("foo:bar", after)  # 默认宽松：未知 key 保留
        self.assertNotIn("task:0", after)
        self.assertFalse(any("bad key" in t for t in after))


if __name__ == "__main__":
    unittest.main()

