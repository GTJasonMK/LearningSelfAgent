import json
import os
import tempfile
import unittest


class TestMaintenanceKnowledgeRollbackVersion(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_rollback_version_test.db")
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

    def _get_tool_row(self, tool_id: int) -> dict:
        from backend.src.storage import get_connection

        with get_connection() as conn:
            row = conn.execute("SELECT * FROM tools_items WHERE id = ?", (int(tool_id),)).fetchone()
        return dict(row) if row else {}

    def test_skill_rollback_to_previous_version(self):
        from backend.src.repositories.skills_repo import SkillCreateParams, create_skill, update_skill
        from backend.src.api.schemas import MaintenanceKnowledgeRollbackVersionRequest
        from backend.src.api.system.routes_maintenance import maintenance_knowledge_rollback_version
        from backend.src.storage import get_connection

        skill_id = create_skill(
            SkillCreateParams(
                name="OldName",
                description="old",
                tags=["domain:misc"],
                version="0.1.0",
                status="approved",
            )
        )
        _ = update_skill(
            skill_id=int(skill_id),
            name="NewName",
            description="new",
            version="0.1.1",
            change_notes="bump",
        )

        row = self._get_skill_row(skill_id)
        self.assertEqual(row.get("version"), "0.1.1")
        self.assertEqual(row.get("name"), "NewName")

        with get_connection() as conn:
            ver_row = conn.execute(
                "SELECT previous_version, next_version, previous_snapshot FROM skill_version_records WHERE skill_id = ? ORDER BY id DESC LIMIT 1",
                (int(skill_id),),
            ).fetchone()
        self.assertIsNotNone(ver_row)
        self.assertEqual(str(ver_row["previous_version"] or ""), "0.1.0")
        self.assertEqual(str(ver_row["next_version"] or ""), "0.1.1")
        self.assertTrue(str(ver_row["previous_snapshot"] or "").strip())

        # dry_run：仅预览，不改库
        preview = maintenance_knowledge_rollback_version(
            MaintenanceKnowledgeRollbackVersionRequest(kind="skill", id=int(skill_id), dry_run=True)
        )
        self.assertTrue(preview.get("ok"))
        self.assertTrue(preview.get("dry_run"))
        self.assertEqual(preview.get("from_version"), "0.1.1")
        self.assertEqual(preview.get("to_version"), "0.1.0")

        # apply：回滚到旧版本
        resp = maintenance_knowledge_rollback_version(
            MaintenanceKnowledgeRollbackVersionRequest(kind="skill", id=int(skill_id), dry_run=False, reason="test")
        )
        self.assertTrue(resp.get("ok"))
        self.assertFalse(resp.get("dry_run"))
        row2 = self._get_skill_row(skill_id)
        self.assertEqual(row2.get("version"), "0.1.0")
        self.assertEqual(row2.get("name"), "OldName")
        self.assertEqual(row2.get("description"), "old")

    def test_tool_rollback_to_previous_version_restores_null_metadata(self):
        from backend.src.repositories.tools_repo import ToolCreateParams, create_tool, update_tool
        from backend.src.api.schemas import MaintenanceKnowledgeRollbackVersionRequest
        from backend.src.api.system.routes_maintenance import maintenance_knowledge_rollback_version
        from backend.src.storage import get_connection

        tool_id = create_tool(
            ToolCreateParams(
                name="tool_demo",
                description="old",
                version="0.1.0",
                metadata=None,
            )
        )
        _ = update_tool(
            tool_id=int(tool_id),
            name=None,
            description="new",
            version="0.1.1",
            metadata={"a": 1},
            change_notes="bump",
        )

        row = self._get_tool_row(tool_id)
        self.assertEqual(row.get("version"), "0.1.1")
        self.assertEqual(row.get("description"), "new")
        self.assertTrue(row.get("metadata"))
        meta = json.loads(row["metadata"])
        self.assertEqual(meta.get("a"), 1)

        with get_connection() as conn:
            ver_row = conn.execute(
                "SELECT previous_version, next_version, previous_snapshot FROM tool_version_records WHERE tool_id = ? ORDER BY id DESC LIMIT 1",
                (int(tool_id),),
            ).fetchone()
        self.assertIsNotNone(ver_row)
        self.assertEqual(str(ver_row["previous_version"] or ""), "0.1.0")
        self.assertEqual(str(ver_row["next_version"] or ""), "0.1.1")
        self.assertTrue(str(ver_row["previous_snapshot"] or "").strip())

        resp = maintenance_knowledge_rollback_version(
            MaintenanceKnowledgeRollbackVersionRequest(kind="tool", id=int(tool_id), dry_run=False, reason="test")
        )
        self.assertTrue(resp.get("ok"))
        row2 = self._get_tool_row(tool_id)
        self.assertEqual(row2.get("version"), "0.1.0")
        self.assertEqual(row2.get("description"), "old")
        self.assertIsNone(row2.get("metadata"))


if __name__ == "__main__":
    unittest.main()

