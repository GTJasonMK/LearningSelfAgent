import json
import os
import tempfile
import unittest


class TestMaintenanceKnowledgeRollback(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_rollback_test.db")
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

    def _get_skill_status(self, skill_id: int) -> str:
        from backend.src.storage import get_connection

        with get_connection() as conn:
            row = conn.execute("SELECT status FROM skills_items WHERE id = ?", (int(skill_id),)).fetchone()
        return str(row["status"] or "") if row else ""

    def _get_tool_approval_status(self, tool_id: int) -> str:
        from backend.src.storage import get_connection

        with get_connection() as conn:
            row = conn.execute("SELECT metadata FROM tools_items WHERE id = ?", (int(tool_id),)).fetchone()
        if not row or not row["metadata"]:
            return ""
        try:
            meta = json.loads(row["metadata"])
        except Exception:
            return ""
        approval = meta.get("approval") if isinstance(meta, dict) else None
        if not isinstance(approval, dict):
            return ""
        return str(approval.get("status") or "")

    def test_dry_run_does_not_change_db(self):
        from backend.src.repositories.skills_repo import SkillCreateParams, create_skill
        from backend.src.repositories.tools_repo import ToolCreateParams, create_tool
        from backend.src.api.schemas import MaintenanceKnowledgeRollbackRequest
        from backend.src.api.system.routes_maintenance import maintenance_knowledge_rollback

        run_id = 123

        # skills：一个 draft（按 source_run_id 命中），一个 approved（按 tags 命中）
        draft_skill_id = create_skill(
            SkillCreateParams(
                name="DraftSkill",
                tags=[f"run:{run_id}"],
                status="draft",
                source_run_id=run_id,
            )
        )
        approved_skill_id = create_skill(
            SkillCreateParams(
                name="ApprovedSkill",
                tags=[f"run:{run_id}"],
                status="approved",
                source_run_id=None,
            )
        )

        # tools：一个由该 run 创建的 draft 工具，一个不相关工具
        created_tool_id = create_tool(
            ToolCreateParams(
                name="tool_created_by_run",
                description="desc",
                version="0.1.0",
                metadata={"approval": {"status": "draft", "created_run_id": run_id}},
            )
        )
        _other_tool_id = create_tool(
            ToolCreateParams(
                name="tool_other",
                description="desc",
                version="0.1.0",
                metadata={"approval": {"status": "draft", "created_run_id": 999}},
            )
        )

        resp = maintenance_knowledge_rollback(MaintenanceKnowledgeRollbackRequest(run_id=run_id, dry_run=True))
        self.assertTrue(resp.get("ok"))
        self.assertTrue(resp.get("dry_run"))
        # dry_run：DB 状态不应变化
        self.assertEqual(self._get_skill_status(draft_skill_id), "draft")
        self.assertEqual(self._get_skill_status(approved_skill_id), "approved")
        self.assertEqual(self._get_tool_approval_status(created_tool_id), "draft")

    def test_apply_marks_statuses(self):
        from backend.src.repositories.skills_repo import SkillCreateParams, create_skill
        from backend.src.repositories.tools_repo import ToolCreateParams, create_tool
        from backend.src.api.schemas import MaintenanceKnowledgeRollbackRequest
        from backend.src.api.system.routes_maintenance import maintenance_knowledge_rollback

        run_id = 456

        draft_skill_id = create_skill(
            SkillCreateParams(
                name="DraftSkill",
                tags=[f"run:{run_id}"],
                status="draft",
                source_run_id=run_id,
            )
        )
        approved_skill_id = create_skill(
            SkillCreateParams(
                name="ApprovedSkill",
                tags=[f"run:{run_id}"],
                status="approved",
            )
        )
        created_tool_id = create_tool(
            ToolCreateParams(
                name="tool_created_by_run",
                description="desc",
                version="0.1.0",
                metadata={"approval": {"status": "draft", "created_run_id": run_id}},
            )
        )

        resp = maintenance_knowledge_rollback(MaintenanceKnowledgeRollbackRequest(run_id=run_id, dry_run=False))
        self.assertTrue(resp.get("ok"))
        self.assertFalse(resp.get("dry_run"))

        # apply：draft -> abandoned；approved -> deprecated；tool approval -> rejected
        self.assertEqual(self._get_skill_status(draft_skill_id), "abandoned")
        self.assertEqual(self._get_skill_status(approved_skill_id), "deprecated")
        self.assertEqual(self._get_tool_approval_status(created_tool_id), "rejected")


if __name__ == "__main__":
    unittest.main()

