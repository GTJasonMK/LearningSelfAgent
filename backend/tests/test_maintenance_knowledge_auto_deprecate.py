import json
import os
import tempfile
import unittest


class TestMaintenanceKnowledgeAutoDeprecate(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_auto_deprecate_test.db")
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

    def test_dry_run_does_not_change(self):
        from backend.src.repositories.skills_repo import SkillCreateParams, create_skill
        from backend.src.repositories.tools_repo import ToolCreateParams, create_tool
        from backend.src.repositories.tool_call_records_repo import ToolCallRecordCreateParams, create_tool_call_record
        from backend.src.api.schemas import MaintenanceKnowledgeAutoDeprecateRequest
        from backend.src.api.system.routes_maintenance import maintenance_knowledge_auto_deprecate

        tool_id = create_tool(
            ToolCreateParams(
                name="tool_ok",
                description="desc",
                version="0.1.0",
                metadata={"approval": {"status": "approved"}},
            )
        )
        skill_id = create_skill(SkillCreateParams(name="SkillOK", status="approved"))

        # 制造失败信号（reuse_status=fail）
        create_tool_call_record(
            ToolCallRecordCreateParams(
                tool_id=int(tool_id),
                task_id=None,
                skill_id=int(skill_id),
                run_id=None,
                reuse=1,
                reuse_status="fail",
                reuse_notes=None,
                input="x",
                output="y",
            )
        )
        create_tool_call_record(
            ToolCallRecordCreateParams(
                tool_id=int(tool_id),
                task_id=None,
                skill_id=int(skill_id),
                run_id=None,
                reuse=1,
                reuse_status="fail",
                reuse_notes=None,
                input="x2",
                output="y2",
            )
        )

        resp = maintenance_knowledge_auto_deprecate(
            MaintenanceKnowledgeAutoDeprecateRequest(
                since_days=30, min_calls=2, success_rate_threshold=0.6, dry_run=True
            )
        )
        self.assertTrue(resp.get("ok"))
        self.assertTrue(resp.get("dry_run"))

        self.assertEqual(self._get_skill_status(skill_id), "approved")
        self.assertEqual(self._get_tool_approval_status(tool_id), "approved")

    def test_apply_marks_deprecated_and_rejected(self):
        from backend.src.repositories.skills_repo import SkillCreateParams, create_skill
        from backend.src.repositories.tools_repo import ToolCreateParams, create_tool
        from backend.src.repositories.tool_call_records_repo import ToolCallRecordCreateParams, create_tool_call_record
        from backend.src.api.schemas import MaintenanceKnowledgeAutoDeprecateRequest
        from backend.src.api.system.routes_maintenance import maintenance_knowledge_auto_deprecate

        tool_id = create_tool(
            ToolCreateParams(
                name="tool_low_quality",
                description="desc",
                version="0.1.0",
                metadata={"approval": {"status": "approved"}},
            )
        )
        skill_id = create_skill(SkillCreateParams(name="SkillLowQuality", status="approved"))

        for i in range(3):
            create_tool_call_record(
                ToolCallRecordCreateParams(
                    tool_id=int(tool_id),
                    task_id=None,
                    skill_id=int(skill_id),
                    run_id=None,
                    reuse=1,
                    reuse_status="fail",
                    reuse_notes=None,
                    input=f"in{i}",
                    output=f"out{i}",
                )
            )

        resp = maintenance_knowledge_auto_deprecate(
            MaintenanceKnowledgeAutoDeprecateRequest(
                since_days=30, min_calls=3, success_rate_threshold=0.6, dry_run=False
            )
        )
        self.assertTrue(resp.get("ok"))
        self.assertFalse(resp.get("dry_run"))

        self.assertEqual(self._get_skill_status(skill_id), "deprecated")
        self.assertEqual(self._get_tool_approval_status(tool_id), "rejected")


if __name__ == "__main__":
    unittest.main()

