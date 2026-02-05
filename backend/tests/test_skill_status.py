"""
测试技能状态管理功能（draft/approved/deprecated/abandoned 生命周期）。
"""
import os
import tempfile
import unittest


class TestSkillStatusManagement(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_skill_status_test.db")

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

    def test_valid_skill_statuses_constant(self):
        """测试有效状态常量是否存在。"""
        from backend.src.repositories.skills_repo import VALID_SKILL_STATUSES

        self.assertIn("draft", VALID_SKILL_STATUSES)
        self.assertIn("approved", VALID_SKILL_STATUSES)
        self.assertIn("deprecated", VALID_SKILL_STATUSES)
        self.assertIn("abandoned", VALID_SKILL_STATUSES)
        self.assertEqual(len(VALID_SKILL_STATUSES), 4)

    def test_update_skill_status_function(self):
        """测试状态更新函数。"""
        from backend.src.repositories.skills_repo import (
            SkillCreateParams,
            create_skill,
            get_skill,
            update_skill_status,
        )

        # 创建一个 draft 技能
        params = SkillCreateParams(
            name="测试技能",
            status="draft",
        )
        skill_id = create_skill(params)

        # 验证初始状态
        skill = get_skill(skill_id=skill_id)
        self.assertEqual(skill["status"], "draft")

        # 更新为 approved
        updated = update_skill_status(skill_id=skill_id, status="approved")
        self.assertIsNotNone(updated)
        self.assertEqual(updated["status"], "approved")

        # 更新为 deprecated
        updated = update_skill_status(skill_id=skill_id, status="deprecated")
        self.assertIsNotNone(updated)
        self.assertEqual(updated["status"], "deprecated")

        # 重新激活
        updated = update_skill_status(skill_id=skill_id, status="approved")
        self.assertIsNotNone(updated)
        self.assertEqual(updated["status"], "approved")

    def test_update_skill_status_invalid(self):
        """测试无效状态值。"""
        from backend.src.repositories.skills_repo import (
            SkillCreateParams,
            create_skill,
            update_skill_status,
        )

        params = SkillCreateParams(name="测试技能")
        skill_id = create_skill(params)

        # 无效状态
        result = update_skill_status(skill_id=skill_id, status="invalid_status")
        self.assertIsNone(result)

    def test_update_skill_status_not_found(self):
        """测试不存在的技能。"""
        from backend.src.repositories.skills_repo import update_skill_status

        result = update_skill_status(skill_id=99999, status="approved")
        self.assertIsNone(result)

    def test_list_skills_by_status(self):
        """测试按状态列出技能。"""
        from backend.src.repositories.skills_repo import (
            SkillCreateParams,
            create_skill,
            list_skills_by_status,
        )

        # 创建不同状态的技能
        create_skill(SkillCreateParams(name="草稿技能1", status="draft"))
        create_skill(SkillCreateParams(name="草稿技能2", status="draft"))
        create_skill(SkillCreateParams(name="已审核技能", status="approved"))
        create_skill(SkillCreateParams(name="已废弃技能", status="deprecated"))
        create_skill(SkillCreateParams(name="已放弃技能", status="abandoned"))

        # 列出 draft
        total, rows = list_skills_by_status(status="draft")
        self.assertEqual(total, 2)

        # 列出 approved
        total, rows = list_skills_by_status(status="approved")
        self.assertEqual(total, 1)

        # 列出 deprecated
        total, rows = list_skills_by_status(status="deprecated")
        self.assertEqual(total, 1)

        # 列出 abandoned
        total, rows = list_skills_by_status(status="abandoned")
        self.assertEqual(total, 1)

    def test_list_skills_by_status_invalid(self):
        """测试无效状态值的列表查询。"""
        from backend.src.repositories.skills_repo import list_skills_by_status

        total, rows = list_skills_by_status(status="invalid")
        self.assertEqual(total, 0)
        self.assertEqual(len(rows), 0)

    def test_retrieval_excludes_draft_by_default(self):
        """测试检索默认排除 draft 状态技能。"""
        from backend.src.repositories.skills_repo import (
            SkillCreateParams,
            create_skill,
        )
        from backend.src.repositories import agent_retrieval_repo

        # 创建测试数据
        create_skill(SkillCreateParams(name="Draft测试技能", status="draft"))
        create_skill(SkillCreateParams(name="Approved测试技能", status="approved"))
        create_skill(SkillCreateParams(name="Deprecated测试技能", status="deprecated"))

        # 默认检索（不包含 draft）
        candidates = agent_retrieval_repo.list_skill_candidates(limit=100)
        names = [c["name"] for c in candidates]

        # approved 应该在结果中
        self.assertIn("Approved测试技能", names)
        # draft 不应该在结果中
        self.assertNotIn("Draft测试技能", names)
        # deprecated 也不应该在结果中
        self.assertNotIn("Deprecated测试技能", names)

    def test_retrieval_includes_draft_when_requested(self):
        """测试检索可选包含 draft 状态技能。"""
        from backend.src.repositories.skills_repo import (
            SkillCreateParams,
            create_skill,
        )
        from backend.src.repositories import agent_retrieval_repo

        # 创建测试数据
        create_skill(SkillCreateParams(name="DraftInclude测试技能", status="draft"))
        create_skill(SkillCreateParams(name="ApprovedInclude测试技能", status="approved"))

        # 包含 draft 的检索
        candidates = agent_retrieval_repo.list_skill_candidates(limit=100, include_draft=True)
        names = [c["name"] for c in candidates]

        # approved 和 draft 都应该在结果中
        self.assertIn("ApprovedInclude测试技能", names)
        self.assertIn("DraftInclude测试技能", names)


if __name__ == "__main__":
    unittest.main()
