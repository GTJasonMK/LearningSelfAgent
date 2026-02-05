"""
测试技能组合功能。
"""
import os
import tempfile
import unittest


class TestSkillComposition(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_skill_compose_test.db")

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

    def test_composed_skill_result_dataclass(self):
        """测试 ComposedSkillResult 数据类。"""
        from backend.src.agent.retrieval import ComposedSkillResult

        result = ComposedSkillResult(
            success=True,
            name="复合技能",
            description="由多个技能组合而成",
            steps=["步骤1", "步骤2", "步骤3"],
            source_skill_ids=[1, 2, 3],
            domain_id="data.analyze",
        )

        self.assertTrue(result.success)
        self.assertEqual(result.name, "复合技能")
        self.assertEqual(result.description, "由多个技能组合而成")
        self.assertEqual(len(result.steps), 3)
        self.assertEqual(result.source_skill_ids, [1, 2, 3])
        self.assertEqual(result.domain_id, "data.analyze")
        self.assertIsNone(result.error)

    def test_composed_skill_result_failure(self):
        """测试失败情况的数据类。"""
        from backend.src.agent.retrieval import ComposedSkillResult

        result = ComposedSkillResult(
            success=False,
            name="",
            description="",
            steps=[],
            source_skill_ids=[],
            domain_id="misc",
            error="无可用技能进行组合",
        )

        self.assertFalse(result.success)
        self.assertEqual(result.error, "无可用技能进行组合")

    def test_compose_skills_exported(self):
        """测试函数是否正确导出。"""
        from backend.src.agent.support import (
            _compose_skills,
            ComposedSkillResult,
        )

        self.assertTrue(callable(_compose_skills))
        self.assertIsNotNone(ComposedSkillResult)

    def test_skill_compose_prompt_template_exists(self):
        """测试技能组合 prompt 模板是否存在。"""
        from backend.src.constants import SKILL_COMPOSE_PROMPT_TEMPLATE

        self.assertIsInstance(SKILL_COMPOSE_PROMPT_TEMPLATE, str)
        self.assertIn("{message}", SKILL_COMPOSE_PROMPT_TEMPLATE)
        self.assertIn("{skills}", SKILL_COMPOSE_PROMPT_TEMPLATE)

    def test_skill_create_params_new_fields(self):
        """测试 SkillCreateParams 包含新增字段。"""
        from backend.src.repositories.skills_repo import SkillCreateParams

        params = SkillCreateParams(
            name="测试技能",
            domain_id="data.analyze",
            skill_type="solution",
            status="draft",
            source_task_id=1,
            source_run_id=2,
        )

        self.assertEqual(params.name, "测试技能")
        self.assertEqual(params.domain_id, "data.analyze")
        self.assertEqual(params.skill_type, "solution")
        self.assertEqual(params.status, "draft")
        self.assertEqual(params.source_task_id, 1)
        self.assertEqual(params.source_run_id, 2)

    def test_create_skill_with_new_fields(self):
        """测试使用新字段创建技能。"""
        from backend.src.repositories.skills_repo import (
            SkillCreateParams,
            create_skill,
            get_skill,
        )

        params = SkillCreateParams(
            name="组合技能测试",
            description="测试组合功能创建的技能",
            steps=["步骤1", "步骤2"],
            domain_id="finance.stock",
            skill_type="solution",
            status="draft",
            source_task_id=100,
            source_run_id=200,
        )

        skill_id = create_skill(params)
        self.assertIsInstance(skill_id, int)
        self.assertGreater(skill_id, 0)

        skill = get_skill(skill_id=skill_id)
        self.assertIsNotNone(skill)
        self.assertEqual(skill["name"], "组合技能测试")
        self.assertEqual(skill["domain_id"], "finance.stock")
        self.assertEqual(skill["skill_type"], "solution")
        self.assertEqual(skill["status"], "draft")
        self.assertEqual(skill["source_task_id"], 100)
        self.assertEqual(skill["source_run_id"], 200)


if __name__ == "__main__":
    unittest.main()
