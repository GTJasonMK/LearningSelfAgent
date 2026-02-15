"""
测试知识充分性判断功能。
"""
import os
import tempfile
import unittest
from unittest.mock import patch


class TestKnowledgeSufficiency(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_sufficiency_test.db")

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

    def test_knowledge_sufficiency_result_dataclass(self):
        """测试 KnowledgeSufficiencyResult 数据类。"""
        from backend.src.agent.retrieval import KnowledgeSufficiencyResult

        result = KnowledgeSufficiencyResult(
            sufficient=True,
            reason="有足够的技能支撑任务",
            missing_knowledge="none",
            suggestion="proceed",
            skill_count=3,
            graph_count=2,
            memory_count=5,
        )

        self.assertTrue(result.sufficient)
        self.assertEqual(result.reason, "有足够的技能支撑任务")
        self.assertEqual(result.missing_knowledge, "none")
        self.assertEqual(result.suggestion, "proceed")
        self.assertEqual(result.skill_count, 3)
        self.assertEqual(result.graph_count, 2)
        self.assertEqual(result.memory_count, 5)

    def test_knowledge_sufficiency_result_insufficient(self):
        """测试不充分情况的数据类。"""
        from backend.src.agent.retrieval import KnowledgeSufficiencyResult

        result = KnowledgeSufficiencyResult(
            sufficient=False,
            reason="缺少数据处理相关技能",
            missing_knowledge="skill",
            suggestion="compose_skills",
            skill_count=0,
            graph_count=0,
            memory_count=0,
        )

        self.assertFalse(result.sufficient)
        self.assertEqual(result.missing_knowledge, "skill")
        self.assertEqual(result.suggestion, "compose_skills")

    def test_constants_exist(self):
        """测试相关常量是否存在。"""
        from backend.src.constants import (
            KNOWLEDGE_SUFFICIENCY_MIN_SKILLS,
            KNOWLEDGE_SUFFICIENCY_MIN_GRAPH_NODES,
            KNOWLEDGE_SUFFICIENCY_PROMPT_TEMPLATE,
            STREAM_TAG_KNOWLEDGE,
        )

        self.assertIsInstance(KNOWLEDGE_SUFFICIENCY_MIN_SKILLS, int)
        self.assertIsInstance(KNOWLEDGE_SUFFICIENCY_MIN_GRAPH_NODES, int)
        self.assertIsInstance(KNOWLEDGE_SUFFICIENCY_PROMPT_TEMPLATE, str)
        self.assertIn("{message}", KNOWLEDGE_SUFFICIENCY_PROMPT_TEMPLATE)
        self.assertIn("{skills}", KNOWLEDGE_SUFFICIENCY_PROMPT_TEMPLATE)
        self.assertIn("{graph}", KNOWLEDGE_SUFFICIENCY_PROMPT_TEMPLATE)
        self.assertIn("{memories}", KNOWLEDGE_SUFFICIENCY_PROMPT_TEMPLATE)
        self.assertEqual(STREAM_TAG_KNOWLEDGE, "【知识】")

    def test_assess_function_exported(self):
        """测试函数是否正确导出。"""
        from backend.src.agent.support import (
            _assess_knowledge_sufficiency,
            KnowledgeSufficiencyResult,
        )

        self.assertTrue(callable(_assess_knowledge_sufficiency))
        self.assertIsNotNone(KnowledgeSufficiencyResult)

    def test_assess_cold_start_overrides_ask_user_to_draft(self):
        """
        冷启动回归（docs/agent 对齐）：
        - 当 skills/graph/memories 都为空且 LLM 建议 ask_user，
          如果缺失类型是 skill/methodology/tool，应改为 create_draft_skill，
          避免无意义的 pending_planning waiting 阻断主链路。
        """
        import json

        from backend.src.agent.retrieval import _assess_knowledge_sufficiency

        llm_json = json.dumps(
            {
                "sufficient": False,
                "reason": "缺少获取黄金价格数据的具体技能或工具",
                "missing_knowledge": "skill",
                "suggestion": "ask_user",
            },
            ensure_ascii=False,
        )

        with patch(
            "backend.src.agent.retrieval._cached_call_openai",
            return_value=(llm_json, 0, None),
        ):
            result = _assess_knowledge_sufficiency(
                message="收集黄金价格并保存 CSV",
                skills=[],
                graph_nodes=[],
                memories=[],
                model="base-model",
                parameters={"temperature": 0},
            )

        self.assertFalse(bool(result.sufficient))
        self.assertEqual(str(result.suggestion), "create_draft_skill")
        self.assertIn("冷启动", str(result.reason))

    def test_assess_domain_knowledge_ask_user_not_overridden(self):
        """
        ask_user 仍需保留：当缺失的是 domain_knowledge（用户约束/输入输出不完整）时，
        应允许进入 pending_planning waiting，等待用户补充。
        """
        import json

        from backend.src.agent.retrieval import _assess_knowledge_sufficiency

        llm_json = json.dumps(
            {
                "sufficient": False,
                "reason": "需要你指定目标文件路径",
                "missing_knowledge": "domain_knowledge",
                "suggestion": "ask_user",
            },
            ensure_ascii=False,
        )

        with patch(
            "backend.src.agent.retrieval._cached_call_openai",
            return_value=(llm_json, 0, None),
        ):
            result = _assess_knowledge_sufficiency(
                message="写一个文件",
                skills=[],
                graph_nodes=[],
                memories=[],
                model="base-model",
                parameters={"temperature": 0},
            )

        self.assertFalse(bool(result.sufficient))
        self.assertEqual(str(result.suggestion), "ask_user")


if __name__ == "__main__":
    unittest.main()
