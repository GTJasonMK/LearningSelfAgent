import unittest


class TestKnowledgeGovernanceFacadeExports(unittest.TestCase):
    def test_facade_exports_expected_symbols(self):
        from backend.src.services.knowledge import knowledge_governance as g

        expected = [
            "validate_and_fix_skill_tags",
            "dedupe_and_merge_skills",
            "rollback_knowledge_from_run",
            "auto_deprecate_low_quality_knowledge",
            "rollback_skill_to_previous_version",
            "rollback_tool_to_previous_version",
        ]

        missing = [name for name in expected if not hasattr(g, name)]
        self.assertEqual([], missing)


if __name__ == "__main__":
    unittest.main()
