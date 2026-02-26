import unittest


class TestKnowledgeQueryFacadeExports(unittest.TestCase):
    def test_facade_exports_expected_symbols(self):
        from backend.src.services.knowledge import knowledge_query as q

        expected = [
            "GraphEdgeCreateParams",
            "GraphNodeCreateParams",
            "SkillCreateParams",
            "DomainCreateParams",
            "DomainUpdateParams",
            "ChatMessageCreateParams",
            "VALID_SKILL_STATUSES",
            "count_graph_nodes",
            "count_graph_edges",
            "create_graph_node",
            "create_graph_edge",
            "search_memory_fts_or_like",
            "list_skill_validations",
            "search_skills_filtered_like",
            "list_graph_extract_tasks",
            "create_llm_record",
            "list_tool_call_records",
            "summarize_tool_reuse",
            "create_search_record",
            "list_eval_records_by_task",
            "create_chat_message",
            "list_domains",
            "list_prompt_templates",
            "list_tool_hints",
            "list_domain_candidates",
            "list_skill_candidates",
            "list_memory_candidates",
            "list_graph_candidates",
        ]

        missing = [name for name in expected if not hasattr(q, name)]
        self.assertEqual([], missing)


if __name__ == "__main__":
    unittest.main()
