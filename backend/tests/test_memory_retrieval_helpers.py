import unittest
from unittest.mock import patch


class TestMemoryRetrievalHelpers(unittest.TestCase):
    def test_format_memories_for_prompt_truncates_and_includes_meta(self):
        from backend.src.agent.retrieval import _format_memories_for_prompt

        memories = [
            {
                "id": 1,
                "content": "a" * 1000,
                "memory_type": "short_term",
                "tags": ["pref", "path"],
            }
        ]
        text = _format_memories_for_prompt(memories)
        self.assertIn("1.", text)
        self.assertIn("tags=pref,path", text)
        self.assertIn("type=short_term", text)
        # 应截断并带省略号
        self.assertIn("...", text)

    def test_select_relevant_memories_uses_llm_pick_ids(self):
        from backend.src.agent import retrieval as r

        candidates = [
            {"id": 1, "content": "foo", "memory_type": "short_term", "tags": ["a"]},
            {"id": 2, "content": "bar", "memory_type": "long_term", "tags": ["b"]},
        ]

        with patch.object(r, "AGENT_MEMORY_PICK_MAX_ITEMS", 1), patch.object(
            r, "_list_memory_candidates", return_value=candidates
        ), patch.object(r, "call_openai", return_value=("{\"memory_ids\":[2]}", None, None)):
            selected = r._select_relevant_memories(
                message="test",
                model="gpt-test",
                parameters={"temperature": 0},
            )

        self.assertEqual(len(selected), 1)
        self.assertEqual(int(selected[0]["id"]), 2)


if __name__ == "__main__":
    unittest.main()
