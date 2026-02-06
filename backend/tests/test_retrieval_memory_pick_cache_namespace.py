import unittest
from unittest.mock import patch


class TestRetrievalMemoryPickCacheNamespace(unittest.TestCase):
    def test_select_relevant_memories_uses_memory_pick_cache_namespace(self):
        """
        回归：_select_relevant_memories 的缓存 namespace 应为 memory_pick，
        避免误用 graph_pick 导致缓存命名与淘汰策略混淆。
        """
        from backend.src.agent import retrieval

        candidates = [
            {"id": i, "content": f"mem-{i}", "memory_type": "short", "tags": []}
            for i in range(1, 12)
        ]

        called = {"cache_namespace": None}

        def _fake_cached_call_openai(*, cache_namespace: str, prompt: str, model: str, params: dict):
            called["cache_namespace"] = cache_namespace
            return '{"memory_ids":[1,3,5]}', None, None

        with patch.object(retrieval, "_list_memory_candidates", return_value=candidates), patch.object(
            retrieval, "_cached_call_openai", side_effect=_fake_cached_call_openai
        ):
            selected = retrieval._select_relevant_memories(
                message="test", model="fake-model", parameters={}
            )

        self.assertEqual(called["cache_namespace"], "memory_pick")
        self.assertEqual([it.get("id") for it in selected], [1, 3, 5])

