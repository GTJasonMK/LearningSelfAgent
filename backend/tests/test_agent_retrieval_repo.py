import os
import tempfile
import unittest
from pathlib import Path


class TestAgentRetrievalRepo(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp.name) / "agent_test.db"
        os.environ["AGENT_DB_PATH"] = str(db_path)
        os.environ["AGENT_PROMPT_ROOT"] = str(Path(self._tmp.name) / "prompt")

        import backend.src.storage as storage

        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
            self._tmp.cleanup()
        except Exception:
            pass

    def test_skill_candidates_prefer_fts(self):
        """
        验证：候选技能优先走 FTS 相关性召回，而不是仅取“最近 N 条”。
        """
        from backend.src.repositories.agent_retrieval_repo import list_skill_candidates
        from backend.src.repositories.skills_repo import SkillCreateParams, create_skill

        sid1 = create_skill(
            SkillCreateParams(
                name="sokoban",
                description="push boxes",
                scope="tool",
                category="game",
                tags=["sokoban"],
                triggers=[],
                aliases=[],
                source_path=None,
                prerequisites=[],
                inputs=[],
                outputs=[],
                steps=[],
                failure_modes=[],
                validation=[],
                version="1.0.0",
                task_id=None,
            )
        )
        _ = create_skill(
            SkillCreateParams(
                name="zzz_latest",
                description="latest but irrelevant",
                scope="tool",
                category="misc",
                tags=["zzz"],
                triggers=[],
                aliases=[],
                source_path=None,
                prerequisites=[],
                inputs=[],
                outputs=[],
                steps=[],
                failure_modes=[],
                validation=[],
                version="1.0.0",
                task_id=None,
            )
        )

        debug = {}
        items = list_skill_candidates(limit=1, query_text="sokoban", debug=debug)
        self.assertTrue(items)
        self.assertEqual(int(items[0]["id"]), int(sid1))
        self.assertTrue(debug.get("fts_used") or debug.get("fts_available"))

    def test_memory_candidates_prefer_fts(self):
        """
        验证：候选记忆优先走 FTS 相关性召回，而不是仅取“最近 N 条”。
        """
        from backend.src.repositories.agent_retrieval_repo import list_memory_candidates
        from backend.src.repositories.memory_repo import create_memory_item

        mid1, _ = create_memory_item(
            content="python web scraper example",
            memory_type="task_result",
            tags=["python", "scraper"],
            task_id=None,
        )
        _ = create_memory_item(
            content="zzz_latest",
            memory_type="note",
            tags=["zzz"],
            task_id=None,
        )

        debug = {}
        items = list_memory_candidates(limit=1, query_text="scraper", debug=debug)
        self.assertTrue(items)
        self.assertEqual(int(items[0]["id"]), int(mid1))
        self.assertTrue(debug.get("fts_used") or debug.get("fts_available"))

    def test_list_tool_hints(self):
        from backend.src.repositories.agent_retrieval_repo import list_tool_hints
        from backend.src.repositories.tools_repo import ToolCreateParams, create_tool

        tid = create_tool(
            ToolCreateParams(
                name="demo_tool",
                description="demo",
                version="0.1.0",
                metadata={"exec": {"type": "shell", "command": "echo hi"}},
            )
        )

        items = list_tool_hints(limit=8)
        ids = [int(it["id"]) for it in items]
        self.assertIn(int(tid), ids)


if __name__ == "__main__":
    unittest.main()
