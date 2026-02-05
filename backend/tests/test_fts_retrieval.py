import os
import tempfile
import unittest


class TestFtsRetrieval(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_fts_test.db")

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

    def test_list_memory_candidates_prefers_relevant_fts(self):
        from backend.src.storage import get_connection
        from backend.src.agent.retrieval import _list_memory_candidates
        from backend.src.services.search.fts_search import fts_table_exists
        from backend.src.api.utils import now_iso

        with get_connection() as conn:
            # 如果环境不支持 FTS5（极少数情况），则跳过该用例
            if not fts_table_exists(conn, "memory_items_fts"):
                self.skipTest("FTS5 not available")

            created_at = now_iso()
            # 先插入一条“老但相关”的记忆
            conn.execute(
                "INSERT INTO memory_items (content, created_at, memory_type, tags, task_id) VALUES (?, ?, ?, ?, ?)",
                ("四川 天气 信息：多云", created_at, "short_term", '["weather","四川"]', None),
            )
            # 再插入一些“新但无关”的记忆
            for i in range(40):
                conn.execute(
                    "INSERT INTO memory_items (content, created_at, memory_type, tags, task_id) VALUES (?, ?, ?, ?, ?)",
                    (f"无关内容 {i}", created_at, "short_term", '["noise"]', None),
                )

        items = _list_memory_candidates(8, query_text="查询四川天气")
        ids = [int(it.get("id")) for it in items if it.get("id") is not None]
        self.assertTrue(ids, "should return candidates")
        # 相关记忆应能被召回到候选中（否则 Agent 永远只看到最近 N 条）
        self.assertIn(1, ids)

    def test_list_skill_candidates_prefers_relevant_fts(self):
        from backend.src.storage import get_connection
        from backend.src.agent.retrieval import _list_skill_candidates
        from backend.src.services.search.fts_search import fts_table_exists
        from backend.src.api.utils import now_iso

        with get_connection() as conn:
            if not fts_table_exists(conn, "skills_items_fts"):
                self.skipTest("FTS5 not available")

            created_at = now_iso()
            # 先插入一条“老但相关”的技能
            conn.execute(
                "INSERT INTO skills_items (name, created_at, description, scope, category, tags, triggers, aliases, source_path, prerequisites, inputs, outputs, steps, failure_modes, validation, version, task_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "web_fetch",
                    created_at,
                    "抓取 URL 内容并返回文本",
                    "适用于网页内容获取",
                    "tool.web",
                    '["web","fetch"]',
                    '["抓取","下载","获取网页"]',
                    "[]",
                    None,
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "0.1.0",
                    None,
                ),
            )
            # 再插入一些无关技能
            for i in range(20):
                conn.execute(
                    "INSERT INTO skills_items (name, created_at, description, scope, category, tags, triggers, aliases, source_path, prerequisites, inputs, outputs, steps, failure_modes, validation, version, task_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        f"misc_{i}",
                        created_at,
                        "无关描述",
                        None,
                        "misc",
                        '["noise"]',
                        "[]",
                        "[]",
                        None,
                        "[]",
                        "[]",
                        "[]",
                        "[]",
                        "[]",
                        "[]",
                        "0.1.0",
                        None,
                    ),
                )

        items = _list_skill_candidates(8, query_text="帮我抓取一个网页 url")
        names = [str(it.get("name") or "") for it in items]
        self.assertTrue(names, "should return skill candidates")
        self.assertIn("web_fetch", names)
