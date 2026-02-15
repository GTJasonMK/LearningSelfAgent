import os
import tempfile
import unittest


class TestFtsSelfHeal(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_fts_heal_test.db")

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

    def _break_fts_shadow_tables(self):
        from backend.src.storage import get_connection

        # 直接清空 shadow tables 会导致 “vtable constructor failed”
        # 该行为用于模拟 scripts/reset_agent_data.py 的历史误操作路径。
        with get_connection() as conn:
            conn.execute("DELETE FROM memory_items_fts_config")
            conn.execute("DELETE FROM memory_items_fts_data")
            conn.execute("DELETE FROM memory_items_fts_idx")
            conn.execute("DELETE FROM memory_items_fts_docsize")

            conn.execute("DELETE FROM skills_items_fts_config")
            conn.execute("DELETE FROM skills_items_fts_data")
            conn.execute("DELETE FROM skills_items_fts_idx")
            conn.execute("DELETE FROM skills_items_fts_docsize")

    def test_init_db_disables_triggers_when_fts_is_broken(self):
        import backend.src.storage as storage
        from backend.src.storage import get_connection
        from backend.src.services.search.fts_search import fts_table_exists

        self._break_fts_shadow_tables()

        # 重新执行 init_db（模拟应用重启触发 migrations）
        storage.reset_db_cache()
        storage.init_db()

        with get_connection() as conn:
            # FTS 虚拟表仍在 sqlite_master，但已不可用：fts_table_exists 必须返回 False，查询侧回退 LIKE。
            self.assertFalse(fts_table_exists(conn, "memory_items_fts"))
            self.assertFalse(fts_table_exists(conn, "skills_items_fts"))

            # 关键：写入主表不应再被 FTS trigger 阻塞（否则 memory/skills 无法落库，Agent 自我进化链路会断）
            conn.execute(
                "INSERT INTO memory_items (content, created_at, memory_type, tags, task_id) VALUES (?, ?, ?, ?, ?)",
                ("hello", "2026-01-01T00:00:00Z", "short_term", '["t"]', None),
            )
            conn.execute(
                "INSERT INTO skills_items (name, created_at, description, scope, category, tags, triggers, aliases, source_path, prerequisites, inputs, outputs, steps, failure_modes, validation, version, task_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "skill_demo",
                    "2026-01-01T00:00:00Z",
                    "desc",
                    None,
                    "misc",
                    "[]",
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

            triggers = [
                r["name"]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='trigger' AND name IN ("
                    "'memory_items_ai','memory_items_ad','memory_items_au',"
                    "'skills_items_ai','skills_items_ad','skills_items_au'"
                    ")"
                ).fetchall()
            ]
            self.assertEqual(triggers, [], "FTS broken -> triggers should be dropped to keep writes working")


if __name__ == "__main__":
    unittest.main()

