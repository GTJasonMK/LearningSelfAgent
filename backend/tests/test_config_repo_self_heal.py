import sqlite3
import unittest


class TestConfigRepoSelfHeal(unittest.TestCase):
    def test_repo_can_self_heal_missing_tables_on_external_connection(self):
        from backend.src.repositories.config_repo import fetch_llm_store_config, fetch_permissions_store

        # 模拟“脚本/测试”场景：直接 sqlite3.connect，未经过 storage.get_connection 初始化。
        conn = sqlite3.connect(":memory:")
        try:
            cfg = fetch_llm_store_config(conn=conn)
            self.assertIsInstance(cfg, dict)
            self.assertIn("provider", cfg)
            self.assertIn("api_key", cfg)
            self.assertIn("base_url", cfg)
            self.assertIn("model", cfg)
            self.assertEqual(cfg.get("provider"), "rightcode")
            self.assertEqual(cfg.get("base_url"), "https://right.codes/codex/v1")
            self.assertEqual(cfg.get("model"), "gpt-5.2")

            allowed_paths, allowed_ops, disabled_actions, disabled_tools = fetch_permissions_store(conn=conn)
            self.assertTrue(str(allowed_paths).strip().startswith("["))
            self.assertTrue(str(allowed_ops).strip().startswith("["))
            self.assertTrue(str(disabled_actions).strip().startswith("["))
            self.assertTrue(str(disabled_tools).strip().startswith("["))

            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='config_store'",
            ).fetchone()
            self.assertTrue(row)
        finally:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    unittest.main()

