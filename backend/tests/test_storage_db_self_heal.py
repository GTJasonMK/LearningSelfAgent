import os
import tempfile
import unittest
from pathlib import Path


class TestStorageDbSelfHeal(unittest.TestCase):
    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
        except Exception:
            pass

    def test_memory_db_is_initialized_per_connection(self):
        import backend.src.storage as storage
        from backend.src.repositories.config_repo import fetch_llm_store_config

        os.environ["AGENT_DB_PATH"] = ":memory:"
        storage.reset_db_cache()

        cfg = fetch_llm_store_config()
        self.assertIsInstance(cfg, dict)
        self.assertIn("provider", cfg)
        self.assertIn("model", cfg)

    def test_relative_db_path_is_stable_across_chdir(self):
        import backend.src.storage as storage

        project_root = Path(storage.__file__).resolve().parents[2]
        sandbox_dir = project_root / "backend" / ".agent" / "workspace" / "tests"
        sandbox_dir.mkdir(parents=True, exist_ok=True)

        old_cwd = os.getcwd()
        tmp = tempfile.TemporaryDirectory(dir=str(sandbox_dir))
        try:
            db_abs = Path(tmp.name) / "agent_rel.db"
            rel_db = os.path.relpath(str(db_abs), start=str(project_root))
            os.environ["AGENT_DB_PATH"] = rel_db
            storage.reset_db_cache()

            p1 = storage.resolve_db_path()
            os.chdir(str(project_root / "backend" / "src"))
            p2 = storage.resolve_db_path()

            self.assertEqual(p1, p2)
            self.assertEqual(Path(p1), (project_root / rel_db).resolve())

            with storage.get_connection() as conn:
                row = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='config_store'",
                ).fetchone()
            self.assertTrue(row)
        finally:
            try:
                os.chdir(old_cwd)
            except Exception:
                pass
            tmp.cleanup()

    def test_builtin_tool_seed_is_self_healed_after_deletion(self):
        """
        回归：用户/脚本可能在运行中删除 tools_items 的 seed 行。
        get_connection 的快路径应做轻量 seeds 兜底，避免后续规划/执行拿不到内置工具（如 web_fetch）。
        """
        import backend.src.storage as storage

        tmp = tempfile.TemporaryDirectory()
        try:
            os.environ["AGENT_DB_PATH"] = str(Path(tmp.name) / "agent_seed_heal.db")
            storage.reset_db_cache()
            storage.init_db()

            # 删除内置工具（模拟 reset 脚本或手工误删）
            with storage.get_connection() as conn:
                conn.execute("DELETE FROM tools_items WHERE name = 'web_fetch'")

            # 再次打开连接：应自动补齐 web_fetch（非 draft）
            with storage.get_connection() as conn:
                rows = conn.execute(
                    "SELECT metadata FROM tools_items WHERE name = 'web_fetch' ORDER BY id ASC",
                ).fetchall()

            self.assertTrue(rows)
        finally:
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
