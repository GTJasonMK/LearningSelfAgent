import os
import tempfile
import unittest
from pathlib import Path


class TestToolsStoreBaseDir(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._db_path = Path(self._tmp.name) / "agent_test.db"
        self._prompt_root = Path(self._tmp.name) / "prompt"

        os.environ["AGENT_DB_PATH"] = str(self._db_path)
        os.environ["AGENT_PROMPT_ROOT"] = str(self._prompt_root)

        import backend.src.storage as storage

        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
        except Exception:
            pass
        self._tmp.cleanup()

    def test_sync_tools_from_files_publishes_missing_source_path_to_base_dir(self):
        from backend.src.common.utils import now_iso
        from backend.src.services.tools.tools_store import sync_tools_from_files
        from backend.src.storage import get_connection

        created_at = now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tools_items (name, description, version, created_at, updated_at, last_used_at, metadata, source_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("my tool", "desc", "0.1.0", created_at, created_at, created_at, None, ""),
            )
            tool_id = int(cursor.lastrowid)

        base_dir = Path(self._tmp.name) / "custom_tools"
        result = sync_tools_from_files(base_dir=base_dir, prune=False)
        self.assertGreaterEqual(int(result.get("published") or 0), 1)

        with get_connection() as conn:
            row = conn.execute(
                "SELECT source_path FROM tools_items WHERE id = ?",
                (int(tool_id),),
            ).fetchone()
        source_path = str(row["source_path"] or "").strip()
        self.assertTrue(source_path)

        # 必须落到 base_dir，而不是默认 prompt_root/tools
        self.assertTrue((base_dir / source_path).exists())
        self.assertFalse((self._prompt_root / "tools" / source_path).exists())


if __name__ == "__main__":
    unittest.main()
