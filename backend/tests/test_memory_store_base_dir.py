import os
import tempfile
import unittest
from pathlib import Path


class TestMemoryStoreBaseDir(unittest.TestCase):
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

    def test_sync_memory_from_files_publishes_missing_uid_to_base_dir(self):
        from backend.src.common.utils import now_iso
        from backend.src.services.memory.memory_store import sync_memory_from_files
        from backend.src.storage import get_connection

        created_at = now_iso()
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO memory_items (content, created_at, memory_type, tags, task_id, uid) VALUES (?, ?, ?, ?, ?, ?)",
                ("hi", created_at, "short_term", "[]", None, ""),
            )

        base_dir = Path(self._tmp.name) / "custom_memory"
        result = sync_memory_from_files(base_dir=base_dir, prune=False)
        self.assertEqual(int(result.get("published") or 0), 1)

        with get_connection() as conn:
            row = conn.execute("SELECT uid FROM memory_items ORDER BY id ASC LIMIT 1").fetchone()
        uid = str(row["uid"] or "").strip()
        self.assertTrue(uid)

        # 必须落到 base_dir，而不是默认 prompt_root/memory
        self.assertTrue((base_dir / f"{uid}.md").exists())
        self.assertFalse((self._prompt_root / "memory" / f"{uid}.md").exists())


if __name__ == "__main__":
    unittest.main()

