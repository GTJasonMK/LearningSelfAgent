import os
import tempfile
import unittest
from pathlib import Path


class TestMemoryFileConsistency(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        os.environ["AGENT_DB_PATH"] = str(Path(self._tmp.name) / "agent_test.db")
        os.environ["AGENT_PROMPT_ROOT"] = str(Path(self._tmp.name) / "prompt")

        import backend.src.storage as storage

        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
        except Exception:
            pass
        self._tmp.cleanup()

    def test_create_publishes_file_and_sets_uid(self):
        from backend.src.services.memory.memory_items import create_memory_item
        from backend.src.services.memory.memory_store import memory_file_path

        result = create_memory_item({"content": "hello", "tags": ["t1"]})
        item = result.get("item") or {}
        uid = str(item.get("uid") or "").strip()
        self.assertTrue(uid)

        path = memory_file_path(uid)
        self.assertTrue(path.exists())
        text = path.read_text(encoding="utf-8")
        # JSON frontmatter 应包含 uid 字段
        self.assertIn('"uid"', text)
        self.assertIn("hello", text)

    def test_delete_removes_db_and_file(self):
        from backend.src.services.memory.memory_items import create_memory_item, delete_memory_item
        from backend.src.services.memory.memory_store import memory_file_path
        from backend.src.storage import get_connection

        created = create_memory_item({"content": "to_delete"})
        item = created.get("item") or {}
        item_id = int(item.get("id"))
        uid = str(item.get("uid") or "").strip()
        self.assertTrue(uid)
        self.assertTrue(memory_file_path(uid).exists())

        delete_memory_item(item_id)

        with get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM memory_items WHERE id = ?",
                (int(item_id),),
            ).fetchone()
        self.assertIsNone(row)
        self.assertFalse(memory_file_path(uid).exists())


if __name__ == "__main__":
    unittest.main()

