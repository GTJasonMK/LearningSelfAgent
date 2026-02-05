import os
import tempfile
import unittest
from pathlib import Path


class TestToolsFileConsistency(unittest.TestCase):
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

    def test_publish_and_delete_tool_file(self):
        from backend.src.common.utils import now_iso
        from backend.src.services.tools.tools_delete import delete_tool_strong
        from backend.src.services.tools.tools_store import publish_tool_file
        from backend.src.prompt.paths import tools_prompt_dir
        from backend.src.storage import get_connection

        created_at = now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tools_items (name, description, version, created_at, updated_at, last_used_at, metadata, source_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("demo_tool", "demo", "0.1.0", created_at, created_at, created_at, None, None),
            )
            tool_id = int(cursor.lastrowid)

            info = publish_tool_file(tool_id, conn=conn)
            self.assertTrue(info.get("ok"))
            source_path = str(info.get("source_path") or "")
            self.assertTrue(source_path)

        tool_file = tools_prompt_dir() / Path(source_path)
        self.assertTrue(tool_file.exists())

        delete_tool_strong(tool_id)
        self.assertFalse(tool_file.exists())

        with get_connection() as conn:
            row = conn.execute("SELECT * FROM tools_items WHERE id = ?", (int(tool_id),)).fetchone()
        self.assertIsNone(row)


if __name__ == "__main__":
    unittest.main()

