import os
import tempfile
import unittest
from pathlib import Path


class TestSkillDeleteStrongConsistency(unittest.TestCase):
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

    def test_delete_skill_removes_db_and_file(self):
        from backend.src.common.utils import now_iso
        from backend.src.prompt.paths import skills_prompt_dir
        from backend.src.services.skills.skills_delete import delete_skill_strong
        from backend.src.services.skills.skills_publish import publish_skill_file
        from backend.src.storage import get_connection

        created_at = now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO skills_items (name, created_at) VALUES (?, ?)",
                ("test_delete_skill", created_at),
            )
            skill_id = int(cursor.lastrowid)

        source_path, err = publish_skill_file(skill_id)
        self.assertIsNone(err)
        self.assertTrue(source_path)

        skill_file = skills_prompt_dir() / Path(str(source_path))
        self.assertTrue(skill_file.exists())

        # 文件与 DB 均应被删除
        delete_skill_strong(int(skill_id))
        self.assertFalse(skill_file.exists())
        with get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM skills_items WHERE id = ?",
                (int(skill_id),),
            ).fetchone()
        self.assertIsNone(row)


if __name__ == "__main__":
    unittest.main()
