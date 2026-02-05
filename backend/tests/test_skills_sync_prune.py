import os
import tempfile
import unittest
from pathlib import Path


class TestSkillsSyncPrune(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_test.db")
        self._skills_dir = Path(self._tmpdir.name) / "skills"

        self._skills_dir.mkdir(parents=True, exist_ok=True)

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

    def test_sync_prunes_db_rows_when_file_missing(self):
        from backend.src.common.utils import now_iso
        from backend.src.storage import get_connection
        from backend.src.services.skills.skills_sync import sync_skills_from_files

        # DB 里先放一条“有 source_path 但文件不存在”的技能记录
        created_at = now_iso()
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO skills_items (name, created_at, description, scope, category, tags, triggers, aliases, source_path, prerequisites, inputs, outputs, steps, failure_modes, validation, version, task_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "dead_skill",
                    created_at,
                    "to be pruned",
                    None,
                    "misc",
                    "[]",
                    "[]",
                    "[]",
                    "misc/dead.md",
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

        # 文件系统里放一条“存在的技能文件”
        keep_path = self._skills_dir / "tool" / "web"
        keep_path.mkdir(parents=True, exist_ok=True)
        keep_file = keep_path / "keep.md"
        with keep_file.open("w", encoding="utf-8") as f:
            f.write(
                "---\n"
                "name: keep_skill\n"
                "description: keep\n"
                "category: tool.web\n"
                "tags: [test]\n"
                "---\n"
                "\n"
                "# keep_skill\n"
                "\n"
                "body\n"
            )

        result = sync_skills_from_files(base_dir=self._skills_dir, prune=True)
        self.assertEqual(result.get("deleted"), 1)

        with get_connection() as conn:
            dead = conn.execute(
                "SELECT COUNT(*) AS c FROM skills_items WHERE source_path = ?",
                ("misc/dead.md",),
            ).fetchone()
            keep = conn.execute(
                "SELECT COUNT(*) AS c FROM skills_items WHERE source_path = ?",
                ("tool/web/keep.md",),
            ).fetchone()

        self.assertEqual(int(dead["c"]), 0)
        self.assertEqual(int(keep["c"]), 1)


if __name__ == "__main__":
    unittest.main()
