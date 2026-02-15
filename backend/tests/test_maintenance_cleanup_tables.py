import os
import sqlite3
import tempfile
import unittest


class TestMaintenanceCleanupTables(unittest.TestCase):
    def test_cleanup_supports_chat_messages_and_tasks_tables(self):
        from backend.src.common.utils import now_iso
        from backend.src.constants import DB_ENV_VAR
        from backend.src import storage
        from backend.src.api.schemas import MaintenanceCleanupRequest
        from backend.src.api.system import routes_maintenance

        old_db = os.environ.get(DB_ENV_VAR)
        try:
            with tempfile.TemporaryDirectory() as tmp:
                db_path = os.path.join(tmp, "agent.db")
                os.environ[DB_ENV_VAR] = db_path
                storage.reset_db_cache()
                storage.init_db()

                with storage.get_connection() as conn:
                    conn.execute(
                        "INSERT INTO chat_messages (role, content, created_at) VALUES (?, ?, ?)",
                        ("user", "hi", "2020-01-01T00:00:00Z"),
                    )
                    conn.execute(
                        "INSERT INTO tasks (title, status, created_at) VALUES (?, ?, ?)",
                        ("t1", "done", "2020-01-01T00:00:00Z"),
                    )

                payload = MaintenanceCleanupRequest(
                    mode="delete",
                    tables=["chat_messages", "tasks"],
                    retention_days=0,
                    before=None,
                    limit=1000,
                    dry_run=False,
                )
                result = routes_maintenance._cleanup_execute(payload)
                self.assertIsInstance(result, dict)
                self.assertEqual(result.get("summary", {}).get("mode"), "delete")

                con = sqlite3.connect(db_path)
                cur = con.cursor()
                self.assertEqual(int(cur.execute("SELECT COUNT(*) FROM chat_messages").fetchone()[0]), 0)
                self.assertEqual(int(cur.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]), 0)
                con.close()
        finally:
            if old_db is None:
                os.environ.pop(DB_ENV_VAR, None)
            else:
                os.environ[DB_ENV_VAR] = old_db


if __name__ == "__main__":
    unittest.main()

