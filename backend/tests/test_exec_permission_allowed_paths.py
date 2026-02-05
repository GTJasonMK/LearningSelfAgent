import json
import os
import tempfile
import unittest


class TestExecPermissionAllowedPaths(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_test.db")
        os.environ["AGENT_DB_PATH"] = self._db_path
        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
        except Exception:
            pass
        self._tmpdir.cleanup()

    def test_has_exec_permission_uses_dir_boundary_not_prefix(self):
        """
        回归：allowed_paths 不能用 startswith 做前缀判断，否则 /a 会误匹配 /ab。
        """
        from backend.src.storage import get_connection
        from backend.src.services.permissions.permissions_store import has_exec_permission

        base = os.path.join(self._tmpdir.name, "base")
        os.makedirs(base, exist_ok=True)
        os.makedirs(os.path.join(base, "child"), exist_ok=True)
        os.makedirs(base + "x", exist_ok=True)

        with get_connection() as conn:
            conn.execute(
                "UPDATE permissions_store SET allowed_ops = ?, allowed_paths = ? WHERE id = 1",
                (
                    json.dumps(["execute"], ensure_ascii=False),
                    json.dumps([base], ensure_ascii=False),
                ),
            )

        self.assertTrue(has_exec_permission(base))
        self.assertTrue(has_exec_permission(os.path.join(base, "child")))
        self.assertFalse(has_exec_permission(base + "x"))


if __name__ == "__main__":
    unittest.main()

