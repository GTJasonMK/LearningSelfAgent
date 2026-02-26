import json
import os
import tempfile
import unittest
from pathlib import Path


class TestFileActionPathPermissions(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmp = tempfile.TemporaryDirectory()
        os.environ["AGENT_DB_PATH"] = str(Path(self._tmp.name) / "agent_test.db")
        storage.init_db()

        self.allowed_dir = Path(self._tmp.name) / "allowed"
        self.denied_dir = Path(self._tmp.name) / "denied"
        self.allowed_dir.mkdir(parents=True, exist_ok=True)
        self.denied_dir.mkdir(parents=True, exist_ok=True)

        self._set_permissions(
            allowed_ops=["read", "write", "execute"],
            allowed_paths=[str(self.allowed_dir)],
        )

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
        except Exception:
            pass
        self._tmp.cleanup()

    def _set_permissions(self, *, allowed_ops, allowed_paths):
        from backend.src.storage import get_connection

        with get_connection() as conn:
            conn.execute(
                "UPDATE permissions_store SET allowed_ops = ?, allowed_paths = ? WHERE id = 1",
                (
                    json.dumps(list(allowed_ops), ensure_ascii=False),
                    json.dumps(list(allowed_paths), ensure_ascii=False),
                ),
            )

    def test_file_write_denied_outside_allowed_paths(self):
        from backend.src.actions.handlers.file_write import execute_file_write

        outside = str(self.denied_dir / "out.txt")
        result, error = execute_file_write({"path": outside, "content": "x"}, context={})
        self.assertIsNone(result)
        self.assertTrue(isinstance(error, str) and "permission_denied" in error)

    def test_file_write_allowed_inside_allowed_paths(self):
        from backend.src.actions.handlers.file_write import execute_file_write

        inside = str(self.allowed_dir / "ok.txt")
        result, error = execute_file_write({"path": inside, "content": "hello"}, context={})
        self.assertIsNone(error)
        self.assertIsInstance(result, dict)
        self.assertTrue(Path(inside).exists())

    def test_file_read_requires_write_op_even_when_path_allowed(self):
        from backend.src.actions.handlers.file_read import execute_file_read

        inside = self.allowed_dir / "readme.txt"
        inside.write_text("abc", encoding="utf-8")
        self._set_permissions(allowed_ops=["execute"], allowed_paths=[str(self.allowed_dir)])

        result, error = execute_file_read({"path": str(inside)})
        self.assertIsNone(result)
        self.assertTrue(isinstance(error, str) and "permission_denied" in error)


if __name__ == "__main__":
    unittest.main()
