import json
import os
import tempfile
import unittest


class TestShellCommandStdin(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        db_path = os.path.join(self._tmpdir.name, "agent_test.db")
        os.environ["AGENT_DB_PATH"] = db_path
        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
        except Exception:
            pass
        self._tmpdir.cleanup()

    def test_shell_command_stdin_is_not_dropped_by_payload_whitelist(self):
        """
        回归：shell_command 的 stdin 字段需要通过 action payload 白名单，
        否则 executor 会丢弃该字段导致子进程读不到输入。
        """
        from backend.src.actions.executor import _execute_step_action

        with tempfile.TemporaryDirectory() as workdir:
            step_row = {
                "id": 1,
                "title": "echo stdin",
                "detail": json.dumps(
                    {
                        "type": "shell_command",
                        "payload": {
                            "command": ["python3", "-c", "import sys;print(sys.stdin.read())"],
                            "workdir": workdir,
                            "timeout_ms": 20000,
                            "stdin": "hello",
                        },
                    },
                    ensure_ascii=False,
                ),
            }

            result, error_message = _execute_step_action(1, 1, step_row, context=None)

        self.assertIsNone(error_message)
        self.assertIsInstance(result, dict)
        self.assertTrue(result.get("ok"))
        self.assertEqual(str(result.get("stdout") or "").strip(), "hello")


if __name__ == "__main__":
    unittest.main()

