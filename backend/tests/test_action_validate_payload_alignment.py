import os
import tempfile
import unittest


class TestActionValidatePayloadAlignment(unittest.TestCase):
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

    def test_file_write_allows_missing_content(self):
        """
        回归：validate_action_object 应与执行器一致：
        - file_write.content 允许缺失/None（执行器会视为 ""）
        """
        from backend.src.actions.registry import validate_action_object

        err = validate_action_object(
            {
                "action": {
                    "type": "file_write",
                    "payload": {"path": "a.txt"},
                }
            }
        )
        self.assertIsNone(err)

    def test_file_append_allows_missing_content(self):
        from backend.src.actions.registry import validate_action_object

        err = validate_action_object(
            {
                "action": {
                    "type": "file_append",
                    "payload": {"path": "a.txt"},
                }
            }
        )
        self.assertIsNone(err)

    def test_tool_call_rejects_non_string_input(self):
        from backend.src.actions.registry import validate_action_object

        err = validate_action_object(
            {
                "action": {
                    "type": "tool_call",
                    "payload": {"input": {"k": "v"}, "output": ""},
                }
            }
        )
        self.assertEqual(err, "tool_call.input 不能为空")

    def test_file_list_rejects_non_string_path(self):
        from backend.src.actions.registry import validate_action_object

        err = validate_action_object(
            {
                "action": {
                    "type": "file_list",
                    "payload": {"path": {"bad": True}},
                }
            }
        )
        self.assertEqual(err, "file_list.path 不能为空")

    def test_shell_command_rejects_empty_list_head(self):
        from backend.src.actions.registry import validate_action_object

        err = validate_action_object(
            {
                "action": {
                    "type": "shell_command",
                    "payload": {"command": [""], "workdir": self._tmpdir.name},
                }
            }
        )
        self.assertEqual(err, "shell_command.command 不能为空")

    def test_shell_command_accepts_command_list(self):
        from backend.src.actions.registry import validate_action_object

        err = validate_action_object(
            {
                "action": {
                    "type": "shell_command",
                    "payload": {"command": ["echo", "hi"], "workdir": self._tmpdir.name},
                }
            }
        )
        self.assertIsNone(err)

    def test_http_request_accepts_fallback_urls_and_strict_status_code(self):
        from backend.src.actions.registry import validate_action_object

        err = validate_action_object(
            {
                "action": {
                    "type": "http_request",
                    "payload": {
                        "url": "https://example.com/data",
                        "fallback_urls": [
                            "https://example.net/data",
                            "https://backup.example.org/data",
                        ],
                        "strict_status_code": True,
                    },
                }
            }
        )
        self.assertIsNone(err)

    def test_http_request_rejects_invalid_fallback_urls_type(self):
        from backend.src.actions.registry import validate_action_object

        err = validate_action_object(
            {
                "action": {
                    "type": "http_request",
                    "payload": {
                        "url": "https://example.com/data",
                        "fallback_urls": {"bad": "type"},
                    },
                }
            }
        )
        self.assertEqual(err, "http_request.fallback_urls 必须是字符串或字符串数组")


if __name__ == "__main__":
    unittest.main()

