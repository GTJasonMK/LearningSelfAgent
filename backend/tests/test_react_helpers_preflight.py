import json
import os
import tempfile
import unittest


class TestReactHelpersPreflight(unittest.TestCase):
    def test_shell_command_missing_script_is_rejected_before_execution(self):
        from backend.src.agent.runner.react_helpers import validate_and_normalize_action_text

        with tempfile.TemporaryDirectory() as tmp:
            missing_script = os.path.join(tmp, "not_exists.py")
            action_text = json.dumps(
                {
                    "action": {
                        "type": "shell_command",
                        "payload": {
                            "command": ["python", missing_script],
                            "workdir": tmp,
                        },
                    }
                },
                ensure_ascii=False,
            )
            action_obj, action_type, payload_obj, err = validate_and_normalize_action_text(
                action_text=action_text,
                step_title="shell_command:运行脚本",
                workdir=tmp,
            )

        self.assertIsNone(action_obj)
        self.assertIsNone(action_type)
        self.assertIsNone(payload_obj)
        self.assertIn("script_missing", str(err))

    def test_shell_command_existing_script_passes_preflight(self):
        from backend.src.agent.runner.react_helpers import validate_and_normalize_action_text

        with tempfile.TemporaryDirectory() as tmp:
            script_path = os.path.join(tmp, "ok.py")
            with open(script_path, "w", encoding="utf-8") as handle:
                handle.write("print('ok')\n")
            action_text = json.dumps(
                {
                    "action": {
                        "type": "shell_command",
                        "payload": {
                            "command": ["python", script_path],
                            "workdir": tmp,
                        },
                    }
                },
                ensure_ascii=False,
            )
            action_obj, action_type, payload_obj, err = validate_and_normalize_action_text(
                action_text=action_text,
                step_title="shell_command:运行脚本",
                workdir=tmp,
            )

        self.assertIsNone(err)
        self.assertIsInstance(action_obj, dict)
        self.assertEqual(action_type, "shell_command")
        self.assertIsInstance(payload_obj, dict)


if __name__ == "__main__":
    unittest.main()
