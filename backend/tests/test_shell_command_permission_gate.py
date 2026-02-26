import os
import tempfile
import unittest
from unittest.mock import patch

from backend.src.constants import ERROR_MESSAGE_PERMISSION_DENIED


class TestShellCommandPermissionGate(unittest.TestCase):
    def test_permission_denied_blocks_auto_rewrite_side_effect(self):
        from backend.src.actions.handlers.shell_command import execute_shell_command

        complex_code = (
            "import pathlib\n"
            "path='out.txt'\n"
            "with open(path,'w',encoding='utf-8') as f:\n"
            "    f.write('ok')\n"
            "print('ok')"
        )

        with tempfile.TemporaryDirectory() as tmp:
            context = {
                "disallow_complex_python_c": True,
                "auto_rewrite_complex_python_c": True,
                "enforce_shell_script_dependency": True,
                "agent_workspace_rel": "workspace",
            }

            with patch(
                "backend.src.actions.handlers.shell_command.has_exec_permission",
                return_value=False,
            ), patch(
                "backend.src.actions.handlers.shell_command.run_shell_command"
            ) as mocked_run:
                with self.assertRaises(ValueError) as exc:
                    execute_shell_command(
                        task_id=7,
                        run_id=9,
                        step_row={"id": 11},
                        payload={"command": ["python3", "-c", complex_code], "workdir": tmp, "timeout_ms": 10000},
                        context=context,
                    )

            self.assertEqual(str(exc.exception), ERROR_MESSAGE_PERMISSION_DENIED)
            mocked_run.assert_not_called()

            workspace_dir = os.path.join(tmp, "workspace")
            self.assertFalse(os.path.exists(workspace_dir))

            auto_files = []
            for root, _dirs, files in os.walk(tmp):
                for name in files:
                    if str(name).startswith("_auto_python_c_"):
                        auto_files.append(os.path.join(root, name))
            self.assertEqual(auto_files, [])
            self.assertFalse(bool(context.get("shell_auto_rewrite_last_script")))


if __name__ == "__main__":
    unittest.main()
