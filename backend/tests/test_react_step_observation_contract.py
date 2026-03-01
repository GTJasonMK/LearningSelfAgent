import unittest


class TestReactStepObservationContract(unittest.TestCase):
    def test_shell_command_script_run_observation_updates_structured_context(self):
        from backend.src.agent.runner.react_step_executor import build_observation_line
        from backend.src.constants import ACTION_TYPE_SHELL_COMMAND

        context = {}
        obs_line, visible = build_observation_line(
            action_type=ACTION_TYPE_SHELL_COMMAND,
            title="script_run:parse",
            result={
                "ok": True,
                "stdout": '{"ok": true}',
                "stderr": "",
                "parsed_output": {"ok": True, "rows": 3},
                "artifacts": [
                    {"path": "data/out.csv", "exists": True},
                    {"path": "data/meta.json", "exists": False},
                ],
            },
            context=context,
        )

        self.assertIsNone(visible)
        self.assertIn("script_run parsed_output=", obs_line)
        self.assertIn("artifacts=1/2", obs_line)
        self.assertEqual((context.get("latest_script_json_output") or {}).get("rows"), 3)
        self.assertEqual(len(context.get("latest_script_artifacts") or []), 2)
        self.assertIn('"rows": 3', str(context.get("latest_parse_input_text") or ""))


if __name__ == "__main__":
    unittest.main()
