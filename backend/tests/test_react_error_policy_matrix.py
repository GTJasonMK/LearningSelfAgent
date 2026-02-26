import json
import os
import unittest


class TestReactErrorPolicyMatrix(unittest.TestCase):
    def tearDown(self):
        try:
            os.environ.pop("AGENT_REACT_ERROR_POLICY_MATRIX", None)
        except Exception:
            pass
        from backend.src.agent.runner.react_error_policy import resolve_react_error_policy_matrix

        resolve_react_error_policy_matrix.cache_clear()

    def test_custom_matrix_overrides_defaults(self):
        os.environ["AGENT_REACT_ERROR_POLICY_MATRIX"] = json.dumps(
            {
                "structural_replan_codes": ["custom_struct"],
                "env_replan_codes": ["custom_env"],
                "legacy_keywords": ["CUSTOM_MATCH"],
            },
            ensure_ascii=False,
        )
        from backend.src.agent.runner.react_error_policy import should_force_replan_on_action_error

        self.assertTrue(should_force_replan_on_action_error("[code=custom_struct] failed"))
        self.assertTrue(should_force_replan_on_action_error("[code=custom_env] failed"))
        self.assertTrue(should_force_replan_on_action_error("... CUSTOM_MATCH ..."))
        self.assertFalse(should_force_replan_on_action_error("[code=plan_patch_not_action] x"))


if __name__ == "__main__":
    unittest.main()

