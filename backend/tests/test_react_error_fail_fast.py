import json
import os
import unittest


class TestReactErrorFailFast(unittest.TestCase):
    def setUp(self):
        self._old = os.environ.get("AGENT_REACT_ERROR_POLICY_MATRIX")

    def tearDown(self):
        from backend.src.agent.runner.react_error_policy import resolve_react_error_policy_matrix

        if self._old is None:
            os.environ.pop("AGENT_REACT_ERROR_POLICY_MATRIX", None)
        else:
            os.environ["AGENT_REACT_ERROR_POLICY_MATRIX"] = self._old
        resolve_react_error_policy_matrix.cache_clear()

    def test_default_non_retriable_codes(self):
        from backend.src.agent.runner.react_error_policy import should_fail_fast_on_step_error

        self.assertTrue(should_fail_fast_on_step_error("[code=invalid_action_payload] 参数不合法"))
        self.assertTrue(should_fail_fast_on_step_error("[code=missing_tool_exec_spec] 缺少执行定义"))
        self.assertFalse(should_fail_fast_on_step_error("[code=rate_limited] 上游限流"))
        # script_run 契约错误应优先触发 replan，而非立即终止整轮执行。
        self.assertFalse(should_fail_fast_on_step_error("[code=script_args_missing] 缺少 --out_csv"))
        self.assertFalse(should_fail_fast_on_step_error("[code=missing_expected_artifact] 缺少 data/out.csv"))

    def test_env_override_non_retriable_codes(self):
        from backend.src.agent.runner.react_error_policy import (
            resolve_react_error_policy_matrix,
            should_fail_fast_on_step_error,
        )

        os.environ["AGENT_REACT_ERROR_POLICY_MATRIX"] = json.dumps(
            {
                "non_retriable_codes": ["rate_limited"],
            },
            ensure_ascii=False,
        )
        resolve_react_error_policy_matrix.cache_clear()
        self.assertTrue(should_fail_fast_on_step_error("[code=rate_limited] 上游限流"))


if __name__ == "__main__":
    unittest.main()
