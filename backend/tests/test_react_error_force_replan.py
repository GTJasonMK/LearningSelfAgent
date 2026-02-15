import unittest


class TestReactErrorForceReplan(unittest.TestCase):
    def test_force_replan_on_llm_connection_error(self):
        from backend.src.agent.runner.react_error_handler import should_force_replan_on_action_error

        self.assertTrue(should_force_replan_on_action_error("action_invalid:LLM调用失败:Connection error."))
        self.assertTrue(should_force_replan_on_action_error("LLM调用失败:Read timed out"))
        self.assertTrue(should_force_replan_on_action_error("tool_call.input 不能为空"))

    def test_no_force_replan_on_plain_business_error(self):
        from backend.src.agent.runner.react_error_handler import should_force_replan_on_action_error

        self.assertFalse(should_force_replan_on_action_error("命令执行失败:脚本不存在"))


if __name__ == "__main__":
    unittest.main()
