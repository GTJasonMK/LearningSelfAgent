import sys
import types
import unittest


def _install_httpx_stub() -> None:
    if "httpx" in sys.modules:
        return

    module = types.ModuleType("httpx")

    class _Client:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    module.Client = _Client
    sys.modules["httpx"] = module


_install_httpx_stub()


class TestReactErrorForceReplan(unittest.TestCase):
    def test_force_replan_on_llm_connection_error(self):
        from backend.src.agent.runner.react_error_handler import should_force_replan_on_action_error

        self.assertTrue(should_force_replan_on_action_error("action_invalid:LLM调用失败:Connection error."))
        self.assertTrue(should_force_replan_on_action_error("LLM调用失败:Read timed out"))
        self.assertTrue(should_force_replan_on_action_error("tool_call.input 不能为空"))

    def test_no_force_replan_on_plain_business_error(self):
        from backend.src.agent.runner.react_error_handler import should_force_replan_on_action_error

        self.assertFalse(should_force_replan_on_action_error("命令执行失败:脚本不存在"))

    def test_force_replan_on_structured_shell_error_codes(self):
        from backend.src.agent.runner.react_error_handler import should_force_replan_on_action_error

        self.assertTrue(
            should_force_replan_on_action_error(
                "[code=script_arg_contract_mismatch] 命令执行失败: 脚本参数契约不匹配"
            )
        )
        self.assertTrue(
            should_force_replan_on_action_error("[code=dns_resolution_failed] 命令执行失败: DNS 解析失败")
        )
        self.assertTrue(should_force_replan_on_action_error("[code=http_502] 命令执行失败: 上游不可用"))
        self.assertTrue(should_force_replan_on_action_error("[code=network_unreachable] 命令执行失败: 网络不可达"))


if __name__ == "__main__":
    unittest.main()
