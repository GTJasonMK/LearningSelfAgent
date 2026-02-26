import unittest
import sys
import types


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


class TestReactFailureBudget(unittest.TestCase):
    def test_record_failure_signature_counts_repeats(self):
        from backend.src.agent.runner.react_error_handler import _record_failure_signature

        state = {}
        sig1, count1 = _record_failure_signature(
            agent_state=state,
            action_type="shell_command",
            step_error="命令执行失败:ERROR: failed to fetch data",
        )
        sig2, count2 = _record_failure_signature(
            agent_state=state,
            action_type="shell_command",
            step_error="命令执行失败:ERROR: failed to fetch data",
        )
        self.assertEqual(sig1, sig2)
        self.assertEqual(count1, 1)
        self.assertEqual(count2, 2)

    def test_normalize_failure_signature_prefers_error_code(self):
        from backend.src.agent.runner.react_error_handler import _normalize_failure_signature

        signature = _normalize_failure_signature(
            action_type="tool_call",
            step_error="[code=rate_limited] web_fetch 可能被限流",
        )
        self.assertEqual(signature, "tool_call|code:rate_limited")


if __name__ == "__main__":
    unittest.main()
