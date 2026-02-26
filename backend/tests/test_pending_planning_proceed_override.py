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


class TestPendingPlanningProceedOverride(unittest.TestCase):
    def test_direct_phrase_is_recognized(self):
        from backend.src.agent.runner.pending_planning_flow import _is_proceed_with_current_info_answer

        self.assertTrue(
            _is_proceed_with_current_info_answer(
                user_input="请按当前已知信息继续执行，并明确列出关键假设。",
                paused={},
            )
        )

    def test_choice_value_match_is_recognized(self):
        from backend.src.agent.runner.pending_planning_flow import _is_proceed_with_current_info_answer

        paused = {
            "choices": [
                {
                    "label": "按当前信息继续",
                    "value": "请按当前已知信息继续执行，并明确列出关键假设。",
                },
                {"label": "先给我澄清问题", "value": "请先给我需要补充的关键问题列表，我再补充。"},
            ]
        }
        self.assertTrue(
            _is_proceed_with_current_info_answer(
                user_input="请按当前已知信息继续执行，并明确列出关键假设。",
                paused=paused,
            )
        )

    def test_unrelated_input_is_not_recognized(self):
        from backend.src.agent.runner.pending_planning_flow import _is_proceed_with_current_info_answer

        self.assertFalse(
            _is_proceed_with_current_info_answer(
                user_input="我先补充两个数据源，暂不继续执行。",
                paused={
                    "choices": [
                        {"label": "按当前信息继续", "value": "请按当前已知信息继续执行，并明确列出关键假设。"}
                    ]
                },
            )
        )

    def test_first_choice_no_longer_implies_proceed(self):
        from backend.src.agent.runner.pending_planning_flow import _is_proceed_with_current_info_answer

        paused = {
            "choices": [
                {"label": "先给我澄清问题", "value": "ask_for_clarification"},
                {"label": "按当前信息继续", "value": "proceed_with_assumptions"},
            ]
        }
        self.assertFalse(
            _is_proceed_with_current_info_answer(
                user_input="ask_for_clarification",
                paused=paused,
            )
        )


if __name__ == "__main__":
    unittest.main()
