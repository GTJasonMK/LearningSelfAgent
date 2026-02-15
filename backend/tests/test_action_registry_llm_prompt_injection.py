import unittest
from unittest.mock import patch


class TestActionRegistryLlmPromptInjection(unittest.TestCase):
    def test_inject_latest_parse_input_into_llm_prompt(self):
        from backend.src.actions.registry import _exec_llm_call

        captured = {}

        def fake_execute_llm_call(task_id: int, run_id: int, payload: dict):
            captured["task_id"] = task_id
            captured["run_id"] = run_id
            captured["payload"] = dict(payload or {})
            return {"ok": True}, None

        context = {
            "latest_parse_input_text": "Date,Close\n2026-02-13,672.1\n2026-02-14,673.2\n",
        }
        payload = {"prompt": "请根据真实数据计算最近两天均价。"}

        with patch("backend.src.actions.registry.execute_llm_call", side_effect=fake_execute_llm_call):
            result, error_message = _exec_llm_call(
                task_id=1,
                run_id=2,
                step_row={"id": 10, "title": "llm_call"},
                payload=payload,
                context=context,
            )

        self.assertIsNone(error_message)
        self.assertEqual(result, {"ok": True})
        prompt = str(captured["payload"].get("prompt") or "")
        self.assertIn("【可用观测数据（自动注入）】", prompt)
        self.assertIn("2026-02-14,673.2", prompt)
        self.assertTrue(bool(context.get("llm_prompt_auto_observation_injected")))

    def test_skip_injection_when_no_parse_input(self):
        from backend.src.actions.registry import _exec_llm_call

        captured = {}

        def fake_execute_llm_call(task_id: int, run_id: int, payload: dict):
            captured["payload"] = dict(payload or {})
            return {"ok": True}, None

        payload = {"prompt": "只做总结。"}
        with patch("backend.src.actions.registry.execute_llm_call", side_effect=fake_execute_llm_call):
            _exec_llm_call(
                task_id=1,
                run_id=2,
                step_row={"id": 11, "title": "llm_call"},
                payload=payload,
                context={},
            )

        self.assertEqual(str(captured["payload"].get("prompt") or ""), "只做总结。")

    def test_avoid_duplicate_injection_marker(self):
        from backend.src.actions.registry import _exec_llm_call

        captured = {}

        def fake_execute_llm_call(task_id: int, run_id: int, payload: dict):
            captured["payload"] = dict(payload or {})
            return {"ok": True}, None

        original = "分析数据。\n\n【可用观测数据（自动注入）】\nabc"
        payload = {"prompt": original}
        context = {"latest_parse_input_text": "abc"}
        with patch("backend.src.actions.registry.execute_llm_call", side_effect=fake_execute_llm_call):
            _exec_llm_call(
                task_id=1,
                run_id=2,
                step_row={"id": 12, "title": "llm_call"},
                payload=payload,
                context=context,
            )

        self.assertEqual(str(captured["payload"].get("prompt") or ""), original)


if __name__ == "__main__":
    unittest.main()
