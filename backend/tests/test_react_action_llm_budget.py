import unittest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from backend.src.agent.runner.react_step_executor import (
    REACT_ACTION_MAX_TOKENS,
    REACT_ACTION_RETRY_MAX_TOKENS,
    REACT_LLM_INNER_HARD_TIMEOUT_SECONDS,
    REACT_LLM_INNER_RETRY_MAX_ATTEMPTS,
    generate_action_with_retry,
)


class TestReactActionLlmBudget(unittest.TestCase):
    def test_generate_action_with_retry_uses_inner_llm_budget(self):
        captured = []

        def _fake_call_llm_for_text(*_args, **kwargs):
            captured.append(
                {
                    "retry_max_attempts": kwargs.get("retry_max_attempts"),
                    "hard_timeout_seconds": kwargs.get("hard_timeout_seconds"),
                }
            )
            return '{"action":{"type":"file_list","payload":{"path":"."}}}', None

        with patch(
            "backend.src.agent.runner.react_step_executor.call_llm_for_text",
            side_effect=_fake_call_llm_for_text,
        ):
            action_obj, action_type, payload_obj, action_validate_error, _last_action_text = generate_action_with_retry(
                llm_call=lambda _payload: {},
                react_prompt="prompt",
                task_id=1,
                run_id=1,
                step_order=1,
                step_title="file_list:.",
                workdir=".",
                model="gpt-5.2",
                react_params={"temperature": 0.2},
                variables_source="agent_react",
            )

        self.assertIsNotNone(action_obj)
        self.assertEqual(action_type, "file_list")
        self.assertIsInstance(payload_obj, dict)
        self.assertIsNone(action_validate_error)
        self.assertTrue(captured)
        self.assertEqual(
            int(captured[0]["retry_max_attempts"]),
            int(REACT_LLM_INNER_RETRY_MAX_ATTEMPTS),
        )
        self.assertEqual(
            int(captured[0]["hard_timeout_seconds"]),
            int(REACT_LLM_INNER_HARD_TIMEOUT_SECONDS),
        )

    def test_generate_action_with_retry_transport_error_uses_separate_budget(self):
        from backend.src.agent.runner import react_step_executor as mod

        call_count = {"n": 0}

        def _fake_call_llm_for_text(*_args, **_kwargs):
            call_count["n"] += 1
            return None, "LLM call timeout after 20s"

        with patch.object(mod, "REACT_LLM_ERROR_MAX_ATTEMPTS", 1), patch(
            "backend.src.agent.runner.react_step_executor.call_llm_for_text",
            side_effect=_fake_call_llm_for_text,
        ):
            action_obj, action_type, payload_obj, action_validate_error, _last_action_text = generate_action_with_retry(
                llm_call=lambda _payload: {},
                react_prompt="prompt",
                task_id=1,
                run_id=1,
                step_order=1,
                step_title="file_list:.",
                workdir=".",
                model="gpt-5.2",
                react_params={"temperature": 0.2},
                variables_source="agent_react",
            )

        self.assertIsNone(action_obj)
        self.assertIsNone(action_type)
        self.assertIsNone(payload_obj)
        self.assertIn("timeout", str(action_validate_error or "").lower())
        self.assertEqual(call_count["n"], 1)

    def test_generate_action_with_retry_transient_timeout_retries_with_longer_budget(self):
        from backend.src.agent.runner import react_step_executor as mod

        captured = []
        call_count = {"n": 0}

        def _fake_call_llm_for_text(*_args, **kwargs):
            call_count["n"] += 1
            captured.append(int(kwargs.get("hard_timeout_seconds") or 0))
            if call_count["n"] == 1:
                return None, "LLM call timeout after 45s"
            return '{"action":{"type":"file_list","payload":{"path":"."}}}', None

        with patch.object(mod, "REACT_LLM_ERROR_MAX_ATTEMPTS", 2), patch.object(
            mod, "REACT_LLM_INNER_HARD_TIMEOUT_SECONDS", 45
        ), patch(
            "backend.src.agent.runner.react_step_executor.call_llm_for_text",
            side_effect=_fake_call_llm_for_text,
        ):
            action_obj, action_type, payload_obj, action_validate_error, _last_action_text = generate_action_with_retry(
                llm_call=lambda _payload: {},
                react_prompt="prompt",
                task_id=1,
                run_id=1,
                step_order=1,
                step_title="file_write:tmp.py",
                workdir=".",
                model="gpt-5.2",
                react_params={"temperature": 0.2},
                variables_source="agent_react",
            )

        self.assertIsNotNone(action_obj)
        self.assertEqual(action_type, "file_list")
        self.assertIsInstance(payload_obj, dict)
        self.assertIsNone(action_validate_error)
        self.assertEqual(call_count["n"], 2)
        self.assertGreaterEqual(captured[0], 45)
        self.assertGreater(captured[1], captured[0])

    def test_generate_action_with_retry_caps_max_tokens_for_action_json(self):
        from backend.src.agent.runner import react_step_executor as mod

        captured_max_tokens = []
        call_count = {"n": 0}

        def _fake_call_llm_for_text(*_args, **kwargs):
            call_count["n"] += 1
            params = kwargs.get("parameters") or {}
            try:
                captured_max_tokens.append(int(params.get("max_tokens") or 0))
            except Exception:
                captured_max_tokens.append(0)
            if call_count["n"] == 1:
                return None, "LLM call timeout after 45s"
            return '{"action":{"type":"file_list","payload":{"path":"."}}}', None

        with patch.object(mod, "REACT_LLM_ERROR_MAX_ATTEMPTS", 2), patch(
            "backend.src.agent.runner.react_step_executor.call_llm_for_text",
            side_effect=_fake_call_llm_for_text,
        ):
            action_obj, action_type, payload_obj, action_validate_error, _last_action_text = generate_action_with_retry(
                llm_call=lambda _payload: {},
                react_prompt="prompt",
                task_id=1,
                run_id=1,
                step_order=1,
                step_title="file_list:.",
                workdir=".",
                model="gpt-5.2",
                react_params={"temperature": 0.2, "max_tokens": 4096},
                variables_source="agent_react",
            )

        self.assertIsNotNone(action_obj)
        self.assertEqual(action_type, "file_list")
        self.assertIsInstance(payload_obj, dict)
        self.assertIsNone(action_validate_error)
        self.assertGreaterEqual(len(captured_max_tokens), 2)
        self.assertEqual(int(captured_max_tokens[0]), int(REACT_ACTION_MAX_TOKENS))
        self.assertEqual(int(captured_max_tokens[1]), int(REACT_ACTION_RETRY_MAX_TOKENS))
    def test_generate_action_with_retry_timeout_switches_to_compact_prompt(self):
        from backend.src.agent.runner import react_step_executor as mod

        prompts = []
        call_count = {"n": 0}

        def _fake_call_llm_for_text(*_args, **kwargs):
            call_count["n"] += 1
            prompts.append(str(kwargs.get("prompt") or ""))
            if call_count["n"] == 1:
                return None, "LLM call timeout after 45s"
            return '{"action":{"type":"file_write","payload":{"path":"tmp.py","content":"print(1)\\n"}}}', None

        with patch.object(mod, "REACT_LLM_ERROR_MAX_ATTEMPTS", 2), patch(
            "backend.src.agent.runner.react_step_executor.call_llm_for_text",
            side_effect=_fake_call_llm_for_text,
        ):
            action_obj, action_type, payload_obj, action_validate_error, _last_action_text = generate_action_with_retry(
                llm_call=lambda _payload: {},
                react_prompt="x" * 5000,
                task_id=1,
                run_id=1,
                step_order=2,
                step_title="file_write:tmp.py 写脚本",
                workdir=".",
                model="gpt-5.2",
                react_params={"temperature": 0.2, "max_tokens": 4096},
                variables_source="agent_react",
                allowed_actions_text="file_write",
            )

        self.assertEqual(call_count["n"], 2)
        self.assertIsNotNone(action_obj)
        self.assertEqual(action_type, "file_write")
        self.assertIsInstance(payload_obj, dict)
        self.assertIsNone(action_validate_error)
        self.assertGreaterEqual(len(prompts), 2)
        self.assertLess(len(prompts[1]), len(prompts[0]))
        self.assertIn("超时降载重试模式", prompts[1])
        self.assertIn("禁止输出 skeleton", prompts[1])
        self.assertNotIn("最小可执行骨架", prompts[1])

    def test_generate_action_with_retry_file_write_timeout_without_existing_content_does_not_fake_success(self):
        from backend.src.agent.runner import react_step_executor as mod

        call_count = {"n": 0}

        def _fake_call_llm_for_text(*_args, **_kwargs):
            call_count["n"] += 1
            return None, "LLM call timeout after 60s"

        with tempfile.TemporaryDirectory() as tmp, patch.object(mod, "REACT_LLM_ERROR_MAX_ATTEMPTS", 2), patch(
            "backend.src.agent.runner.react_step_executor.call_llm_for_text",
            side_effect=_fake_call_llm_for_text,
        ):
            action_obj, action_type, payload_obj, action_validate_error, _last_action_text = generate_action_with_retry(
                llm_call=lambda _payload: {},
                react_prompt="prompt",
                task_id=1,
                run_id=1,
                step_order=3,
                step_title="file_write:backend/.agent/workspace/x.py 写脚本",
                workdir=tmp,
                model="gpt-5.2",
                react_params={"temperature": 0.2, "max_tokens": 4096},
                variables_source="agent_react",
                allowed_actions_text="file_write",
            )

        self.assertGreaterEqual(call_count["n"], 2)
        self.assertIsNone(action_obj)
        self.assertIsNone(action_type)
        self.assertIsNone(payload_obj)
        self.assertIn("timeout", str(action_validate_error or "").lower())

    def test_generate_action_with_retry_file_write_timeout_can_reuse_existing_content(self):
        from backend.src.agent.runner import react_step_executor as mod

        call_count = {"n": 0}

        def _fake_call_llm_for_text(*_args, **_kwargs):
            call_count["n"] += 1
            return None, "LLM call timeout after 60s"

        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "backend/.agent/workspace/x.py"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("print(1)\n", encoding="utf-8")

            with patch.object(mod, "REACT_LLM_ERROR_MAX_ATTEMPTS", 2), patch(
                "backend.src.agent.runner.react_step_executor.call_llm_for_text",
                side_effect=_fake_call_llm_for_text,
            ):
                action_obj, action_type, payload_obj, action_validate_error, _last_action_text = generate_action_with_retry(
                    llm_call=lambda _payload: {},
                    react_prompt="prompt",
                    task_id=1,
                    run_id=1,
                    step_order=3,
                    step_title="file_write:backend/.agent/workspace/x.py 写脚本",
                    workdir=tmp,
                    model="gpt-5.2",
                    react_params={"temperature": 0.2, "max_tokens": 4096},
                    variables_source="agent_react",
                    allowed_actions_text="file_write",
                )

        self.assertGreaterEqual(call_count["n"], 2)
        self.assertIsNone(action_validate_error)
        self.assertIsNotNone(action_obj)
        self.assertEqual(action_type, "file_write")
        self.assertIsInstance(payload_obj, dict)
        self.assertEqual(str(payload_obj.get("path") or ""), "backend/.agent/workspace/x.py")
        self.assertEqual(str(payload_obj.get("content") or ""), "print(1)\n")


    def test_generate_action_with_retry_raises_token_cap_for_llm_call_steps(self):
        captured_max_tokens = []

        def _fake_call_llm_for_text(*_args, **kwargs):
            params = kwargs.get("parameters") or {}
            captured_max_tokens.append(int(params.get("max_tokens") or 0))
            return '{"action":{"type":"llm_call","payload":{"prompt":"分析最近观测"}}}', None

        with patch(
            "backend.src.agent.runner.react_step_executor.call_llm_for_text",
            side_effect=_fake_call_llm_for_text,
        ):
            action_obj, action_type, payload_obj, action_validate_error, _last_action_text = generate_action_with_retry(
                llm_call=lambda _payload: {},
                react_prompt="prompt",
                task_id=1,
                run_id=1,
                step_order=2,
                step_title="llm_call:分析数据并提取价格",
                workdir=".",
                model="deepseek-chat",
                react_params={"temperature": 0.2, "max_tokens": 4096},
                variables_source="agent_react",
                allowed_actions_text="llm_call",
            )

        self.assertIsNotNone(action_obj)
        self.assertEqual(action_type, "llm_call")
        self.assertIsInstance(payload_obj, dict)
        self.assertIsNone(action_validate_error)
        self.assertTrue(captured_max_tokens)
        self.assertGreater(int(captured_max_tokens[0]), int(REACT_ACTION_MAX_TOKENS))

    def test_generate_action_with_retry_raises_token_cap_for_script_file_write_steps(self):
        captured_max_tokens = []

        def _fake_call_llm_for_text(*_args, **kwargs):
            params = kwargs.get("parameters") or {}
            captured_max_tokens.append(int(params.get("max_tokens") or 0))
            return json.dumps({"action": {"type": "file_write", "payload": {"path": "backend/.agent/workspace/parse_gold.py", "content": "print(1)\n"}}}), None

        with patch(
            "backend.src.agent.runner.react_step_executor.call_llm_for_text",
            side_effect=_fake_call_llm_for_text,
        ):
            action_obj, action_type, payload_obj, action_validate_error, _last_action_text = generate_action_with_retry(
                llm_call=lambda _payload: {},
                react_prompt="prompt",
                task_id=1,
                run_id=1,
                step_order=2,
                step_title="file_write:backend/.agent/workspace/parse_gold.py",
                workdir=".",
                model="deepseek-chat",
                react_params={"temperature": 0.2, "max_tokens": 4096},
                variables_source="agent_react",
                allowed_actions_text="file_write",
            )

        self.assertIsNotNone(action_obj)
        self.assertEqual(action_type, "file_write")
        self.assertIsInstance(payload_obj, dict)
        self.assertIsNone(action_validate_error)
        self.assertTrue(captured_max_tokens)
        self.assertGreater(int(captured_max_tokens[0]), int(REACT_ACTION_MAX_TOKENS))
    def test_generate_action_with_retry_llm_call_invalid_json_uses_contract_fallback(self):
        call_count = {"n": 0}

        def _fake_call_llm_for_text(*_args, **_kwargs):
            call_count["n"] += 1
            return '{"action":{"type":"llm_call","payload":{"prompt":"请从以下网页内容中提取', None

        with patch(
            "backend.src.agent.runner.react_step_executor.call_llm_for_text",
            side_effect=_fake_call_llm_for_text,
        ):
            action_obj, action_type, payload_obj, action_validate_error, _last_action_text = generate_action_with_retry(
                llm_call=lambda _payload: {},
                react_prompt="prompt",
                task_id=1,
                run_id=1,
                step_order=2,
                step_title="llm_call:分析数据并提取价格",
                workdir=".",
                model="deepseek-chat",
                react_params={"temperature": 0.2, "max_tokens": 4096},
                variables_source="agent_react",
                allowed_actions_text="llm_call",
            )

        self.assertGreaterEqual(call_count["n"], 1)
        self.assertIsNotNone(action_obj)
        self.assertEqual(action_type, "llm_call")
        self.assertIsInstance(payload_obj, dict)
        self.assertIsNone(action_validate_error)
        prompt = str(payload_obj.get("prompt") or "")
        self.assertIn("分析数据并提取价格", prompt)
        self.assertIn("系统会自动注入最近一次真实观测", prompt)


if __name__ == "__main__":
    unittest.main()
