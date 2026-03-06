import unittest


class TestActionContractGuide(unittest.TestCase):
    def test_action_payload_keys_guide_includes_http_request_extended_fields(self):
        from backend.src.actions.registry import action_payload_keys_guide

        text = str(action_payload_keys_guide() or "")
        self.assertIn("- http_request:", text)
        self.assertIn("fallback_urls", text)
        self.assertIn("strict_status_code", text)

    def test_action_payload_keys_guide_covers_all_registered_action_types(self):
        from backend.src.actions.registry import action_payload_keys_guide, list_action_types

        text = str(action_payload_keys_guide() or "")
        for action_type in list_action_types():
            self.assertIn(f"- {action_type}:", text)

    def test_think_planning_allowed_action_types_sync_with_registry(self):
        from backend.src.actions.registry import list_action_types
        from backend.src.agent.think.think_planning import ALLOWED_ACTION_TYPES

        self.assertEqual(list(ALLOWED_ACTION_TYPES), list_action_types())

    def test_build_react_step_prompt_includes_runtime_payload_guide(self):
        from backend.src.agent.runner.react_helpers import build_react_step_prompt

        prompt = build_react_step_prompt(
            workdir=".",
            message="抓取数据",
            plan="[\"http_request:抓取\", \"task_output:输出\"]",
            step_index=1,
            step_title="http_request:抓取",
            allowed_actions="http_request",
            observations="- (无)",
            recent_source_failures="(无)",
            graph="(无)",
            tools="(无)",
            skills="(无)",
            memories="(无)",
            now_utc="2026-02-24T00:00:00Z",
            execution_hint="- 若需脚本优先低参数",
            recent_step_feedback="- step#1 | failed",
            retry_requirements="- must_change=source_selection",
            failure_guidance="- 这些是优先策略提示，不是唯一解",
        )
        self.assertIn("fallback_urls", prompt)
        self.assertIn("strict_status_code", prompt)
        self.assertIn("执行修复约束", prompt)
        self.assertIn("失败修复策略提示", prompt)

    def test_build_react_step_prompt_adds_grounded_script_constraints_for_script_file_write(self):
        from backend.src.agent.runner.react_helpers import build_react_step_prompt

        prompt = build_react_step_prompt(
            workdir=".",
            message="生成解析脚本",
            plan='["file_write:backend/.agent/workspace/parse_gold.py", "shell_command:运行脚本"]',
            step_index=1,
            step_title="file_write:backend/.agent/workspace/parse_gold.py 写入解析脚本",
            allowed_actions="file_write",
            observations="- 最近一次真实观测：HTTP 返回 JSON 片段",
            recent_source_failures="(无)",
            graph="(无)",
            tools="(无)",
            skills="(无)",
            memories="(无)",
            now_utc="2026-02-24T00:00:00Z",
        )
        self.assertIn("脚本 file_write 额外约束", prompt)
        self.assertIn("真实观测", prompt)
        self.assertIn("禁止 skeleton", prompt)

    def test_build_react_step_prompt_injects_latest_sample_for_sample_sensitive_steps(self):
        from backend.src.agent.runner.react_helpers import build_react_step_prompt

        prompt = build_react_step_prompt(
            workdir=".",
            message="生成解析脚本",
            plan='["file_write:backend/.agent/workspace/parse_gold.py", "shell_command:运行脚本"]',
            step_index=1,
            step_title="file_write:backend/.agent/workspace/parse_gold.py 写入解析脚本",
            allowed_actions="file_write",
            observations="- 第2步已抓到真实页面",
            recent_source_failures="(无)",
            graph="(无)",
            tools="(无)",
            skills="(无)",
            memories="(无)",
            latest_parse_input_text="<table><tr><td>2026-03-01</td><td>680</td></tr></table>",
            latest_external_url="http://example.com/gold",
            now_utc="2026-02-24T00:00:00Z",
        )
        self.assertIn("最近真实样本（自动注入，优先使用）", prompt)
        self.assertIn("2026-03-01", prompt)
        self.assertIn("最近样本来源", prompt)
        self.assertIn("假设数据结构", prompt)

    def test_build_execution_constraints_hint_from_agent_state(self):
        from backend.src.agent.runner.react_helpers import build_execution_constraints_hint

        agent_state = {
            "execution_constraints": {
                "prefer_low_param_scripts_until_step": 12,
                "require_script_materialization_until_step": 12,
                "enforce_exclusive_input_args_until_step": 12,
                "require_grounded_script_file_write_until_step": 12,
            }
        }
        hint = build_execution_constraints_hint(agent_state=agent_state, step_order=10)
        self.assertIn("低参数设计", hint)
        self.assertIn("先执行 file_write/file_append", hint)
        self.assertIn("互斥输入", hint)
        self.assertIn("真实脚本", hint)

    def test_export_action_contract_schema_contains_http_request_payload(self):
        from backend.src.actions.registry import export_action_contract_schema

        schema = export_action_contract_schema()
        self.assertEqual(schema.get("title"), "AgentActionContract")
        payloads = ((schema.get("$defs") or {}).get("payloads") or {})
        http_payload = payloads.get("http_request") if isinstance(payloads, dict) else None
        self.assertTrue(isinstance(http_payload, dict))
        props = http_payload.get("properties") if isinstance(http_payload, dict) else {}
        self.assertIn("fallback_urls", props or {})
        self.assertIn("strict_status_code", props or {})
        self.assertFalse(bool((http_payload or {}).get("additionalProperties")))


if __name__ == "__main__":
    unittest.main()
