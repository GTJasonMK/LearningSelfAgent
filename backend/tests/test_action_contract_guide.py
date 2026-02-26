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
        )
        self.assertIn("fallback_urls", prompt)
        self.assertIn("strict_status_code", prompt)

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
