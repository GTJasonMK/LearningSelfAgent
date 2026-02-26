import unittest


class TestHttpRequestContractParity(unittest.TestCase):
    def test_registry_whitelist_contains_extended_http_fields(self):
        from backend.src.actions.registry import get_action_spec
        from backend.src.constants import ACTION_TYPE_HTTP_REQUEST

        spec = get_action_spec(ACTION_TYPE_HTTP_REQUEST)
        self.assertIsNotNone(spec)
        allowed = set(spec.allowed_payload_keys or set())
        self.assertIn("fallback_urls", allowed)
        self.assertIn("strict_status_code", allowed)

    def test_react_prompt_runtime_contract_mentions_extended_http_fields(self):
        from backend.src.agent.runner.react_helpers import build_react_step_prompt
        from backend.src.constants.prompts import AGENT_REACT_STEP_PROMPT_TEMPLATE

        template = str(AGENT_REACT_STEP_PROMPT_TEMPLATE or "")
        self.assertIn("{action_payload_keys_guide}", template)

        prompt = build_react_step_prompt(
            workdir=".",
            message="抓取接口",
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


if __name__ == "__main__":
    unittest.main()
