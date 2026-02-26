import json
import unittest


class TestHttpRequestPayloadWhitelist(unittest.TestCase):
    def test_executor_keeps_fallback_urls_and_strict_status_code(self):
        from backend.src.actions import registry as action_registry
        from backend.src.actions.executor import _execute_step_action

        captured_payload = {}
        original_execute_http_request = action_registry.execute_http_request

        def _fake_execute_http_request(payload):
            captured_payload.clear()
            captured_payload.update(dict(payload or {}))
            return {"ok": True}, None

        action_registry.execute_http_request = _fake_execute_http_request
        try:
            step_row = {
                "detail": json.dumps(
                    {
                        "type": "http_request",
                        "payload": {
                            "url": "https://example.com/primary",
                            "fallback_urls": [
                                "https://example.net/fallback",
                                "https://example.org/backup",
                            ],
                            "strict_status_code": True,
                            "unknown_field_should_drop": "x",
                        },
                    }
                )
            }
            result, error = _execute_step_action(1, 2, step_row, context={})
        finally:
            action_registry.execute_http_request = original_execute_http_request

        self.assertIsNone(error)
        self.assertTrue(isinstance(result, dict))
        self.assertEqual(result.get("ok"), True)
        # 执行器会统一附加 result_contract；本用例的 fake result 缺少 status_code/content，
        # 应被后置验证器标记为 warn（但不应阻塞主链路）。
        contract = result.get("result_contract") if isinstance(result, dict) else None
        self.assertTrue(isinstance(contract, dict))
        self.assertEqual(contract.get("action_type"), "http_request")
        self.assertEqual(contract.get("status"), "warn")
        warnings = result.get("warnings") if isinstance(result, dict) else None
        self.assertTrue(isinstance(warnings, list))
        self.assertIn("http_request.status_code missing", warnings)
        self.assertIn("http_request.content is empty", warnings)
        self.assertEqual(captured_payload.get("url"), "https://example.com/primary")
        self.assertEqual(
            captured_payload.get("fallback_urls"),
            ["https://example.net/fallback", "https://example.org/backup"],
        )
        self.assertIs(captured_payload.get("strict_status_code"), True)
        self.assertNotIn("unknown_field_should_drop", captured_payload)


if __name__ == "__main__":
    unittest.main()
