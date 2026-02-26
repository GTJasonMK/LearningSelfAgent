import unittest


class TestPostActionVerifier(unittest.TestCase):
    def test_empty_result_for_required_action_returns_structured_error(self):
        from backend.src.actions.post_action_verifier import verify_and_normalize_action_result

        result, error = verify_and_normalize_action_result(
            action_type="http_request",
            payload={"url": "https://example.com"},
            result=None,
            error=None,
            context={},
        )
        self.assertIsNone(result)
        self.assertIn("[code=empty_action_result]", str(error or ""))

    def test_task_output_autofill_from_last_llm_response(self):
        from backend.src.actions.post_action_verifier import verify_and_normalize_action_result

        result, error = verify_and_normalize_action_result(
            action_type="task_output",
            payload={"output_type": "text", "content": ""},
            result={"content": ""},
            error=None,
            context={"last_llm_response": "final answer"},
        )
        self.assertIsNone(error)
        self.assertTrue(isinstance(result, dict))
        self.assertEqual(result.get("content"), "final answer")
        warnings = result.get("warnings") if isinstance(result.get("warnings"), list) else []
        self.assertTrue(any("auto-filled" in str(item) for item in warnings))
        contract = result.get("result_contract") if isinstance(result.get("result_contract"), dict) else {}
        self.assertEqual(int(contract.get("version") or 0), 1)
        self.assertEqual(str(contract.get("action_type") or ""), "task_output")
        self.assertEqual(str(contract.get("status") or ""), "warn")


if __name__ == "__main__":
    unittest.main()
