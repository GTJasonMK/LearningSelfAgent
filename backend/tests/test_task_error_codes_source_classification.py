import unittest


class TestTaskErrorCodeSourceClassification(unittest.TestCase):
    def test_source_failure_exact_and_prefix_codes(self):
        from backend.src.common.task_error_codes import is_source_failure_error_code

        self.assertTrue(is_source_failure_error_code("rate_limited"))
        self.assertTrue(is_source_failure_error_code("http_429"))
        self.assertTrue(is_source_failure_error_code("http_502"))
        self.assertTrue(is_source_failure_error_code("dns_resolution_failed"))
        self.assertTrue(is_source_failure_error_code("network_unreachable"))

    def test_source_failure_timeout_codes(self):
        from backend.src.common.task_error_codes import is_source_failure_error_code

        self.assertTrue(is_source_failure_error_code("timeout"))
        self.assertTrue(is_source_failure_error_code("connect_timeout"))

    def test_non_source_failure_code(self):
        from backend.src.common.task_error_codes import is_source_failure_error_code

        self.assertFalse(is_source_failure_error_code("script_arg_contract_mismatch"))
        self.assertFalse(is_source_failure_error_code("invalid_action_payload"))


if __name__ == "__main__":
    unittest.main()
