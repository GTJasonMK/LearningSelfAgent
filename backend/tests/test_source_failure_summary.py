import unittest


class TestSourceFailureSummary(unittest.TestCase):
    def test_summary_extracts_host_and_error_code(self):
        from backend.src.agent.source_failure_summary import summarize_recent_source_failures_for_prompt

        observations = [
            "tool_call:web_fetch 抓取 https://stooq.com/q/l/?s=usdcny: FAIL [code=rate_limited] web_fetch 可能被限流",
            "tool_call:web_fetch 抓取 https://api.exchangerate.host/live?source=USD: FAIL [code=missing_api_key] 缺少 access key",
            "tool_call:web_fetch 抓取 https://metals.live/gold: FAIL [code=tls_handshake_failed] tls handshake failed",
        ]

        summary = summarize_recent_source_failures_for_prompt(observations=observations)
        self.assertIn("host=stooq.com", summary)
        self.assertIn("code=rate_limited", summary)
        self.assertIn("host=api.exchangerate.host", summary)
        self.assertIn("code=missing_api_key", summary)
        self.assertIn("host=metals.live", summary)
        self.assertIn("code=tls_handshake_failed", summary)

    def test_summary_merges_failure_signature_counts(self):
        from backend.src.agent.source_failure_summary import summarize_recent_source_failures_for_prompt

        summary = summarize_recent_source_failures_for_prompt(
            observations=[],
            failure_signatures={
                "tool_call|code:rate_limited": {"count": 3},
                "tool_call|code:http_502": {"count": 2},
                "shell_command|code:script_arg_contract_mismatch": {"count": 5},
            },
        )
        self.assertIn("code=rate_limited", summary)
        self.assertIn("recent_count=3", summary)
        self.assertIn("code=http_502", summary)
        self.assertIn("recent_count=2", summary)
        self.assertNotIn("script_arg_contract_mismatch", summary)

    def test_summary_returns_none_marker_when_no_source_failure(self):
        from backend.src.agent.source_failure_summary import summarize_recent_source_failures_for_prompt

        summary = summarize_recent_source_failures_for_prompt(
            observations=["步骤A: ok", "步骤B: file_write output.txt 12 bytes"],
            error="",
            failure_signatures={},
        )
        self.assertEqual(summary, "(无)")

    def test_summary_infers_network_unreachable_from_plain_error(self):
        from backend.src.agent.source_failure_summary import summarize_recent_source_failures_for_prompt

        observations = [
            "shell_command: curl https://api.example.net/data FAIL curl: (7) Failed to connect: Connection refused",
        ]
        summary = summarize_recent_source_failures_for_prompt(observations=observations)
        self.assertIn("host=api.example.net", summary)
        self.assertIn("code=network_unreachable", summary)


if __name__ == "__main__":
    unittest.main()
