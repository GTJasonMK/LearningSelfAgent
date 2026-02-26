import unittest

from backend.src.agent.contracts.resume_contract import validate_waiting_resume_contract


class TestResumeWaitingContract(unittest.TestCase):
    def test_pass_when_no_required_fields(self):
        err = validate_waiting_resume_contract(
            required_session_key="",
            request_session_key="",
            required_prompt_token="",
            request_prompt_token="",
        )
        self.assertEqual(err, "")

    def test_reject_missing_session_key(self):
        err = validate_waiting_resume_contract(
            required_session_key="sess-required",
            request_session_key="",
            required_prompt_token="",
            request_prompt_token="",
        )
        self.assertIn("缺少 session_key", err)

    def test_reject_mismatched_session_key(self):
        err = validate_waiting_resume_contract(
            required_session_key="sess-required",
            request_session_key="sess-other",
            required_prompt_token="",
            request_prompt_token="",
        )
        self.assertIn("session_key 不匹配", err)

    def test_reject_missing_prompt_token(self):
        err = validate_waiting_resume_contract(
            required_session_key="",
            request_session_key="",
            required_prompt_token="token-required",
            request_prompt_token="",
        )
        self.assertIn("缺少 prompt_token", err)

    def test_reject_mismatched_prompt_token(self):
        err = validate_waiting_resume_contract(
            required_session_key="",
            request_session_key="",
            required_prompt_token="token-required",
            request_prompt_token="token-other",
        )
        self.assertIn("prompt_token 不匹配", err)

    def test_pass_when_all_required_fields_match(self):
        err = validate_waiting_resume_contract(
            required_session_key="sess-required",
            request_session_key="sess-required",
            required_prompt_token="token-required",
            request_prompt_token="token-required",
        )
        self.assertEqual(err, "")


if __name__ == "__main__":
    unittest.main()
