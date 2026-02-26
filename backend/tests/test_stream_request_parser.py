import unittest
from types import SimpleNamespace
import importlib.util


HAS_FASTAPI = importlib.util.find_spec("fastapi") is not None


class TestStreamRequestParser(unittest.TestCase):
    def test_parse_uses_defaults_when_payload_missing_optional_fields(self):
        from backend.src.agent.runner.stream_request import (
            ParsedStreamCommandRequest,
            parse_stream_command_request,
        )
        from backend.src.constants import AGENT_DEFAULT_MAX_STEPS

        payload = SimpleNamespace(
            message="  hello world  ",
            max_steps=None,
            dry_run=False,
            model="",
            parameters=None,
        )

        parsed = parse_stream_command_request(payload)
        self.assertIsInstance(parsed, ParsedStreamCommandRequest)
        self.assertEqual("hello world", parsed.message)
        self.assertEqual(AGENT_DEFAULT_MAX_STEPS, parsed.requested_max_steps)
        self.assertEqual(int(AGENT_DEFAULT_MAX_STEPS), parsed.normalized_max_steps)
        self.assertEqual({"temperature": 0.2}, parsed.parameters)

    def test_parse_rejects_empty_message(self):
        from backend.src.agent.runner.stream_request import (
            ParsedStreamCommandRequest,
            parse_stream_command_request,
        )

        if not HAS_FASTAPI:
            self.skipTest("fastapi not installed")

        payload = SimpleNamespace(
            message="   ",
            max_steps=3,
            dry_run=True,
            model="gpt-test",
            parameters={"temperature": 0.1},
        )

        parsed = parse_stream_command_request(payload)
        self.assertFalse(isinstance(parsed, ParsedStreamCommandRequest))
        self.assertEqual(400, getattr(parsed, "status_code", None))


if __name__ == "__main__":
    unittest.main()
