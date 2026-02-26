import json
import unittest


class TestStreamEntryCommonMissingResult(unittest.TestCase):
    def test_chunk_has_visible_result_tag_detects_tag(self):
        from backend.src.agent.runner.stream_entry_common import chunk_has_visible_result_tag
        from backend.src.constants import STREAM_TAG_RESULT

        self.assertFalse(chunk_has_visible_result_tag("data: {\"delta\": \"hello\"}\n\n"))
        self.assertTrue(chunk_has_visible_result_tag(f"data: {{\"delta\": \"{STREAM_TAG_RESULT}\\nhi\"}}\\n\\n"))

    def test_build_missing_visible_result_sse_wraps_result_tag(self):
        from backend.src.agent.runner.stream_entry_common import build_missing_visible_result_sse
        from backend.src.constants import RUN_STATUS_FAILED, STREAM_TAG_RESULT

        msg = build_missing_visible_result_sse(RUN_STATUS_FAILED, task_id=12, run_id=34)
        self.assertIsInstance(msg, str)
        self.assertIn("data:", msg)

        data_lines = [line for line in msg.splitlines() if line.startswith("data:")]
        self.assertTrue(data_lines)
        payload = json.loads(data_lines[0][len("data: ") :])
        self.assertIn("delta", payload)
        self.assertTrue(str(payload["delta"]).startswith(STREAM_TAG_RESULT))
        self.assertIn("失败", str(payload["delta"]))

