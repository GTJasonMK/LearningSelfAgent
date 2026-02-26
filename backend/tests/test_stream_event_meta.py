import json
import unittest

from backend.src.agent.contracts.stream_events import attach_stream_event_meta
from backend.src.services.llm.llm_client import sse_json


def _parse_sse_data_json(msg: str):
    text = str(msg or "")
    prefix = "data: "
    if prefix not in text:
        return None
    payload_line = text.split(prefix, 1)[1].splitlines()[0]
    try:
        return json.loads(payload_line)
    except Exception:
        return None


class TestStreamEventMeta(unittest.TestCase):
    def test_attach_meta_enriches_typed_event(self):
        raw = sse_json({"type": "run_created", "task_id": 1, "run_id": 2})
        out, attached = attach_stream_event_meta(
            raw,
            task_id=1,
            run_id=2,
            session_key="sess_abc",
            event_seq=3,
        )
        self.assertTrue(attached)
        obj = _parse_sse_data_json(out)
        self.assertIsNotNone(obj)
        self.assertEqual(obj.get("schema_name"), "lsa_stream_event")
        self.assertEqual(obj.get("schema_version"), 2)
        self.assertEqual(obj.get("session_key"), "sess_abc")
        self.assertEqual(obj.get("causation_id"), "sess_abc")
        self.assertTrue(str(obj.get("event_id") or "").startswith("sess_abc:2:3:run_created"))
        self.assertTrue(bool(str(obj.get("emitted_at") or "").strip()))

    def test_attach_meta_ignores_non_typed_payload(self):
        raw = sse_json({"delta": "hello"})
        out, attached = attach_stream_event_meta(
            raw,
            task_id=1,
            run_id=2,
            session_key="sess_abc",
            event_seq=1,
        )
        self.assertFalse(attached)
        self.assertEqual(out, raw)

    def test_parse_stream_event_chunk_rejects_unsupported_schema(self):
        from backend.src.agent.contracts.stream_events import parse_stream_event_chunk

        raw = sse_json({"type": "run_status", "schema_version": 999, "status": "running"})
        obj = parse_stream_event_chunk(raw)
        self.assertIsNone(obj)


if __name__ == "__main__":
    unittest.main()
