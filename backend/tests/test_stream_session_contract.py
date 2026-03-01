import contextlib
import io
import json
from pathlib import Path
import unittest

from backend.src.cli.commands.stream_session import run_stream_session
from backend.src.cli.sse import SseEvent


class _ClientStub:
    def __init__(self, stream_events, replay_payload=None):
        self._stream_events = list(stream_events or [])
        self._replay_payload = replay_payload if isinstance(replay_payload, dict) else {"items": []}
        self.get_calls = []

    def stream_post(self, _path, json_data=None):
        _ = json_data
        for event in self._stream_events:
            yield event

    def get(self, path, params=None):
        self.get_calls.append((str(path), dict(params or {})))
        return self._replay_payload


class TestStreamSessionContract(unittest.TestCase):
    def test_cross_surface_shared_fixture_contract(self):
        fixture_path = Path(__file__).resolve().parents[2] / "test-fixtures" / "stream_cross_surface_cases.json"
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
        cases = fixture.get("cases") if isinstance(fixture, dict) else None
        self.assertTrue(isinstance(cases, list) and cases)

        for case in cases:
            case_id = str((case or {}).get("id") or "unknown")
            stream_events_raw = (case or {}).get("stream_events")
            replay_payload = (case or {}).get("replay_response")
            expected_cli = (case or {}).get("expected_cli")
            self.assertIsInstance(stream_events_raw, list, msg=f"{case_id}: stream_events invalid")
            self.assertIsInstance(expected_cli, dict, msg=f"{case_id}: expected_cli missing")

            stream_events = []
            for item in stream_events_raw:
                if not isinstance(item, dict):
                    continue
                event_name = str(item.get("event") or "message").strip() or "message"
                payload = item.get("payload")
                if not isinstance(payload, dict):
                    payload = {}
                stream_events.append(
                    SseEvent(
                        event=event_name,
                        data=json.dumps(payload, ensure_ascii=False),
                        json_data=payload,
                    )
                )

            client = _ClientStub(stream_events=stream_events, replay_payload=replay_payload)
            # 屏蔽 CLI 渲染输出，聚焦契约断言。
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                result = run_stream_session(
                    client=client,
                    path="/agent/command/stream",
                    payload={"message": "x"},
                    output_json=False,
                    done_message="done",
                    enable_agent_replay=True,
                )

            self.assertEqual(bool(result.seen_done), bool(expected_cli.get("seen_done")), msg=case_id)
            self.assertEqual(bool(result.seen_error), bool(expected_cli.get("seen_error")), msg=case_id)
            self.assertEqual(
                bool(result.saw_business_state_event),
                bool(expected_cli.get("saw_business_state_event")),
                msg=case_id,
            )
            self.assertEqual(str(result.last_run_status or ""), str(expected_cli.get("last_run_status") or ""), msg=case_id)
            self.assertEqual(int(result.replay_applied), int(expected_cli.get("replay_applied") or 0), msg=case_id)
            self.assertEqual(len(client.get_calls), int(expected_cli.get("replay_calls") or 0), msg=case_id)

    def test_replay_triggered_when_done_without_business_state(self):
        stream_events = [
            SseEvent(
                event="done",
                data='{"type":"stream_end","task_id":1,"run_id":7}',
                json_data={"type": "stream_end", "task_id": 1, "run_id": 7},
            )
        ]
        replay_payload = {
            "items": [
                {
                    "event_id": "sess_x:7:2:run_status",
                    "payload": {
                        "type": "run_status",
                        "task_id": 1,
                        "run_id": 7,
                        "status": "done",
                        "event_id": "sess_x:7:2:run_status",
                    },
                }
            ]
        }
        client = _ClientStub(stream_events=stream_events, replay_payload=replay_payload)
        result = run_stream_session(
            client=client,
            path="/agent/command/stream",
            payload={"message": "x"},
            output_json=True,
            done_message="done",
            enable_agent_replay=True,
        )
        self.assertTrue(result.seen_done)
        self.assertTrue(result.saw_business_state_event)
        self.assertEqual(result.replay_applied, 1)
        self.assertTrue(bool(client.get_calls))

    def test_replay_not_triggered_when_business_state_seen(self):
        stream_events = [
            SseEvent(
                event="message",
                data='{"type":"run_status","task_id":1,"run_id":7,"status":"running"}',
                json_data={"type": "run_status", "task_id": 1, "run_id": 7, "status": "running"},
            ),
            SseEvent(
                event="done",
                data='{"type":"stream_end","task_id":1,"run_id":7}',
                json_data={"type": "stream_end", "task_id": 1, "run_id": 7},
            ),
        ]
        client = _ClientStub(stream_events=stream_events, replay_payload={"items": []})
        result = run_stream_session(
            client=client,
            path="/agent/command/stream",
            payload={"message": "x"},
            output_json=True,
            done_message="done",
            enable_agent_replay=True,
        )
        self.assertTrue(result.seen_done)
        self.assertTrue(result.saw_business_state_event)
        self.assertEqual(result.replay_applied, 0)
        self.assertFalse(client.get_calls)

    def test_replay_dedupes_duplicate_event_id(self):
        stream_events = [
            SseEvent(
                event="message",
                data='{"type":"run_created","task_id":1,"run_id":7,"event_id":"sess_x:7:1:run_created"}',
                json_data={
                    "type": "run_created",
                    "task_id": 1,
                    "run_id": 7,
                    "event_id": "sess_x:7:1:run_created",
                },
            ),
            SseEvent(event="error", data='{"message":"stream broken"}', json_data={"message": "stream broken"}),
        ]
        replay_payload = {
            "items": [
                {
                    "event_id": "sess_x:7:2:run_status",
                    "payload": {
                        "type": "run_status",
                        "task_id": 1,
                        "run_id": 7,
                        "status": "done",
                        "event_id": "sess_x:7:2:run_status",
                    },
                },
                {
                    "event_id": "sess_x:7:2:run_status",
                    "payload": {
                        "type": "run_status",
                        "task_id": 1,
                        "run_id": 7,
                        "status": "done",
                        "event_id": "sess_x:7:2:run_status",
                    },
                },
                {
                    "event_id": "sess_x:7:3:stream_end",
                    "payload": {
                        "type": "stream_end",
                        "task_id": 1,
                        "run_id": 7,
                        "event_id": "sess_x:7:3:stream_end",
                        "run_status": "done",
                    },
                },
            ]
        }
        client = _ClientStub(stream_events=stream_events, replay_payload=replay_payload)
        result = run_stream_session(
            client=client,
            path="/agent/command/stream",
            payload={"message": "x"},
            output_json=True,
            done_message="done",
            enable_agent_replay=True,
        )
        # 重复 run_status(event_id 相同)只算一次补齐。
        self.assertEqual(result.replay_applied, 2)
        self.assertEqual(result.last_run_status, "done")
        self.assertFalse(result.seen_error)


if __name__ == "__main__":
    unittest.main()
