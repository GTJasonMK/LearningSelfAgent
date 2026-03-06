import json
import unittest

from backend.src.agent.runner.finalization_pipeline import emit_unreachable_proof_if_failed
from backend.src.constants import RUN_STATUS_DONE, RUN_STATUS_FAILED


def _parse_sse_payload(line: str):
    text = str(line or "")
    data_lines = [row for row in text.splitlines() if row.startswith("data: ")]
    if not data_lines:
        return None
    try:
        return json.loads(data_lines[0][len("data: ") :])
    except Exception:
        return None


class TestFinalizationUnreachableProof(unittest.TestCase):
    def test_emit_unreachable_proof_if_failed(self):
        emitted: list[str] = []
        agent_state = {
            "unreachable_proof": {
                "type": "unreachable_proof",
                "proof_id": "proof_x1",
                "reason": "repeat_failure_budget_exceeded",
                "failure_class": "source_unavailable",
            }
        }

        emit_unreachable_proof_if_failed(
            run_status=RUN_STATUS_FAILED,
            agent_state=agent_state,
            task_id=3,
            run_id=9,
            yield_func=lambda msg: emitted.append(str(msg)),
        )

        self.assertEqual(len(emitted), 1)
        payload = _parse_sse_payload(emitted[0]) or {}
        self.assertEqual(str(payload.get("type") or ""), "unreachable_proof")
        self.assertEqual(str(payload.get("proof_id") or ""), "proof_x1")
        self.assertEqual(int(payload.get("task_id") or 0), 3)
        self.assertEqual(int(payload.get("run_id") or 0), 9)

    def test_emit_unreachable_proof_skips_when_not_failed(self):
        emitted: list[str] = []
        agent_state = {"unreachable_proof": {"proof_id": "proof_x2"}}

        emit_unreachable_proof_if_failed(
            run_status=RUN_STATUS_DONE,
            agent_state=agent_state,
            task_id=3,
            run_id=9,
            yield_func=lambda msg: emitted.append(str(msg)),
        )

        self.assertEqual(emitted, [])


if __name__ == "__main__":
    unittest.main()
