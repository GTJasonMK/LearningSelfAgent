import json
import os
import tempfile
import unittest
from unittest.mock import patch


class TestAgentMetrics(unittest.TestCase):
    def setUp(self):
        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_metrics_test.db")
        os.environ["AGENT_DB_PATH"] = self._db_path
        os.environ["AGENT_PROMPT_ROOT"] = os.path.join(self._tmpdir.name, "prompt")
        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
        except Exception:
            pass
        self._tmpdir.cleanup()

    def test_compute_agent_metrics(self):
        from backend.src.storage import get_connection
        from backend.src.services.metrics.agent_metrics import compute_agent_metrics

        # 固定 now_iso：让 since 口径可断言
        with patch("backend.src.services.metrics.agent_metrics.now_iso", return_value="2026-02-05T00:00:00Z"):
            created_at = "2026-02-01T00:00:00Z"

            with get_connection() as conn:
                # 任务（仅为满足外键语义；当前 schema 未强制 FK）
                conn.execute(
                    "INSERT INTO tasks (id, title, status, created_at) VALUES (?, ?, ?, ?)",
                    (1, "t1", "done", created_at),
                )
                conn.execute(
                    "INSERT INTO tasks (id, title, status, created_at) VALUES (?, ?, ?, ?)",
                    (2, "t2", "failed", created_at),
                )

                # run1：do/done
                conn.execute(
                    "INSERT INTO task_runs (id, task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        10,
                        1,
                        "done",
                        "agent_command_react",
                        created_at,
                        created_at,
                        created_at,
                        created_at,
                        json.dumps({"mode": "do", "replan_attempts": 1}, ensure_ascii=False),
                    ),
                )
                # run2：think/failed
                conn.execute(
                    "INSERT INTO task_runs (id, task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        11,
                        2,
                        "failed",
                        "agent_command_react",
                        created_at,
                        created_at,
                        created_at,
                        created_at,
                        json.dumps({"mode": "think", "reflection_count": 2}, ensure_ascii=False),
                    ),
                )

                # steps：run1=3，run2=2
                for i in range(1, 4):
                    conn.execute(
                        "INSERT INTO task_steps (task_id, run_id, title, status, created_at, updated_at, step_order) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (1, 10, f"s{i}", "done", created_at, created_at, i),
                    )
                for i in range(1, 3):
                    conn.execute(
                        "INSERT INTO task_steps (task_id, run_id, title, status, created_at, updated_at, step_order) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (2, 11, f"x{i}", "failed", created_at, created_at, i),
                    )

                # llm_records：run1=100，run2=80
                conn.execute(
                    "INSERT INTO llm_records (prompt, response, run_id, created_at, updated_at, tokens_total) VALUES (?, ?, ?, ?, ?, ?)",
                    ("p", "r", 10, created_at, created_at, 50),
                )
                conn.execute(
                    "INSERT INTO llm_records (prompt, response, run_id, created_at, updated_at, tokens_total) VALUES (?, ?, ?, ?, ?, ?)",
                    ("p2", "r2", 10, created_at, created_at, 50),
                )
                conn.execute(
                    "INSERT INTO llm_records (prompt, response, run_id, created_at, updated_at, tokens_total) VALUES (?, ?, ?, ?, ?, ?)",
                    ("p3", "r3", 11, created_at, created_at, 80),
                )

                # tool_call_records：3 calls，其中 2 reuse（pass+fail）
                cur = conn.execute(
                    "INSERT INTO tools_items (name, description, version, metadata, created_at, updated_at, last_used_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        "tool",
                        "d",
                        "0.1.0",
                        json.dumps({"approval": {"status": "approved"}}, ensure_ascii=False),
                        created_at,
                        created_at,
                        created_at,
                    ),
                )
                tool_id = int(cur.lastrowid)
                conn.execute(
                    "INSERT INTO tool_call_records (tool_id, task_id, skill_id, run_id, reuse, reuse_status, reuse_notes, input, output, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (tool_id, 1, 1, 10, 1, "pass", None, "in", "out", created_at),
                )
                conn.execute(
                    "INSERT INTO tool_call_records (tool_id, task_id, skill_id, run_id, reuse, reuse_status, reuse_notes, input, output, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (tool_id, 1, 1, 10, 1, "fail", None, "in2", "out2", created_at),
                )
                conn.execute(
                    "INSERT INTO tool_call_records (tool_id, task_id, skill_id, run_id, reuse, reuse_status, reuse_notes, input, output, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (tool_id, 2, 1, 11, 0, None, None, "in3", "out3", created_at),
                )

                # agent_review_records：pass（allow/manual/deny）+ fail（deny）
                conn.execute(
                    "INSERT INTO agent_review_records (task_id, run_id, status, distill_status, distill_notes, distill_evidence_refs, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        1,
                        10,
                        "pass",
                        "allow",
                        "ok",
                        json.dumps([{"kind": "output", "output_id": 1}], ensure_ascii=False),
                        created_at,
                    ),
                )
                conn.execute(
                    "INSERT INTO agent_review_records (task_id, run_id, status, distill_status, distill_notes, distill_evidence_refs, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        1,
                        10,
                        "pass",
                        "manual",
                        "distill_score 未达门槛：默认不自动沉淀",
                        "[]",
                        created_at,
                    ),
                )
                conn.execute(
                    "INSERT INTO agent_review_records (task_id, run_id, status, distill_status, distill_notes, distill_evidence_refs, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        1,
                        10,
                        "pass",
                        "manual",
                        "distill 缺少可定位 evidence_refs：默认不自动沉淀",
                        "[]",
                        created_at,
                    ),
                )
                conn.execute(
                    "INSERT INTO agent_review_records (task_id, run_id, status, distill_status, distill_notes, distill_evidence_refs, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        1,
                        10,
                        "pass",
                        "deny",
                        "一次性任务，不沉淀",
                        "[]",
                        created_at,
                    ),
                )
                conn.execute(
                    "INSERT INTO agent_review_records (task_id, run_id, status, distill_status, distill_notes, distill_evidence_refs, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        2,
                        11,
                        "fail",
                        "deny",
                        "bad",
                        "[]",
                        created_at,
                    ),
                )

            metrics = compute_agent_metrics(since_days=30)

        self.assertTrue(metrics.get("ok"))
        self.assertEqual(metrics["runs"]["total"], 2)
        self.assertEqual(metrics["runs"]["done"], 1)
        self.assertEqual(metrics["runs"]["failed"], 1)
        self.assertAlmostEqual(metrics["runs"]["success_rate"], 0.5)
        self.assertAlmostEqual(metrics["runs"]["avg_steps"], 2.5)
        self.assertAlmostEqual(metrics["runs"]["avg_tokens_total"], 90.0)
        self.assertAlmostEqual(metrics["runs"]["avg_replan_attempts"], 0.5)
        self.assertAlmostEqual(metrics["runs"]["avg_reflection_count"], 1.0)

        self.assertEqual(metrics["tool_calls"]["calls"], 3)
        self.assertEqual(metrics["tool_calls"]["reuse_calls"], 2)
        self.assertAlmostEqual(metrics["tool_calls"]["reuse_rate"], 2 / 3, places=4)
        self.assertEqual(metrics["tool_calls"]["reuse_pass_calls"], 1)
        self.assertEqual(metrics["tool_calls"]["reuse_fail_calls"], 1)
        self.assertAlmostEqual(metrics["tool_calls"]["reuse_pass_rate"], 0.5, places=4)

        self.assertEqual(metrics["reviews"]["total"], 5)
        self.assertEqual(metrics["reviews"]["pass"], 4)
        self.assertEqual(metrics["reviews"]["distill_allow"], 1)
        self.assertEqual(metrics["reviews"]["distill_allow_with_evidence"], 1)
        self.assertAlmostEqual(metrics["reviews"]["distill_evidence_coverage_among_allow"], 1.0, places=4)
        self.assertEqual(metrics["reviews"]["distill_manual"], 2)
        self.assertEqual(metrics["reviews"]["distill_deny"], 1)
        self.assertAlmostEqual(metrics["reviews"]["distill_rate_among_pass"], 0.25, places=4)
        self.assertEqual(metrics["reviews"]["distill_block_reasons_among_pass"].get("score_below_threshold"), 1)
        self.assertEqual(metrics["reviews"]["distill_block_reasons_among_pass"].get("missing_evidence_refs"), 1)
        self.assertEqual(metrics["reviews"]["distill_block_reasons_among_pass"].get("evaluator_denied"), 1)

    def test_metrics_agent_route(self):
        from backend.src.api.system.routes_metrics import metrics_agent
        from backend.src.services.metrics.agent_metrics import compute_agent_metrics

        with patch(
            "backend.src.api.system.routes_metrics.compute_agent_metrics",
            wraps=compute_agent_metrics,
        ):
            resp = metrics_agent(since_days=30)
        self.assertTrue(resp.get("ok"))


if __name__ == "__main__":
    unittest.main()
