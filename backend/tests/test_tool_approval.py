import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


class TestToolApproval(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp.name) / "agent_test.db"
        os.environ["AGENT_DB_PATH"] = str(db_path)
        os.environ["AGENT_PROMPT_ROOT"] = str(Path(self._tmp.name) / "prompt")

        import backend.src.storage as storage

        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
            self._tmp.cleanup()
        except Exception:
            pass

    def _create_task_and_run(self, run_status: str):
        from backend.src.storage import get_connection

        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("工具批准测试", run_status, created_at, None, created_at, None),
            )
            task_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_plan, agent_state) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_status,
                    "agent_command_react",
                    created_at,
                    None,
                    created_at,
                    created_at,
                    json.dumps({"titles": [], "allows": [], "artifacts": []}, ensure_ascii=False),
                    json.dumps({"mode": "do", "message": "工具批准测试"}, ensure_ascii=False),
                ),
            )
            run_id = int(cursor.lastrowid)
        return task_id, run_id

    def _create_draft_tool_via_call_record(self, *, task_id: int, run_id: int) -> int:
        from backend.src.services.tools.tool_records import create_tool_record

        payload = {
            "task_id": int(task_id),
            "run_id": int(run_id),
            "tool_name": "test_tool",
            "tool_description": "用于工具批准回归测试",
            "tool_version": "0.1.0",
            "tool_metadata": {"exec": {"command": ["echo", "hello"], "workdir": os.getcwd()}},
            "input": "ping",
            "output": "pong",
        }
        resp = create_tool_record(payload)
        record = resp.get("record") if isinstance(resp, dict) else None
        self.assertIsInstance(record, dict)
        self.assertIsNotNone(record.get("tool_id"))
        return int(record.get("tool_id"))

    def _load_tool_approval_status(self, *, tool_id: int) -> str:
        from backend.src.storage import get_connection
        from backend.src.constants import TOOL_METADATA_APPROVAL_KEY

        with get_connection() as conn:
            row = conn.execute("SELECT * FROM tools_items WHERE id = ?", (int(tool_id),)).fetchone()
        self.assertIsNotNone(row)
        meta = json.loads(row["metadata"] or "{}") if row["metadata"] else {}
        approval = meta.get(TOOL_METADATA_APPROVAL_KEY) if isinstance(meta, dict) else None
        self.assertIsInstance(approval, dict)
        return str(approval.get("status") or "")

    def _tool_file_text(self, *, tool_id: int) -> str:
        from backend.src.storage import get_connection

        with get_connection() as conn:
            row = conn.execute("SELECT * FROM tools_items WHERE id = ?", (int(tool_id),)).fetchone()
        self.assertIsNotNone(row)
        source_path = str(row["source_path"] or "").strip()
        self.assertTrue(source_path)
        target = (Path(os.environ["AGENT_PROMPT_ROOT"]) / "tools" / Path(source_path)).resolve()
        self.assertTrue(target.exists())
        return target.read_text(encoding="utf-8")

    def test_approves_draft_tool_when_run_done_and_review_pass(self):
        from backend.src.constants import RUN_STATUS_DONE
        from backend.src.services.tools.tool_approval import approve_draft_tools_from_run
        from backend.src.repositories.agent_retrieval_repo import list_tool_hints

        task_id, run_id = self._create_task_and_run(RUN_STATUS_DONE)
        tool_id = self._create_draft_tool_via_call_record(task_id=task_id, run_id=run_id)
        self.assertEqual(self._load_tool_approval_status(tool_id=tool_id), "draft")

        with patch(
            "backend.src.services.tools.tool_approval.autogen_tool_skill_from_call",
            return_value={"ok": True, "status": "skipped_test"},
        ):
            resp = approve_draft_tools_from_run(
                task_id=int(task_id),
                run_id=int(run_id),
                run_status=RUN_STATUS_DONE,
                review_id=123,
                review_status="pass",
                allow_waiting_feedback=False,
                model=None,
            )
        self.assertIsInstance(resp, dict)
        self.assertTrue(resp.get("ok"))

        self.assertEqual(self._load_tool_approval_status(tool_id=tool_id), "approved")
        hints = list_tool_hints(limit=8)
        self.assertTrue(any(int(it.get("id")) == int(tool_id) for it in hints))

        text = self._tool_file_text(tool_id=tool_id)
        self.assertIn('"status": "approved"', text)

    def test_skips_approval_when_run_waiting_and_not_waiting_feedback(self):
        from backend.src.constants import RUN_STATUS_WAITING
        from backend.src.services.tools.tool_approval import approve_draft_tools_from_run
        from backend.src.repositories.agent_retrieval_repo import list_tool_hints

        task_id, run_id = self._create_task_and_run(RUN_STATUS_WAITING)
        tool_id = self._create_draft_tool_via_call_record(task_id=task_id, run_id=run_id)
        self.assertEqual(self._load_tool_approval_status(tool_id=tool_id), "draft")

        with patch(
            "backend.src.services.tools.tool_approval.autogen_tool_skill_from_call",
            return_value={"ok": True, "status": "skipped_test"},
        ):
            resp = approve_draft_tools_from_run(
                task_id=int(task_id),
                run_id=int(run_id),
                run_status=RUN_STATUS_WAITING,
                review_id=123,
                review_status="pass",
                allow_waiting_feedback=False,
                model=None,
            )
        self.assertIsInstance(resp, dict)
        self.assertTrue(resp.get("ok"))

        self.assertEqual(self._load_tool_approval_status(tool_id=tool_id), "draft")
        hints = list_tool_hints(limit=8)
        self.assertFalse(any(int(it.get("id")) == int(tool_id) for it in hints))

    def test_allows_approval_when_run_waiting_and_waiting_feedback(self):
        from backend.src.constants import RUN_STATUS_WAITING
        from backend.src.services.tools.tool_approval import approve_draft_tools_from_run
        from backend.src.repositories.agent_retrieval_repo import list_tool_hints

        task_id, run_id = self._create_task_and_run(RUN_STATUS_WAITING)
        tool_id = self._create_draft_tool_via_call_record(task_id=task_id, run_id=run_id)
        self.assertEqual(self._load_tool_approval_status(tool_id=tool_id), "draft")

        with patch(
            "backend.src.services.tools.tool_approval.autogen_tool_skill_from_call",
            return_value={"ok": True, "status": "skipped_test"},
        ):
            resp = approve_draft_tools_from_run(
                task_id=int(task_id),
                run_id=int(run_id),
                run_status=RUN_STATUS_WAITING,
                review_id=123,
                review_status="pass",
                allow_waiting_feedback=True,
                model=None,
            )
        self.assertIsInstance(resp, dict)
        self.assertTrue(resp.get("ok"))

        self.assertEqual(self._load_tool_approval_status(tool_id=tool_id), "approved")
        hints = list_tool_hints(limit=8)
        self.assertTrue(any(int(it.get("id")) == int(tool_id) for it in hints))

    def test_skips_approval_when_distill_not_allow(self):
        """
        验证：允许任务完成(pass)但 distill!=allow 时，不应自动批准 draft 工具。
        """
        from backend.src.constants import RUN_STATUS_DONE
        from backend.src.services.tools.tool_approval import approve_draft_tools_from_run
        from backend.src.repositories.agent_retrieval_repo import list_tool_hints

        task_id, run_id = self._create_task_and_run(RUN_STATUS_DONE)
        tool_id = self._create_draft_tool_via_call_record(task_id=task_id, run_id=run_id)
        self.assertEqual(self._load_tool_approval_status(tool_id=tool_id), "draft")

        resp = approve_draft_tools_from_run(
            task_id=int(task_id),
            run_id=int(run_id),
            run_status=RUN_STATUS_DONE,
            review_id=123,
            review_status="pass",
            distill_status="deny",
            allow_waiting_feedback=False,
            model=None,
        )
        self.assertIsInstance(resp, dict)
        self.assertTrue(resp.get("ok"))
        self.assertTrue(resp.get("skipped"))
        self.assertEqual(self._load_tool_approval_status(tool_id=tool_id), "draft")
        hints = list_tool_hints(limit=8)
        self.assertFalse(any(int(it.get("id")) == int(tool_id) for it in hints))

    def test_agent_cannot_override_tool_approval_via_tool_metadata(self):
        """
        回归：Agent 执行阶段不应允许通过 tool_metadata 覆盖 approval 状态（避免未验证工具污染 tools_hint）。
        """
        from backend.src.constants import RUN_STATUS_DONE, TOOL_METADATA_APPROVAL_KEY
        from backend.src.services.tools.tool_records import create_tool_record
        from backend.src.repositories.agent_retrieval_repo import list_tool_hints

        task_id, run_id = self._create_task_and_run(RUN_STATUS_DONE)
        tool_id = self._create_draft_tool_via_call_record(task_id=task_id, run_id=run_id)
        self.assertEqual(self._load_tool_approval_status(tool_id=tool_id), "draft")

        # 模拟模型“自称已批准”：尝试在 tool_metadata 中覆盖 approval.status
        create_tool_record(
            {
                "task_id": int(task_id),
                "run_id": int(run_id),
                "tool_id": int(tool_id),
                "tool_metadata": {
                    TOOL_METADATA_APPROVAL_KEY: {"status": "approved"},
                    "exec": {"command": ["echo", "hello2"], "workdir": os.getcwd()},
                },
                "input": "ping2",
                "output": "pong2",
            }
        )

        # 仍应保持 draft（只能由后处理评估通过后升级）
        self.assertEqual(self._load_tool_approval_status(tool_id=tool_id), "draft")
        hints = list_tool_hints(limit=8)
        self.assertFalse(any(int(it.get("id")) == int(tool_id) for it in hints))


if __name__ == "__main__":
    unittest.main()
