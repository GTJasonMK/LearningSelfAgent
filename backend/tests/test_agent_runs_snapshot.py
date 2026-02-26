import os
import importlib.util
import tempfile
import unittest

try:
    import httpx  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    httpx = None

HAS_FASTAPI = importlib.util.find_spec("fastapi") is not None


class TestAgentRunsSnapshot(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        if httpx is None:
            self.skipTest("httpx 未安装，跳过需要 ASGI 客户端的测试")
        if not HAS_FASTAPI:
            self.skipTest("fastapi 未安装，跳过需要 ASGI 客户端的测试")

        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._tmpdir.name, "agent_test.db")

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

    async def test_agent_run_detail_includes_snapshot_and_counters(self):
        from backend.src.common.utils import now_iso
        from backend.src.main import create_app
        from backend.src.repositories.task_runs_repo import update_task_run
        from backend.src.repositories.task_steps_repo import TaskStepCreateParams, create_task_step
        from backend.src.repositories.tool_call_records_repo import (
            ToolCallRecordCreateParams,
            create_tool_call_record,
        )
        from backend.src.services.llm.llm_calls import create_llm_call
        from backend.src.services.tasks.task_run_lifecycle import create_task_and_run_records_for_agent
        from backend.src.storage import get_connection

        task_id, run_id = create_task_and_run_records_for_agent(message="demo", created_at=now_iso())

        # plan/state：只需要最小字段即可生成 plan snapshot
        plan_titles = ["shell_command: echo hi", "task_output 输出最终结果"]
        plan_allows = [["shell_command"], ["task_output"]]
        plan_items = [
            {"id": 1, "brief": "自测", "status": "done"},
            {"id": 2, "brief": "输出", "status": "waiting"},
        ]
        agent_plan = {"titles": plan_titles, "items": plan_items, "allows": plan_allows, "artifacts": []}
        agent_state = {"mode": "do", "stage": "execute", "step_order": 2, "paused": {"question": "ok?"}}

        update_task_run(run_id=int(run_id), status="waiting", agent_plan=agent_plan, agent_state=agent_state, updated_at=now_iso())

        # task_steps：插入一条失败记录，作为 last_error
        create_task_step(
            TaskStepCreateParams(
                task_id=int(task_id),
                run_id=int(run_id),
                title="shell_command: echo hi",
                status="failed",
                error="boom",
                step_order=1,
                started_at=now_iso(),
                finished_at=now_iso(),
            )
        )

        # tool_call_records：插入 2 条（其中 1 条复用 pass）
        create_tool_call_record(
            ToolCallRecordCreateParams(
                tool_id=1,
                task_id=int(task_id),
                skill_id=None,
                run_id=int(run_id),
                reuse=1,
                reuse_status="pass",
                reuse_notes=None,
                input="in",
                output="out",
            )
        )
        create_tool_call_record(
            ToolCallRecordCreateParams(
                tool_id=2,
                task_id=int(task_id),
                skill_id=None,
                run_id=int(run_id),
                reuse=0,
                reuse_status=None,
                reuse_notes=None,
                input="in2",
                output="out2",
            )
        )

        # llm_records：用 dry_run 写入 1 条，再补 token 字段（避免真实网络调用）
        llm = create_llm_call(
            {
                "prompt": "hi",
                "task_id": int(task_id),
                "run_id": int(run_id),
                "model": "gpt-test",
                "parameters": {"temperature": 0},
                "dry_run": True,
            }
        )
        record_id = int(llm["record"]["id"])
        with get_connection() as conn:
            conn.execute(
                "UPDATE llm_records SET tokens_total = ? WHERE id = ?",
                (123, int(record_id)),
            )

        app = create_app()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(f"/api/agent/runs/{int(run_id)}")
            self.assertEqual(resp.status_code, 200)
            data = resp.json()

        self.assertIn("snapshot", data)
        snapshot = data["snapshot"] or {}

        # waiting run：stage 必须收敛为 waiting_input（优先 run.status）
        self.assertEqual(snapshot.get("stage"), "waiting_input")

        plan = (snapshot.get("plan") or {}) if isinstance(snapshot.get("plan"), dict) else {}
        self.assertEqual(int(plan.get("total") or 0), 2)
        cur = plan.get("current_step") or {}
        self.assertEqual(int(cur.get("step_order") or 0), 2)

        counters = snapshot.get("counters") or {}
        self.assertTrue(bool(counters.get("ok")))
        self.assertEqual(int(counters.get("llm", {}).get("calls") or 0), 1)
        self.assertEqual(int(counters.get("llm", {}).get("tokens_total") or 0), 123)
        self.assertEqual(int(counters.get("tools", {}).get("calls") or 0), 2)
        self.assertEqual(int(counters.get("tools", {}).get("reuse_calls") or 0), 1)
        self.assertEqual(int(counters.get("tools", {}).get("pass_calls") or 0), 1)

        last_error = counters.get("last_error") or {}
        self.assertEqual(int(last_error.get("step_order") or 0), 1)
        self.assertIn("boom", str(last_error.get("error") or ""))

    async def test_agent_run_events_endpoint_supports_replay(self):
        from backend.src.common.utils import now_iso
        from backend.src.main import create_app
        from backend.src.repositories.task_run_events_repo import create_task_run_event
        from backend.src.services.tasks.task_run_lifecycle import create_task_and_run_records_for_agent

        task_id, run_id = create_task_and_run_records_for_agent(message="events", created_at=now_iso())
        create_task_run_event(
            task_id=int(task_id),
            run_id=int(run_id),
            session_key="sess_e",
            event_id="sess_e:1:1:run_status",
            event_type="run_status",
            payload={
                "type": "run_status",
                "event_id": "sess_e:1:1:run_status",
                "status": "running",
            },
        )
        create_task_run_event(
            task_id=int(task_id),
            run_id=int(run_id),
            session_key="sess_e",
            event_id="sess_e:1:2:need_input",
            event_type="need_input",
            payload={
                "type": "need_input",
                "event_id": "sess_e:1:2:need_input",
                "question": "继续吗？",
            },
        )

        app = create_app()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            full_resp = await client.get(f"/api/agent/runs/{int(run_id)}/events")
            self.assertEqual(full_resp.status_code, 200)
            full_items = (full_resp.json() or {}).get("items") or []
            self.assertEqual(len(full_items), 2)

            delta_resp = await client.get(
                f"/api/agent/runs/{int(run_id)}/events",
                params={"after_event_id": "sess_e:1:1:run_status"},
            )
            self.assertEqual(delta_resp.status_code, 200)
            delta_items = (delta_resp.json() or {}).get("items") or []
            self.assertEqual(len(delta_items), 1)
            self.assertEqual(str((delta_items[0] or {}).get("event_type") or ""), "need_input")


if __name__ == "__main__":
    unittest.main()

