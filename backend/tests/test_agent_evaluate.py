import json
import os
import tempfile
import unittest
from unittest.mock import patch

try:
    import httpx  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    httpx = None


class TestAgentEvaluate(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        if httpx is None:
            self.skipTest("httpx 未安装，跳过需要 ASGI 客户端的测试")

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

    async def test_agent_evaluate_stream_inserts_review_and_skills(self):
        from backend.src.storage import get_connection
        from backend.src.main import create_app
        from backend.src.api.utils import now_iso

        created_at = now_iso()
        step_id = None
        output_id = None
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("测试任务", "done", created_at, None, created_at, created_at),
            )
            task_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_plan, agent_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    "done",
                    "test_run",
                    created_at,
                    created_at,
                    created_at,
                    created_at,
                    json.dumps({"titles": ["一步"], "allows": [["task_output"]], "artifacts": []}, ensure_ascii=False),
                    json.dumps({"message": "测试", "step_order": 1}, ensure_ascii=False),
                ),
            )
            run_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_id,
                    "输出结果",
                    "done",
                    json.dumps({"type": "task_output", "payload": {"output_type": "text", "content": "hi"}}, ensure_ascii=False),
                    json.dumps({"content": "hi"}, ensure_ascii=False),
                    None,
                    1,
                    created_at,
                    created_at,
                    1,
                    created_at,
                    created_at,
                ),
            )
            step_id = int(cursor.lastrowid)
            conn.execute(
                "INSERT INTO task_outputs (task_id, run_id, output_type, content, created_at) VALUES (?, ?, ?, ?, ?)",
                (task_id, run_id, "text", "hi", created_at),
            )
            output_row = conn.execute(
                "SELECT id FROM task_outputs WHERE run_id = ? ORDER BY id DESC LIMIT 1",
                (int(run_id),),
            ).fetchone()
            output_id = int(output_row["id"]) if output_row and output_row["id"] is not None else None

        fake_eval_json = json.dumps(
            {
                "status": "pass",
                "pass_score": 92,
                "pass_threshold": 80,
                "distill": {
                    "status": "allow",
                    "score": 95,
                    "threshold": 90,
                    "reason": "任务完成且技能可迁移",
                    "evidence_refs": [
                        {"kind": "step", "step_id": int(step_id), "step_order": 1},
                        {"kind": "output", "output_id": int(output_id)},
                    ],
                },
                "summary": "任务完成，可沉淀技能。",
                "issues": [
                    {
                        "title": "示例问题",
                        "severity": "low",
                        "details": "这里只是测试",
                        "evidence_refs": [{"kind": "output", "output_id": int(output_id)}],
                        "evidence_quote": "output: hi",
                        "suggestion": "无",
                    }
                ],
                "next_actions": [{"title": "下一步", "details": "继续完善"}],
                "skills": [
                    {
                        "mode": "create",
                        "name": "测试技能",
                        "description": "用于测试 upsert",
                        "category": "misc",
                        "tags": ["test"],
                        "triggers": ["测试"],
                        "steps": ["step1"],
                        "validation": ["ok"],
                    }
                ],
            },
            ensure_ascii=False,
        )

        app = create_app()
        transport = httpx.ASGITransport(app=app)

        calls = []

        async def fake_to_thread(fn, *args, **kwargs):
            calls.append((fn, args, kwargs))
            return fn(*args, **kwargs)

        with patch(
            "backend.src.api.agent.routes_agent_evaluate.call_openai",
            return_value=(fake_eval_json, None, None),
        ) as mock_call_openai, patch(
            "backend.src.api.agent.routes_agent_evaluate.publish_skill_file",
            return_value=("misc/test_skill.md", None),
        ), patch(
            "backend.src.api.agent.routes_agent_evaluate.asyncio.to_thread",
            fake_to_thread,
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream(
                    "POST",
                    "/api/agent/evaluate/stream",
                    json={"run_id": run_id, "message": "验收点：能运行"},
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    got_review = False
                    async for line in resp.aiter_lines():
                        if not line:
                            continue
                        if line.startswith("data: "):
                            payload = json.loads(line[len("data: ") :])
                            if payload.get("type") == "review":
                                got_review = True
                    self.assertTrue(got_review)

        self.assertTrue(any(call[0] is mock_call_openai for call in calls))

        # DB 校验：review + skill 都应存在
        with get_connection() as conn:
            review = conn.execute(
                "SELECT * FROM agent_review_records WHERE run_id = ? ORDER BY id DESC LIMIT 1",
                (run_id,),
            ).fetchone()
            self.assertIsNotNone(review)
            refs = json.loads(review["distill_evidence_refs"] or "[]")
            self.assertEqual(
                refs,
                [
                    {"kind": "step", "step_id": int(step_id), "step_order": 1},
                    {"kind": "output", "output_id": int(output_id)},
                ],
            )
            skill = conn.execute(
                "SELECT * FROM skills_items WHERE name = ? ORDER BY id DESC LIMIT 1",
                ("测试技能",),
            ).fetchone()
            self.assertIsNotNone(skill)

    async def test_agent_evaluate_stream_distill_allow_but_missing_evidence_refs_should_skip_upsert(self):
        from backend.src.storage import get_connection
        from backend.src.main import create_app
        from backend.src.api.utils import now_iso

        created_at = now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("测试任务", "done", created_at, None, created_at, created_at),
            )
            task_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_plan, agent_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    "done",
                    "test_run_skip_distill",
                    created_at,
                    created_at,
                    created_at,
                    created_at,
                    json.dumps({"titles": ["一步"], "allows": [["task_output"]], "artifacts": []}, ensure_ascii=False),
                    json.dumps({"message": "测试", "step_order": 1}, ensure_ascii=False),
                ),
            )
            run_id = int(cursor.lastrowid)
            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_id,
                    "输出结果",
                    "done",
                    json.dumps({"type": "task_output", "payload": {"output_type": "text", "content": "hi"}}, ensure_ascii=False),
                    json.dumps({"content": "hi"}, ensure_ascii=False),
                    None,
                    1,
                    created_at,
                    created_at,
                    1,
                    created_at,
                    created_at,
                ),
            )
            conn.execute(
                "INSERT INTO task_outputs (task_id, run_id, output_type, content, created_at) VALUES (?, ?, ?, ?, ?)",
                (task_id, run_id, "text", "hi", created_at),
            )

        fake_eval_json = json.dumps(
            {
                "status": "pass",
                "pass_score": 92,
                "pass_threshold": 80,
                "distill": {
                    "status": "allow",
                    "score": 95,
                    "threshold": 90,
                    "reason": "缺少证据引用，不应自动沉淀",
                    "evidence_refs": [],
                },
                "summary": "任务完成，但 distill 缺证据。",
                "issues": [],
                "next_actions": [],
                "skills": [
                    {
                        "mode": "create",
                        "name": "不应沉淀的技能",
                        "description": "用于测试 distill evidence_refs 门槛",
                        "category": "misc",
                        "tags": ["test"],
                        "triggers": ["测试"],
                        "steps": ["step1"],
                        "validation": ["ok"],
                    }
                ],
            },
            ensure_ascii=False,
        )

        app = create_app()
        transport = httpx.ASGITransport(app=app)

        async def fake_to_thread(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        with patch(
            "backend.src.api.agent.routes_agent_evaluate.call_openai",
            return_value=(fake_eval_json, None, None),
        ), patch(
            "backend.src.api.agent.routes_agent_evaluate.upsert_skill_from_agent_payload",
        ) as mock_upsert, patch(
            "backend.src.api.agent.routes_agent_evaluate.publish_skill_file",
        ) as mock_publish, patch(
            "backend.src.api.agent.routes_agent_evaluate.asyncio.to_thread",
            fake_to_thread,
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream(
                    "POST",
                    "/api/agent/evaluate/stream",
                    json={"run_id": run_id, "message": "验收点：distill 缺证据应降级"},
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    got_review = False
                    got_distill_status = None
                    async for line in resp.aiter_lines():
                        if not line:
                            continue
                        if line.startswith("data: "):
                            payload = json.loads(line[len("data: ") :])
                            if payload.get("type") == "review":
                                got_review = True
                                got_distill_status = payload.get("distill_status")
                    self.assertTrue(got_review)
                    self.assertEqual(got_distill_status, "manual")

        self.assertFalse(mock_upsert.called)
        self.assertFalse(mock_publish.called)

        with get_connection() as conn:
            skill = conn.execute(
                "SELECT * FROM skills_items WHERE name = ? ORDER BY id DESC LIMIT 1",
                ("不应沉淀的技能",),
            ).fetchone()
            self.assertIsNone(skill)
            review = conn.execute(
                "SELECT * FROM agent_review_records WHERE run_id = ? ORDER BY id DESC LIMIT 1",
                (run_id,),
            ).fetchone()
            self.assertIsNotNone(review)
            self.assertEqual(str(review["distill_status"] or ""), "manual")

    async def test_agent_evaluate_stream_distill_allow_but_invalid_evidence_refs_should_skip_upsert(self):
        from backend.src.storage import get_connection
        from backend.src.main import create_app
        from backend.src.api.utils import now_iso

        created_at = now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("测试任务", "done", created_at, None, created_at, created_at),
            )
            task_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_plan, agent_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    "done",
                    "test_run_invalid_refs",
                    created_at,
                    created_at,
                    created_at,
                    created_at,
                    json.dumps({"titles": ["一步"], "allows": [["task_output"]], "artifacts": []}, ensure_ascii=False),
                    json.dumps({"message": "测试", "step_order": 1}, ensure_ascii=False),
                ),
            )
            run_id = int(cursor.lastrowid)
            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_id,
                    "输出结果",
                    "done",
                    json.dumps({"type": "task_output", "payload": {"output_type": "text", "content": "hi"}}, ensure_ascii=False),
                    json.dumps({"content": "hi"}, ensure_ascii=False),
                    None,
                    1,
                    created_at,
                    created_at,
                    1,
                    created_at,
                    created_at,
                ),
            )
            conn.execute(
                "INSERT INTO task_outputs (task_id, run_id, output_type, content, created_at) VALUES (?, ?, ?, ?, ?)",
                (task_id, run_id, "text", "hi", created_at),
            )

        fake_eval_json = json.dumps(
            {
                "status": "pass",
                "pass_score": 92,
                "pass_threshold": 80,
                "distill": {
                    "status": "allow",
                    "score": 95,
                    "threshold": 90,
                    "reason": "引用了不存在的证据 id，不应自动沉淀",
                    "evidence_refs": [{"kind": "output", "output_id": 999999}],
                },
                "summary": "任务完成，但 distill 证据引用无效。",
                "issues": [],
                "next_actions": [],
                "skills": [
                    {
                        "mode": "create",
                        "name": "不应沉淀的技能（invalid refs）",
                        "description": "用于测试 distill invalid evidence_refs 门槛",
                        "category": "misc",
                        "tags": ["test"],
                        "triggers": ["测试"],
                        "steps": ["step1"],
                        "validation": ["ok"],
                    }
                ],
            },
            ensure_ascii=False,
        )

        app = create_app()
        transport = httpx.ASGITransport(app=app)

        async def fake_to_thread(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        with patch(
            "backend.src.api.agent.routes_agent_evaluate.call_openai",
            return_value=(fake_eval_json, None, None),
        ), patch(
            "backend.src.api.agent.routes_agent_evaluate.upsert_skill_from_agent_payload",
        ) as mock_upsert, patch(
            "backend.src.api.agent.routes_agent_evaluate.publish_skill_file",
        ) as mock_publish, patch(
            "backend.src.api.agent.routes_agent_evaluate.asyncio.to_thread",
            fake_to_thread,
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream(
                    "POST",
                    "/api/agent/evaluate/stream",
                    json={"run_id": run_id, "message": "验收点：distill invalid refs 应降级"},
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    got_review = False
                    got_distill_status = None
                    got_distill_refs = None
                    async for line in resp.aiter_lines():
                        if not line:
                            continue
                        if line.startswith("data: "):
                            payload = json.loads(line[len("data: ") :])
                            if payload.get("type") == "review":
                                got_review = True
                                got_distill_status = payload.get("distill_status")
                                got_distill_refs = payload.get("distill_evidence_refs")
                    self.assertTrue(got_review)
                    self.assertEqual(got_distill_status, "manual")
                    self.assertEqual(got_distill_refs, [])

        self.assertFalse(mock_upsert.called)
        self.assertFalse(mock_publish.called)

        with get_connection() as conn:
            skill = conn.execute(
                "SELECT * FROM skills_items WHERE name = ? ORDER BY id DESC LIMIT 1",
                ("不应沉淀的技能（invalid refs）",),
            ).fetchone()
            self.assertIsNone(skill)
            review = conn.execute(
                "SELECT * FROM agent_review_records WHERE run_id = ? ORDER BY id DESC LIMIT 1",
                (run_id,),
            ).fetchone()
            self.assertIsNotNone(review)
            self.assertEqual(str(review["distill_status"] or ""), "manual")
            refs = json.loads(review["distill_evidence_refs"] or "[]")
            self.assertEqual(refs, [])

    async def test_agent_evaluate_stream_think_mode_uses_evaluator_model_from_config(self):
        from backend.src.storage import get_connection
        from backend.src.main import create_app
        from backend.src.api.utils import now_iso

        created_at = now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("测试任务", "done", created_at, None, created_at, created_at),
            )
            task_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_plan, agent_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    "done",
                    "test_run_think",
                    created_at,
                    created_at,
                    created_at,
                    created_at,
                    json.dumps({"titles": ["一步"], "allows": [["task_output"]], "artifacts": []}, ensure_ascii=False),
                    json.dumps(
                        {
                            "mode": "think",
                            "model": "base-model",
                            "think_config": {"agents": {"evaluator": "eval-model"}},
                            "winning_planner_id": "planner_a",
                            "vote_records": [{"planner_id": "planner_a", "votes": [1]}],
                            "alternative_plans": [{"planner_id": "planner_b", "plan": [{"title": "B"}]}],
                        },
                        ensure_ascii=False,
                    ),
                ),
            )
            run_id = int(cursor.lastrowid)
            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_id,
                    "输出结果",
                    "done",
                    json.dumps({"type": "task_output", "payload": {"output_type": "text", "content": "hi"}}, ensure_ascii=False),
                    json.dumps({"content": "hi"}, ensure_ascii=False),
                    None,
                    1,
                    created_at,
                    created_at,
                    1,
                    created_at,
                    created_at,
                ),
            )
            conn.execute(
                "INSERT INTO task_outputs (task_id, run_id, output_type, content, created_at) VALUES (?, ?, ?, ?, ?)",
                (task_id, run_id, "text", "hi", created_at),
            )

        fake_eval_json = json.dumps(
            {
                "status": "pass",
                "summary": "ok",
                "issues": [],
                "next_actions": [],
                "skills": [],
            },
            ensure_ascii=False,
        )

        app = create_app()
        transport = httpx.ASGITransport(app=app)

        calls = []

        async def fake_to_thread(fn, *args, **kwargs):
            calls.append((fn, args, kwargs))
            return fn(*args, **kwargs)

        with patch(
            "backend.src.api.agent.routes_agent_evaluate.call_openai",
            return_value=(fake_eval_json, None, None),
        ) as mock_call_openai, patch(
            "backend.src.api.agent.routes_agent_evaluate.asyncio.to_thread",
            fake_to_thread,
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream(
                    "POST",
                    "/api/agent/evaluate/stream",
                    json={"run_id": run_id, "message": "验收点：think 模式评估默认模型选择"},
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    # 消费流即可（断言在 call 参数里）
                    async for _line in resp.aiter_lines():
                        pass

        eval_calls = [c for c in calls if c[0] is mock_call_openai]
        self.assertTrue(eval_calls, "call_openai 未被调用")
        prompt_text, model_used, _params = eval_calls[0][1]
        self.assertEqual(model_used, "eval-model")
        self.assertIn('"mode": "think"', str(prompt_text))
        self.assertIn("vote_records", str(prompt_text))
        self.assertIn("artifacts_check", str(prompt_text))

    async def test_agent_evaluate_stream_think_mode_defaults_to_base_model_when_evaluator_missing(self):
        """
        验证：think_config 未配置 evaluator 时，评估默认使用 run 的 base model（state.model）。
        """
        from backend.src.storage import get_connection
        from backend.src.main import create_app
        from backend.src.api.utils import now_iso

        created_at = now_iso()
        with get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO tasks (title, status, created_at, expectation_id, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("think 评估模型兜底测试", "done", created_at, None, created_at, created_at),
            )
            task_id = int(cursor.lastrowid)
            cursor = conn.execute(
                "INSERT INTO task_runs (task_id, status, summary, started_at, finished_at, created_at, updated_at, agent_plan, agent_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    "done",
                    "test_run_think",
                    created_at,
                    created_at,
                    created_at,
                    created_at,
                    json.dumps({"titles": ["一步"], "allows": [["task_output"]], "artifacts": []}, ensure_ascii=False),
                    json.dumps(
                        {
                            "mode": "think",
                            "model": "base-model",
                            "think_config": {"agents": {"planner_a": "planner-model"}},
                        },
                        ensure_ascii=False,
                    ),
                ),
            )
            run_id = int(cursor.lastrowid)
            conn.execute(
                "INSERT INTO task_steps (task_id, run_id, title, status, detail, result, error, attempts, started_at, finished_at, step_order, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    run_id,
                    "输出结果",
                    "done",
                    json.dumps({"type": "task_output", "payload": {"output_type": "text", "content": "hi"}}, ensure_ascii=False),
                    json.dumps({"content": "hi"}, ensure_ascii=False),
                    None,
                    1,
                    created_at,
                    created_at,
                    1,
                    created_at,
                    created_at,
                ),
            )
            conn.execute(
                "INSERT INTO task_outputs (task_id, run_id, output_type, content, created_at) VALUES (?, ?, ?, ?, ?)",
                (task_id, run_id, "text", "hi", created_at),
            )

        fake_eval_json = json.dumps(
            {
                "status": "pass",
                "summary": "ok",
                "issues": [],
                "next_actions": [],
                "skills": [],
            },
            ensure_ascii=False,
        )

        app = create_app()
        transport = httpx.ASGITransport(app=app)

        calls = []

        async def fake_to_thread(fn, *args, **kwargs):
            calls.append((fn, args, kwargs))
            return fn(*args, **kwargs)

        with patch(
            "backend.src.api.agent.routes_agent_evaluate.call_openai",
            return_value=(fake_eval_json, None, None),
        ) as mock_call_openai, patch(
            "backend.src.api.agent.routes_agent_evaluate.asyncio.to_thread",
            fake_to_thread,
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream(
                    "POST",
                    "/api/agent/evaluate/stream",
                    json={"run_id": run_id, "message": "验收点：think evaluator 缺失兜底"},
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    async for _line in resp.aiter_lines():
                        pass

        eval_calls = [c for c in calls if c[0] is mock_call_openai]
        self.assertTrue(eval_calls, "call_openai 未被调用")
        _prompt_text, model_used, _params = eval_calls[0][1]
        self.assertEqual(model_used, "base-model")


if __name__ == "__main__":
    unittest.main()
