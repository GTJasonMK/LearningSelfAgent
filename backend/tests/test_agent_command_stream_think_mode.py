import json
import os
import shutil
import tempfile
import time
import unittest
from collections import Counter
import re
import threading
from pathlib import Path
from unittest.mock import patch

try:
    import httpx  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    httpx = None


class TestAgentCommandStreamThinkMode(unittest.IsolatedAsyncioTestCase):
    """
    端到端回归：覆盖 /api/agent/command/stream（think 模式）的编排链路能跑通。

    重点断言：
    - Think runner 的 step_llm_config_resolver 会按 executor 角色选择不同模型；
    - SSE 链路可完成创建 run → 规划 → 执行 → 收敛落库。
    """

    def setUp(self):
        if httpx is None:
            self.skipTest("httpx 未安装，跳过需要 ASGI 客户端的测试")

        import backend.src.storage as storage

        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = Path(self._tmpdir.name) / "agent_test.db"
        self._prompt_root = Path(self._tmpdir.name) / "prompt"

        os.environ["AGENT_DB_PATH"] = str(self._db_path)
        os.environ["AGENT_PROMPT_ROOT"] = str(self._prompt_root)
        os.makedirs(self._prompt_root, exist_ok=True)

        storage.init_db()

    def tearDown(self):
        try:
            os.environ.pop("AGENT_DB_PATH", None)
            os.environ.pop("AGENT_PROMPT_ROOT", None)
        except Exception:
            pass

        for _ in range(10):
            try:
                self._tmpdir.cleanup()
                return
            except OSError:
                time.sleep(0.05)
        shutil.rmtree(self._tmpdir.name, ignore_errors=True)

    async def test_agent_command_stream_think_mode_runs_and_uses_executor_models(self):
        from backend.src.agent.retrieval import KnowledgeSufficiencyResult
        from backend.src.agent.think import ThinkPlanResult
        from backend.src.main import create_app
        from backend.src.storage import get_connection

        plan_titles = [
            "file_write:README.md 写文档",
            "file_write:main.py 写代码",
            "task_output 输出结果",
        ]
        plan_briefs = ["写文档", "写代码", "输出"]
        plan_allows = [["file_write"], ["file_write"], ["task_output"]]

        def _fake_run_think_planning_sync(*_args, **_kwargs):
            return ThinkPlanResult(
                plan_titles=list(plan_titles),
                plan_briefs=list(plan_briefs),
                plan_allows=[list(a) for a in plan_allows],
                plan_artifacts=[],
                winning_planner_id="planner_a",
            )

        models: list[str] = []

        def _fake_create_llm_call(payload: dict):
            models.append(str(payload.get("model") or ""))
            prompt_text = str(payload.get("prompt") or "")
            m = re.search(r"当前步骤（第\d+步）：([^\n]+)", prompt_text)
            step_title = str(m.group(1) if m else "").strip()
            if step_title.startswith("file_write:README.md"):
                action = {"action": {"type": "file_write", "payload": {"path": "README.md", "content": "doc"}}}
            elif step_title.startswith("file_write:main.py"):
                action = {"action": {"type": "file_write", "payload": {"path": "main.py", "content": "code"}}}
            elif step_title.startswith("task_output"):
                action = {"action": {"type": "task_output", "payload": {"output_type": "text", "content": "ok"}}}
            else:
                raise AssertionError(f"unexpected step_title: {step_title}")
            resp = json.dumps(action, ensure_ascii=False)
            return {"record": {"status": "success", "response": resp}}

        def _fake_execute_step_action(_task_id, _run_id, _step_row, context=None):
            return {"ok": True}, None

        sufficiency = KnowledgeSufficiencyResult(
            sufficient=True,
            reason="ok",
            missing_knowledge="none",
            suggestion="proceed",
            skill_count=0,
            graph_count=0,
            memory_count=0,
        )

        app = create_app()
        transport = httpx.ASGITransport(app=app)

        # 让 executor_doc/executor_code 使用不同模型，便于断言 resolver 是否生效
        think_config = {
            "agents": {
                "planner_a": "planner-model",
                "executor_doc": "doc-model",
                "executor_code": "code-model",
                "executor_test": "test-model",
                "evaluator": "eval-model",
            }
        }

        with patch(
            "backend.src.agent.runner.stream_think_run._select_relevant_graph_nodes",
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_think_run._filter_relevant_domains",
            return_value=["misc"],
        ), patch(
            "backend.src.agent.runner.stream_think_run._select_relevant_skills",
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_think_run._assess_knowledge_sufficiency",
            return_value=sufficiency,
        ), patch(
            "backend.src.agent.runner.stream_think_run._select_relevant_solutions",
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_think_run._collect_tools_from_solutions",
            return_value="(无)",
        ), patch(
            "backend.src.agent.runner.stream_think_run.run_think_planning_sync",
            side_effect=_fake_run_think_planning_sync,
        ), patch(
            "backend.src.agent.runner.stream_think_run.enqueue_postprocess_thread",
            return_value=None,
        ), patch(
            "backend.src.agent.runner.react_loop.create_llm_call",
            side_effect=_fake_create_llm_call,
        ), patch(
            "backend.src.agent.runner.react_loop._execute_step_action",
            side_effect=_fake_execute_step_action,
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream(
                    "POST",
                    "/api/agent/command/stream",
                    json={
                        "message": "test think stream",
                        "mode": "think",
                        "max_steps": 3,  # len(plan)=3，避免追加“确认满意度”
                        "model": "base-model",
                        "parameters": {"temperature": 0},
                        "think_config": think_config,
                    },
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        # 断言：step_llm_config_resolver 按 executor 选模型
        self.assertEqual(Counter(models), Counter(["doc-model", "code-model", "code-model"]))

        # 断言：run 已落库且 mode=think
        with get_connection() as conn:
            row = conn.execute(
                "SELECT agent_state, agent_plan FROM task_runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertIsNotNone(row)
        state = json.loads(row["agent_state"] or "{}")
        plan = json.loads(row["agent_plan"] or "{}")
        self.assertEqual(str(state.get("mode") or ""), "think")
        self.assertEqual(plan.get("titles"), plan_titles)

    async def test_think_mode_executes_independent_steps_in_parallel(self):
        """
        回归：Think 模式执行阶段应支持“按依赖并行调度”：
        - 两个无依赖的步骤分别分配给 executor_doc / executor_code；
        - 两步执行应发生时间重叠（不是串行一个跑完再跑另一个）。
        """
        from backend.src.agent.retrieval import KnowledgeSufficiencyResult
        from backend.src.agent.think import ThinkPlanResult
        from backend.src.main import create_app

        plan_titles = [
            "file_write:README.md 写文档",
            "file_write:main.py 写代码",
            "task_output 输出结果",
        ]
        plan_briefs = ["写文档", "写代码", "输出"]
        plan_allows = [["file_write"], ["file_write"], ["task_output"]]

        def _fake_run_think_planning_sync(*_args, **_kwargs):
            return ThinkPlanResult(
                plan_titles=list(plan_titles),
                plan_briefs=list(plan_briefs),
                plan_allows=[list(a) for a in plan_allows],
                plan_artifacts=[],
                winning_planner_id="planner_a",
            )

        step_models: dict[str, str] = {}
        step_models_lock = threading.Lock()

        def _fake_create_llm_call(payload: dict):
            prompt_text = str(payload.get("prompt") or "")
            m = re.search(r"当前步骤（第\d+步）：([^\n]+)", prompt_text)
            step_title = str(m.group(1) if m else "").strip()
            with step_models_lock:
                if step_title and step_title not in step_models:
                    step_models[step_title] = str(payload.get("model") or "")

            if step_title.startswith("file_write:README.md"):
                action = {"action": {"type": "file_write", "payload": {"path": "README.md", "content": "doc"}}}
            elif step_title.startswith("file_write:main.py"):
                action = {"action": {"type": "file_write", "payload": {"path": "main.py", "content": "code"}}}
            elif step_title.startswith("task_output"):
                action = {"action": {"type": "task_output", "payload": {"output_type": "text", "content": "ok"}}}
            else:
                raise AssertionError(f"unexpected step_title: {step_title}")
            return {"record": {"status": "success", "response": json.dumps(action, ensure_ascii=False)}}

        exec_events: list[dict] = []
        exec_events_lock = threading.Lock()

        def _fake_execute_step_action(_task_id, _run_id, _step_row, context=None):
            detail = json.loads(_step_row.get("detail") or "{}")
            action_type = str(detail.get("type") or "")
            payload = detail.get("payload") if isinstance(detail.get("payload"), dict) else {}
            title = str(_step_row.get("title") or "")

            started = time.monotonic()
            if action_type == "file_write":
                # 用足够大的 sleep 制造“可观测重叠”，避免并发测试过于脆弱。
                time.sleep(0.6)
                result = {"path": str(payload.get("path") or ""), "bytes": 1}
            elif action_type == "task_output":
                time.sleep(0.05)
                result = {"output_type": "text", "content": str(payload.get("content") or "")}
            else:
                result = {"ok": True}
            finished = time.monotonic()

            with exec_events_lock:
                exec_events.append(
                    {
                        "title": title,
                        "type": action_type,
                        "started": float(started),
                        "finished": float(finished),
                    }
                )
            return result, None

        sufficiency = KnowledgeSufficiencyResult(
            sufficient=True,
            reason="ok",
            missing_knowledge="none",
            suggestion="proceed",
            skill_count=0,
            graph_count=0,
            memory_count=0,
        )

        app = create_app()

        # executor_doc/executor_code 使用不同模型，便于断言 resolver 生效
        think_config = {
            "agents": {
                "planner_a": "planner-model",
                "executor_doc": "doc-model",
                "executor_code": "code-model",
                "executor_test": "test-model",
                "evaluator": "eval-model",
            }
        }

        transport = httpx.ASGITransport(app=app)
        start = time.monotonic()
        with patch(
            "backend.src.agent.runner.stream_think_run._select_relevant_graph_nodes",
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_think_run._filter_relevant_domains",
            return_value=["misc"],
        ), patch(
            "backend.src.agent.runner.stream_think_run._select_relevant_skills",
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_think_run._assess_knowledge_sufficiency",
            return_value=sufficiency,
        ), patch(
            "backend.src.agent.runner.stream_think_run._select_relevant_solutions",
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_think_run._collect_tools_from_solutions",
            return_value="(无)",
        ), patch(
            "backend.src.agent.runner.stream_think_run.run_think_planning_sync",
            side_effect=_fake_run_think_planning_sync,
        ), patch(
            "backend.src.agent.runner.stream_think_run.enqueue_postprocess_thread",
            return_value=None,
        ), patch(
            "backend.src.agent.runner.react_loop.create_llm_call",
            side_effect=_fake_create_llm_call,
        ), patch(
            "backend.src.agent.runner.react_loop._execute_step_action",
            side_effect=_fake_execute_step_action,
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream(
                    "POST",
                    "/api/agent/command/stream",
                    json={
                        "message": "test think parallel",
                        "mode": "think",
                        "max_steps": 3,  # len(plan)=3，避免追加“确认满意度”
                        "model": "base-model",
                        "parameters": {"temperature": 0},
                        "think_config": think_config,
                    },
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()
        _duration = time.monotonic() - start

        # 断言：模型按 executor 角色选择
        self.assertEqual(step_models.get("file_write:README.md 写文档"), "doc-model")
        self.assertEqual(step_models.get("file_write:main.py 写代码"), "code-model")

        # 断言：两个 file_write 的执行窗口发生重叠（说明不是串行）
        doc_ev = None
        code_ev = None
        for ev in exec_events:
            if ev.get("type") != "file_write":
                continue
            title = str(ev.get("title") or "")
            if title.startswith("file_write:README.md"):
                doc_ev = ev
            if title.startswith("file_write:main.py"):
                code_ev = ev
        self.assertIsNotNone(doc_ev)
        self.assertIsNotNone(code_ev)
        overlap = min(float(doc_ev["finished"]), float(code_ev["finished"])) - max(float(doc_ev["started"]), float(code_ev["started"]))
        self.assertGreater(overlap, 0.15)

        # 说明：此测试以“执行窗口重叠”为主判据；总耗时会受到落库/事件循环调度等影响，避免做过强约束导致波动。

    async def test_think_mode_user_prompt_waiting_and_resume_continues(self):
        """
        回归：Think 并行阶段出现 user_prompt 时，应进入 waiting 并可通过 resume 继续执行后续步骤。

        覆盖点：
        - 并行执行器识别 user_prompt 并写入 paused（含 step_id），run 状态置为 waiting；
        - /api/agent/command/resume/stream 注入用户回答后：
          - waiting step 被结算为 done（mark_task_step_done）；
          - user_prompt-only 步骤自动跳到下一步继续执行；
          - 最终收敛到 done。
        """
        from backend.src.agent.retrieval import KnowledgeSufficiencyResult
        from backend.src.agent.think import ThinkPlanResult
        from backend.src.agent.think.think_planning import ElaborationResult
        from backend.src.constants import RUN_STATUS_DONE, RUN_STATUS_WAITING
        from backend.src.main import create_app
        from backend.src.storage import get_connection

        plan_titles = [
            "file_write:a.txt 写文件",
            "user_prompt 请补充信息",
            "task_output 输出结果",
        ]
        plan_briefs = ["写文件", "提问", "输出"]
        plan_allows = [["file_write"], ["user_prompt"], ["task_output"]]

        def _fake_run_think_planning_sync(*_args, **_kwargs):
            return ThinkPlanResult(
                plan_titles=list(plan_titles),
                plan_briefs=list(plan_briefs),
                plan_allows=[list(a) for a in plan_allows],
                plan_artifacts=[],
                winning_planner_id="planner_a",
                elaboration=ElaborationResult(
                    planner_id="planner_a",
                    dependencies=[{"from_step": 0, "to_step": 1}],
                    raw_response="",
                ),
            )

        def _fake_create_llm_call(payload: dict):
            prompt_text = str(payload.get("prompt") or "")
            m = re.search(r"当前步骤（第\d+步）：([^\n]+)", prompt_text)
            step_title = str(m.group(1) if m else "").strip()
            if step_title.startswith("file_write:a.txt"):
                action = {"action": {"type": "file_write", "payload": {"path": "a.txt", "content": "x"}}}
            elif step_title.startswith("user_prompt"):
                action = {
                    "action": {
                        "type": "user_prompt",
                        "payload": {"question": "请输入版本号", "kind": "need_version"},
                    }
                }
            elif step_title.startswith("task_output"):
                action = {"action": {"type": "task_output", "payload": {"output_type": "text", "content": "done"}}}
            else:
                raise AssertionError(f"unexpected step_title: {step_title}")
            return {"record": {"status": "success", "response": json.dumps(action, ensure_ascii=False)}}

        def _fake_execute_step_action(_task_id, _run_id, _step_row, context=None):
            detail = json.loads(_step_row.get("detail") or "{}")
            action_type = str(detail.get("type") or "")
            payload = detail.get("payload") if isinstance(detail.get("payload"), dict) else {}
            if action_type == "file_write":
                return {"path": str(payload.get("path") or ""), "bytes": 1}, None
            if action_type == "task_output":
                return {"output_type": "text", "content": str(payload.get("content") or "")}, None
            return {"ok": True}, None

        sufficiency = KnowledgeSufficiencyResult(
            sufficient=True,
            reason="ok",
            missing_knowledge="none",
            suggestion="proceed",
            skill_count=0,
            graph_count=0,
            memory_count=0,
        )

        app = create_app()
        transport = httpx.ASGITransport(app=app)

        think_config = {
            "agents": {
                "planner_a": "planner-model",
                "executor_doc": "doc-model",
                "executor_code": "code-model",
                "executor_test": "test-model",
                "evaluator": "eval-model",
            }
        }

        # 第一次：执行到 user_prompt，进入 waiting
        with patch(
            "backend.src.agent.runner.stream_think_run._select_relevant_graph_nodes",
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_think_run._filter_relevant_domains",
            return_value=["misc"],
        ), patch(
            "backend.src.agent.runner.stream_think_run._select_relevant_skills",
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_think_run._assess_knowledge_sufficiency",
            return_value=sufficiency,
        ), patch(
            "backend.src.agent.runner.stream_think_run._select_relevant_solutions",
            return_value=[],
        ), patch(
            "backend.src.agent.runner.stream_think_run._collect_tools_from_solutions",
            return_value="(无)",
        ), patch(
            "backend.src.agent.runner.stream_think_run.run_think_planning_sync",
            side_effect=_fake_run_think_planning_sync,
        ), patch(
            "backend.src.agent.runner.stream_think_run.enqueue_postprocess_thread",
            return_value=None,
        ), patch(
            "backend.src.agent.runner.react_loop.create_llm_call",
            side_effect=_fake_create_llm_call,
        ), patch(
            "backend.src.agent.runner.react_loop._execute_step_action",
            side_effect=_fake_execute_step_action,
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream(
                    "POST",
                    "/api/agent/command/stream",
                    json={
                        "message": "test think user_prompt waiting",
                        "mode": "think",
                        "max_steps": 3,  # len(plan)=3，避免追加“确认满意度”
                        "model": "base-model",
                        "parameters": {"temperature": 0},
                        "think_config": think_config,
                    },
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        with get_connection() as conn:
            run_row = conn.execute(
                "SELECT id, status, agent_state FROM task_runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertIsNotNone(run_row)
        run_id = int(run_row["id"])
        self.assertEqual(str(run_row["status"] or ""), RUN_STATUS_WAITING)

        state = json.loads(run_row["agent_state"] or "{}")
        self.assertIsInstance(state.get("paused"), dict)
        self.assertEqual(int(state.get("paused", {}).get("step_order") or 0), 2)
        self.assertTrue(bool(state.get("paused", {}).get("step_id")))
        self.assertIsInstance(state.get("think_parallel_dependencies"), list)

        # resume：输入回答，应继续执行 step3 并收敛到 done
        with patch(
            "backend.src.agent.runner.stream_resume_run.enqueue_postprocess_thread",
            return_value=None,
        ), patch(
            "backend.src.agent.runner.react_loop.create_llm_call",
            side_effect=_fake_create_llm_call,
        ), patch(
            "backend.src.agent.runner.react_loop._execute_step_action",
            side_effect=_fake_execute_step_action,
        ):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream(
                    "POST",
                    "/api/agent/command/resume/stream",
                    json={"run_id": run_id, "message": "v1.0.0"},
                ) as resp:
                    self.assertEqual(resp.status_code, 200)
                    await resp.aread()

        with get_connection() as conn:
            row = conn.execute("SELECT status, agent_state FROM task_runs WHERE id = ?", (int(run_id),)).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(str(row["status"] or ""), RUN_STATUS_DONE)

        state2 = json.loads(row["agent_state"] or "{}")
        self.assertIsNone(state2.get("paused"))
        # user_prompt-only 步骤应被自动结算为 done（避免 resume 后卡回提问）
        with get_connection() as conn:
            step_row = conn.execute(
                "SELECT status FROM task_steps WHERE run_id = ? AND step_order = ? ORDER BY id DESC LIMIT 1",
                (int(run_id), 2),
            ).fetchone()
        self.assertIsNotNone(step_row)
        self.assertEqual(str(step_row["status"] or ""), "done")


if __name__ == "__main__":
    unittest.main()
