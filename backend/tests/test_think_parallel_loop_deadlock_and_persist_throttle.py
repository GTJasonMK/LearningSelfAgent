import json
import unittest
from unittest.mock import patch

from backend.src.agent.core.plan_structure import PlanStructure


class TestThinkParallelLoopGuardrails(unittest.TestCase):
    """
    针对 think_parallel_loop 的“兜底保障”回归测试：
    - persist_loop_state 对 done 步骤应节流（避免高频写库）
    - 当剩余步骤无可运行（依赖在本次区间外/依赖永远不满足）时，应 fail-fast 而不是无限等待
    """

    def test_think_parallel_persist_throttles_done_steps(self):
        from backend.src.agent.runner.think_parallel_loop import run_think_parallel_loop

        plan_titles = [f"file_write:file_{i}.txt 写文件" for i in range(1, 13)] + ["task_output 输出结果"]
        plan_allows = [["file_write"] for _ in range(0, 12)] + [["task_output"]]

        persist_calls: list[dict] = []

        def _fake_persist_loop_state(*_args, **kwargs):
            persist_calls.append({"where": kwargs.get("where"), "status": kwargs.get("status")})
            return True

        def _fake_generate_action_with_retry(*_args, **kwargs):
            step_title = str(kwargs.get("step_title") or "")
            if step_title.startswith("task_output"):
                action_type = "task_output"
                payload = {"output_type": "text", "content": "ok"}
            else:
                # file_write:file_1.txt ... -> file_1.txt
                path = step_title.split("file_write:", 1)[-1].split(" ", 1)[0].strip() or "x.txt"
                action_type = "file_write"
                payload = {"path": path, "content": "x"}
            action_obj = {"action": {"type": action_type, "payload": payload}}
            return action_obj, action_type, payload, None, json.dumps(action_obj, ensure_ascii=False)

        def _fake_execute_step_action(_task_id, _run_id, _step_row, context=None):
            return {"ok": True}, None

        def _safe_write_debug(*_args, **_kwargs):
            return None

        gen = None
        with patch(
            "backend.src.agent.runner.think_parallel_loop.persist_loop_state",
            side_effect=_fake_persist_loop_state,
        ), patch(
            "backend.src.agent.runner.think_parallel_loop.generate_action_with_retry",
            side_effect=_fake_generate_action_with_retry,
        ), patch(
            "backend.src.agent.runner.think_parallel_loop.create_task_step",
            return_value=(1, "", ""),
        ), patch(
            "backend.src.agent.runner.think_parallel_loop.mark_task_step_done",
            return_value=None,
        ), patch(
            "backend.src.agent.runner.think_parallel_loop.mark_task_step_failed",
            return_value=None,
        ), patch(
            # 固定 monotonic：让 done 的落盘尽量被节流合并（只留 final flush）
            "backend.src.agent.runner.think_parallel_loop.time.monotonic",
            return_value=0.0,
        ):
            gen = run_think_parallel_loop(
                task_id=1,
                run_id=1,
                message="test",
                workdir=".",
                model="base",
                parameters={},
                plan_struct=PlanStructure.from_legacy(
                    plan_titles=list(plan_titles),
                    plan_items=[{"id": i + 1, "brief": "", "status": "pending"} for i in range(len(plan_titles))],
                    plan_allows=[list(a) for a in plan_allows],
                    plan_artifacts=[],
                ),
                tools_hint="",
                skills_hint="",
                memories_hint="",
                graph_hint="",
                agent_state={},
                context={},
                observations=[],
                start_step_order=1,
                end_step_order_inclusive=None,
                variables_source="test",
                step_llm_config_resolver=None,
                dependencies=None,
                executor_roles=None,
                llm_call=lambda _payload: {"record": {"status": "success", "response": "{}"}},
                execute_step_action=_fake_execute_step_action,
                safe_write_debug=_safe_write_debug,
            )

            # drain generator 并拿到返回值
            for _ in range(10000):
                try:
                    next(gen)
                except StopIteration as e:
                    result = e.value
                    break
            else:
                self.fail("think_parallel_loop 未在预期迭代次数内结束（可能发生死锁）")

        self.assertEqual(str(result.run_status), "done")

        # 关键断言：节流应显著减少 persist 次数（不应等于步骤数）
        self.assertGreaterEqual(len(persist_calls), 1)
        self.assertLess(len(persist_calls), len(plan_titles))

    def test_think_parallel_deadlock_due_to_dependency_outside_window_fails_fast(self):
        """
        回归：当 end_step_order_inclusive 把某些 step 排除出本次并行区间时，
        若剩余 step 依赖了“区间外的 step”，应 fail-fast（而不是无限等待）。
        """
        from backend.src.agent.runner.think_parallel_loop import run_think_parallel_loop

        plan_titles = [
            "file_write:README.md 写文档",
            "file_write:main.py 写代码",
            "file_write:utils.py 写工具",
            "task_output 输出结果",
        ]
        plan_allows = [["file_write"], ["file_write"], ["file_write"], ["task_output"]]

        # 人为制造：第 1 步依赖第 3 步（utils.py），但本次仅执行到第 2 步（第 3/4 步被排除）
        dependencies = [{"step_index": 0, "depends_on": [2]}]

        debug_messages: list[str] = []
        persist_statuses: list[str] = []

        def _fake_persist_loop_state(*_args, **kwargs):
            persist_statuses.append(str(kwargs.get("status") or ""))
            return True

        def _fake_generate_action_with_retry(*_args, **kwargs):
            step_title = str(kwargs.get("step_title") or "")
            # 本次只会执行到第 2 步即可触发后续 deadlock；action 内容无关紧要，只需满足 allow=file_write
            action_obj = {"action": {"type": "file_write", "payload": {"path": "main.py", "content": "x"}}}
            return action_obj, "file_write", {"path": "main.py", "content": "x"}, None, json.dumps(action_obj, ensure_ascii=False)

        def _fake_execute_step_action(_task_id, _run_id, _step_row, context=None):
            return {"ok": True}, None

        def _safe_write_debug(*_args, **kwargs):
            debug_messages.append(str(kwargs.get("message") or ""))

        gen = None
        with patch(
            "backend.src.agent.runner.think_parallel_loop.persist_loop_state",
            side_effect=_fake_persist_loop_state,
        ), patch(
            "backend.src.agent.runner.think_parallel_loop.generate_action_with_retry",
            side_effect=_fake_generate_action_with_retry,
        ), patch(
            "backend.src.agent.runner.think_parallel_loop.create_task_step",
            return_value=(1, "", ""),
        ), patch(
            "backend.src.agent.runner.think_parallel_loop.mark_task_step_done",
            return_value=None,
        ), patch(
            "backend.src.agent.runner.think_parallel_loop.mark_task_step_failed",
            return_value=None,
        ):
            gen = run_think_parallel_loop(
                task_id=1,
                run_id=1,
                message="test",
                workdir=".",
                model="base",
                parameters={},
                plan_struct=PlanStructure.from_legacy(
                    plan_titles=list(plan_titles),
                    plan_items=[{"id": i + 1, "brief": "", "status": "pending"} for i in range(len(plan_titles))],
                    plan_allows=[list(a) for a in plan_allows],
                    plan_artifacts=[],
                ),
                tools_hint="",
                skills_hint="",
                memories_hint="",
                graph_hint="",
                agent_state={},
                context={},
                observations=[],
                start_step_order=1,
                end_step_order_inclusive=2,  # 只执行前两步
                variables_source="test",
                step_llm_config_resolver=None,
                dependencies=dependencies,
                executor_roles=None,
                llm_call=lambda _payload: {"record": {"status": "success", "response": "{}"}},
                execute_step_action=_fake_execute_step_action,
                safe_write_debug=_safe_write_debug,
            )

            outputs: list[str] = []
            for _ in range(10000):
                try:
                    outputs.append(next(gen))
                except StopIteration as e:
                    result = e.value
                    break
            else:
                self.fail("think_parallel_loop 未在预期迭代次数内结束（可能发生死锁）")

        self.assertEqual(str(result.run_status), "failed")
        self.assertTrue(any("agent.think.parallel.deadlock" in m for m in debug_messages))
        self.assertTrue(any("并行调度死锁" in o for o in outputs))

        # 失败必须尝试落盘（status=failed），不受节流影响
        self.assertTrue(any(s == "failed" for s in persist_statuses))
