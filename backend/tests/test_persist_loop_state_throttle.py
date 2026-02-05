import unittest
from unittest.mock import patch


class TestPersistLoopStateThrottle(unittest.TestCase):
    def setUp(self):
        import backend.src.agent.runner.react_state_manager as sm

        # 清理 throttle 缓存，避免跨测试污染
        sm._PERSIST_THROTTLE_STATE.clear()

    def test_persist_loop_state_throttles_non_critical_updates_but_flushes_on_final_step(self):
        import backend.src.agent.runner.react_state_manager as sm

        calls: list[dict] = []

        def _fake_update_task_run(**kwargs):
            calls.append(dict(kwargs))

        plan_titles = ["a", "b", "c"]
        plan_items = [{"id": 1, "status": "pending"} for _ in plan_titles]
        plan_allows = [["tool_call"] for _ in plan_titles]

        with patch.object(sm, "update_task_run", side_effect=_fake_update_task_run), patch.object(
            sm, "AGENT_REACT_PERSIST_MIN_INTERVAL_SECONDS", 999
        ), patch.object(sm.time, "monotonic", return_value=1.0):
            ok1 = sm.persist_loop_state(
                run_id=1,
                plan_titles=plan_titles,
                plan_items=plan_items,
                plan_allows=plan_allows,
                plan_artifacts=[],
                agent_state={},
                step_order=1,
                observations=[],
                context={},
                where="t1",
            )
            ok2 = sm.persist_loop_state(
                run_id=1,
                plan_titles=plan_titles,
                plan_items=plan_items,
                plan_allows=plan_allows,
                plan_artifacts=[],
                agent_state={},
                step_order=2,
                observations=[],
                context={},
                where="t2",
            )
            # 收尾：step_order >= len(plan_titles)+1 应强制落盘（即使仍在节流窗口内）
            ok3 = sm.persist_loop_state(
                run_id=1,
                plan_titles=plan_titles,
                plan_items=plan_items,
                plan_allows=plan_allows,
                plan_artifacts=[],
                agent_state={},
                step_order=4,
                observations=[],
                context={},
                where="t3",
            )

        self.assertTrue(ok1)
        self.assertTrue(ok2)
        self.assertTrue(ok3)
        # 第 2 次应被节流跳过，因此总写库次数 < 调用次数
        self.assertEqual(len(calls), 2)

    def test_persist_loop_state_does_not_throttle_critical_status(self):
        import backend.src.agent.runner.react_state_manager as sm

        calls: list[dict] = []

        def _fake_update_task_run(**kwargs):
            calls.append(dict(kwargs))

        plan_titles = ["a", "b"]
        plan_items = [{"id": 1, "status": "pending"} for _ in plan_titles]
        plan_allows = [["tool_call"] for _ in plan_titles]

        with patch.object(sm, "update_task_run", side_effect=_fake_update_task_run), patch.object(
            sm, "AGENT_REACT_PERSIST_MIN_INTERVAL_SECONDS", 999
        ), patch.object(sm.time, "monotonic", return_value=1.0):
            sm.persist_loop_state(
                run_id=1,
                plan_titles=plan_titles,
                plan_items=plan_items,
                plan_allows=plan_allows,
                plan_artifacts=[],
                agent_state={},
                step_order=1,
                observations=[],
                context={},
                where="t1",
            )
            sm.persist_loop_state(
                run_id=1,
                plan_titles=plan_titles,
                plan_items=plan_items,
                plan_allows=plan_allows,
                plan_artifacts=[],
                agent_state={},
                step_order=2,
                observations=[],
                context={},
                status=sm.RUN_STATUS_FAILED,
                where="t2",
            )

        self.assertEqual(len(calls), 2)

