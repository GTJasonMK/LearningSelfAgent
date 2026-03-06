import unittest

from backend.src.agent.core.plan_structure import PlanStep, PlanStructure
from backend.src.agent.runner.react_error_handler import (
    _is_source_grounding_blocked_after_failure,
    handle_step_failure,
)
from backend.src.constants import RUN_STATUS_FAILED


class TestReactSourceGroundingBlock(unittest.TestCase):
    def test_blocks_when_source_failed_and_next_step_is_not_reacquisition(self):
        plan = PlanStructure(
            steps=[
                PlanStep(id=1, title="tool_call:web_fetch 抓取数据", brief="抓取数据", allow=["tool_call"]),
                PlanStep(id=2, title="llm_call:校验数据", brief="校验数据", allow=["llm_call"]),
                PlanStep(id=3, title="task_output:输出结果", brief="输出结果", allow=["task_output"]),
            ],
            artifacts=[],
        )

        blocked = _is_source_grounding_blocked_after_failure(
            failure_class="source_unavailable",
            current_idx=0,
            plan_struct=plan,
            context={},
        )

        self.assertTrue(blocked)

    def test_allows_when_followup_step_is_another_source_reacquisition(self):
        plan = PlanStructure(
            steps=[
                PlanStep(id=1, title="tool_call:web_fetch 抓取主来源", brief="抓取主来源", allow=["tool_call"]),
                PlanStep(id=2, title="http_request:抓取备用来源", brief="抓取备用来源", allow=["http_request"]),
                PlanStep(id=3, title="json_parse:解析响应", brief="解析响应", allow=["json_parse"]),
            ],
            artifacts=[],
        )

        blocked = _is_source_grounding_blocked_after_failure(
            failure_class="source_unavailable",
            current_idx=0,
            plan_struct=plan,
            context={},
        )

        self.assertFalse(blocked)

    def test_handle_step_failure_fails_fast_when_source_grounding_cannot_be_restored(self):
        plan = PlanStructure(
            steps=[
                PlanStep(id=1, title="tool_call:web_fetch 抓取数据", brief="抓取数据", allow=["tool_call"]),
                PlanStep(id=2, title="llm_call:校验数据", brief="校验数据", allow=["llm_call"]),
                PlanStep(id=3, title="shell_command:生成CSV", brief="生成CSV", allow=["shell_command"]),
            ],
            artifacts=[],
        )
        agent_state = {}
        context = {}
        observations = []
        marked = []

        def _mark_task_step_failed(**kwargs):
            marked.append(kwargs)

        def _safe_write_debug(**kwargs):
            return None

        def _run_replan_and_merge(**kwargs):
            if False:
                yield ""
            return None

        gen = handle_step_failure(
            task_id=1,
            run_id=1,
            step_id=1,
            step_order=1,
            idx=0,
            title="tool_call:web_fetch 抓取数据",
            message="请抓取最近三个月的黄金价格",
            workdir=".",
            model="test-model",
            react_params={},
            tools_hint="",
            skills_hint="",
            memories_hint="",
            graph_hint="",
            action_type="tool_call",
            step_detail="",
            step_error="[code=low_relevance_candidates] web_fetch 候选页弱相关",
            plan_struct=plan,
            agent_state=agent_state,
            context=context,
            observations=observations,
            max_steps_limit=8,
            run_replan_and_merge=_run_replan_and_merge,
            safe_write_debug=_safe_write_debug,
            mark_task_step_failed=_mark_task_step_failed,
            finished_at="2026-03-07T00:00:00Z",
        )

        events = []
        try:
            while True:
                events.append(next(gen))
        except StopIteration as stop:
            result = stop.value

        self.assertEqual(result, (RUN_STATUS_FAILED, None))
        self.assertTrue(marked)
        self.assertEqual(plan.get_step(0).status, "failed")
        self.assertTrue(any("关键步骤失败且重规划未恢复" in str(item) for item in events))


    def test_handle_step_failure_blocks_on_no_structured_data_extracted(self):
        plan = PlanStructure(
            steps=[
                PlanStep(id=1, title="shell_command:执行解析脚本", brief="执行解析脚本", allow=["shell_command"]),
                PlanStep(id=2, title="llm_call:合并校验数据", brief="合并校验数据", allow=["llm_call"]),
            ],
            artifacts=[],
        )

        def _mark_task_step_failed(**kwargs):
            return None

        def _safe_write_debug(**kwargs):
            return None

        def _run_replan_and_merge(**kwargs):
            if False:
                yield ""
            return None

        gen = handle_step_failure(
            task_id=1,
            run_id=1,
            step_id=1,
            step_order=1,
            idx=0,
            title="shell_command:执行解析脚本",
            message="请提取黄金价格数据",
            workdir=".",
            model="test-model",
            react_params={},
            tools_hint="",
            skills_hint="",
            memories_hint="",
            graph_hint="",
            action_type="shell_command",
            step_detail="",
            step_error="[code=no_structured_data_extracted] 命令执行失败: 未从当前样本中解析出可用结构化数据",
            plan_struct=plan,
            agent_state={},
            context={},
            observations=[],
            max_steps_limit=8,
            run_replan_and_merge=_run_replan_and_merge,
            safe_write_debug=_safe_write_debug,
            mark_task_step_failed=_mark_task_step_failed,
            finished_at="2026-03-07T00:00:00Z",
        )

        events = []
        try:
            while True:
                events.append(next(gen))
        except StopIteration as stop:
            result = stop.value

        self.assertEqual(result, (RUN_STATUS_FAILED, None))
        self.assertTrue(any("关键步骤失败且重规划未恢复" in str(item) for item in events))


if __name__ == "__main__":
    unittest.main()
