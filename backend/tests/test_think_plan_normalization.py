"""
测试 Think 规划输出的执行前归一化（防止 Planner 输出不规范导致执行阶段跑偏/失败）。
"""

import json
import unittest


class TestThinkPlanNormalization(unittest.TestCase):
    """测试 normalize_plan_data_for_execution 的确定性兜底行为。"""

    def test_normalize_adds_task_output_at_end(self):
        """当 Planner 未提供 task_output 步骤时，应自动补齐到最后一步。"""
        from backend.src.agent.think.think_planning import normalize_plan_data_for_execution
        from backend.src.constants import ACTION_TYPE_TASK_OUTPUT, ACTION_TYPE_FILE_WRITE

        plan_data = {
            "plan": [
                {"title": "file_write:out.txt 写入结果"},
                {"title": "验证结果"},
            ],
            "artifacts": ["out.txt"],
        }

        titles, briefs, allows, artifacts, err = normalize_plan_data_for_execution(plan_data, max_steps=10)
        self.assertIsNone(err)
        self.assertTrue(titles)
        self.assertEqual(artifacts, ["out.txt"])
        self.assertEqual(allows[0], [ACTION_TYPE_FILE_WRITE])
        self.assertIn(ACTION_TYPE_TASK_OUTPUT, set(allows[-1] or []))

    def test_normalize_moves_task_output_to_last(self):
        """当 task_output 不在最后一步时，应移动到最后一步。"""
        from backend.src.agent.think.think_planning import normalize_plan_data_for_execution
        from backend.src.constants import ACTION_TYPE_TASK_OUTPUT

        plan_data = {
            "plan": [
                {"title": "task_output 输出结论"},
                {"title": "file_write:a.txt 写文件"},
            ],
            "artifacts": ["a.txt"],
        }

        titles, briefs, allows, artifacts, err = normalize_plan_data_for_execution(plan_data, max_steps=10)
        self.assertIsNone(err)
        self.assertTrue(titles)
        self.assertIn(ACTION_TYPE_TASK_OUTPUT, set(allows[-1] or []))
        self.assertTrue(str(titles[-1]).startswith("task_output"))


class _SequenceLlmStub:
    """
    通过“调用序列”返回固定响应，避免真实 LLM 调用。

    目的：验证 run_think_planning_sync 会对最终胜出方案执行 normalize_plan_data_for_execution，
    而不是直接信任 Planner 输出的 allow/task_output。
    """

    def __init__(self):
        self.calls = 0

        self.plan_response = {
            "plan": [
                {"title": "file_write:docs/out.md 写文档", "brief": ""},
                {"title": "shell_command:python -m compileall backend/src -q", "brief": ""},
            ],
            "artifacts": ["docs/out.md"],
            "rationale": "测试规划",
        }

        self.vote_response_round_3 = {
            "scores": [
                {"plan_id": 0, "total": 100},
                {"plan_id": 1, "total": 0},
                {"plan_id": 2, "total": 0},
            ],
            "vote_for": 0,
        }

        self.vote_response_round_2 = {
            "scores": [
                {"plan_id": 0, "total": 100},
                {"plan_id": 1, "total": 0},
            ],
            "vote_for": 0,
        }

        self.elaborate_response = {
            "step_rationales": [],
            "dependencies": [],
            "overall_confidence": 0.9,
            "key_assumptions": [],
        }

    def __call__(self, prompt: str, model: str, parameters: dict):
        self.calls += 1

        # 1..3 初始规划（3 个 Planner）
        if 1 <= self.calls <= 3:
            return json.dumps(self.plan_response, ensure_ascii=False), None

        # 4..6 第一轮投票（3 个 Planner，候选 3 个）
        if 4 <= self.calls <= 6:
            return json.dumps(self.vote_response_round_3, ensure_ascii=False), None

        # 7..8 改进阶段（2 个未入选 Planner）
        if 7 <= self.calls <= 8:
            return json.dumps(self.plan_response, ensure_ascii=False), None

        # 9..11 第二轮投票（3 个 Planner，候选 3 个）
        if 9 <= self.calls <= 11:
            return json.dumps(self.vote_response_round_3, ensure_ascii=False), None

        # 12..13 详细阐述（2 个入选方案）
        if 12 <= self.calls <= 13:
            return json.dumps(self.elaborate_response, ensure_ascii=False), None

        # 14..16 最终投票（3 个 Planner，候选 2 个）
        if 14 <= self.calls <= 16:
            return json.dumps(self.vote_response_round_2, ensure_ascii=False), None

        raise AssertionError(f"llm_call 次数超出预期：{self.calls}")


class TestRunThinkPlanningSyncNormalization(unittest.TestCase):
    """验证 run_think_planning_sync 会对最终胜出方案做执行前归一化。"""

    def test_run_think_planning_sync_normalizes_final_plan(self):
        from backend.src.agent.think import get_default_think_config
        from backend.src.agent.think.think_planning import run_think_planning_sync
        from backend.src.constants import ACTION_TYPE_TASK_OUTPUT

        config = get_default_think_config(base_model="test-model", planner_count=3)
        llm_stub = _SequenceLlmStub()

        result = run_think_planning_sync(
            config=config,
            message="测试任务",
            workdir=".",
            graph_hint="(无)",
            skills_hint="(无)",
            solutions_hint="(无)",
            tools_hint="(无)",
            max_steps=10,
            llm_call_func=llm_stub,
            yield_progress=None,
            planner_hints=None,
        )

        self.assertTrue(result.plan_titles, "规划结果不应为空")
        self.assertTrue(result.plan_allows, "allow 列表不应为空")
        self.assertTrue(all(isinstance(a, list) and a for a in result.plan_allows), "每步 allow 都应非空")
        self.assertIn(ACTION_TYPE_TASK_OUTPUT, set(result.plan_allows[-1] or []), "最后一步必须是 task_output")


if __name__ == "__main__":
    unittest.main()

