"""
测试 Think 模式核心模块功能。
"""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch


class TestThinkConfig(unittest.TestCase):
    """测试 Think 模式配置。"""

    def test_get_default_think_config(self):
        """测试默认配置生成。"""
        from backend.src.agent.think import get_default_think_config

        config = get_default_think_config()

        # 验证默认 Planner 数量
        self.assertEqual(config.get_planner_count(), 3)

        # 验证 Planner 配置
        planner_a = config.get_planner("planner_a")
        self.assertIsNotNone(planner_a)
        self.assertEqual(planner_a.role_description, "偏技术实现")

        planner_b = config.get_planner("planner_b")
        self.assertIsNotNone(planner_b)
        self.assertEqual(planner_b.role_description, "偏业务流程")

        # 验证 Executor 配置
        from backend.src.constants import EXECUTOR_ROLE_CODE, EXECUTOR_ROLE_DOC

        code_executor = config.get_executor(EXECUTOR_ROLE_CODE)
        self.assertIsNotNone(code_executor)

        doc_executor = config.get_executor(EXECUTOR_ROLE_DOC)
        self.assertIsNotNone(doc_executor)

    def test_create_think_config_from_dict(self):
        """测试从字典创建配置。"""
        from backend.src.agent.think import create_think_config_from_dict

        config_dict = {
            "agents": {
                "planner_a": "gpt-4",
                "planner_b": "claude-3",
                "executor_code": "gpt-4",
            },
            "voting": {
                "first_vote_select_ratio": 0.5,
            },
            "merge_strategy": "best_of_each",
        }

        config = create_think_config_from_dict(config_dict)

        self.assertEqual(config.get_planner_count(), 2)
        self.assertEqual(config.first_vote_select_ratio, 0.5)
        self.assertEqual(config.merge_strategy, "best_of_each")

        planner_a = config.get_planner("planner_a")
        self.assertIsNotNone(planner_a)
        self.assertEqual(planner_a.model, "gpt-4")

    def test_create_think_config_from_dict_defaults_evaluator_to_base_model(self):
        """验证：未配置 evaluator 时，evaluator_model 默认沿用 base_model。"""
        from backend.src.agent.think import create_think_config_from_dict

        config = create_think_config_from_dict(
            {"agents": {"planner_a": "planner-model"}},
            base_model="base-model",
        )
        self.assertEqual(config.evaluator_model, "base-model")


class TestThinkVoting(unittest.TestCase):
    """测试 Think 模式投票机制。"""

    def test_count_votes(self):
        """测试票数统计。"""
        from backend.src.agent.think import VoteResult, PlanScore, count_votes

        # 创建投票结果
        vote1 = VoteResult(voter_id="planner_a", vote_for=0)
        vote2 = VoteResult(voter_id="planner_b", vote_for=0)
        vote3 = VoteResult(voter_id="planner_c", vote_for=1)

        votes = count_votes([vote1, vote2, vote3], candidate_count=3)

        self.assertEqual(votes[0], 2)  # 方案 0 得 2 票
        self.assertEqual(votes[1], 1)  # 方案 1 得 1 票
        self.assertEqual(votes[2], 0)  # 方案 2 得 0 票

    def test_select_winners(self):
        """测试胜出者选择。"""
        from backend.src.agent.think import VoteResult, select_winners

        vote1 = VoteResult(voter_id="planner_a", vote_for=0)
        vote2 = VoteResult(voter_id="planner_b", vote_for=0)
        vote3 = VoteResult(voter_id="planner_c", vote_for=1)

        # 选 1 个胜出者
        winners = select_winners([vote1, vote2, vote3], candidate_count=3, select_count=1)
        self.assertEqual(len(winners), 1)
        self.assertEqual(winners[0], 0)  # 方案 0 得票最多

        # 选 2 个胜出者
        winners = select_winners([vote1, vote2, vote3], candidate_count=3, select_count=2)
        self.assertEqual(len(winners), 2)
        self.assertIn(0, winners)
        self.assertIn(1, winners)

    def test_select_winners_by_ratio(self):
        """测试按比例选择胜出者。"""
        from backend.src.agent.think import VoteResult, select_winners_by_ratio

        vote1 = VoteResult(voter_id="planner_a", vote_for=0)
        vote2 = VoteResult(voter_id="planner_b", vote_for=1)
        vote3 = VoteResult(voter_id="planner_c", vote_for=2)

        # 选 1/3（3 个中选 1 个）
        winners = select_winners_by_ratio(
            [vote1, vote2, vote3],
            candidate_count=3,
            select_ratio=0.34,
        )
        self.assertEqual(len(winners), 1)

    def test_format_plans_for_voting(self):
        """测试方案格式化。"""
        from backend.src.agent.think import format_plans_for_voting

        plans = [
            {
                "plan": [
                    {"title": "步骤1", "brief": "获取数据", "allow": ["tool_call"]},
                    {"title": "步骤2", "brief": "输出结果", "allow": ["task_output"]},
                ],
                "artifacts": ["output.txt"],
                "rationale": "这是一个简单的方案",
            },
        ]

        formatted = format_plans_for_voting(plans)

        self.assertIn("方案 0", formatted)
        self.assertIn("步骤1", formatted)
        self.assertIn("这是一个简单的方案", formatted)


class TestThinkPlanning(unittest.TestCase):
    """测试 Think 模式规划。"""

    def test_planner_result_from_llm_response(self):
        """测试从 LLM 响应解析规划结果。"""
        from backend.src.agent.think.think_planning import PlannerResult

        response = '''{"plan":[{"title":"获取数据","brief":"抓取","allow":["tool_call"]}],"artifacts":[],"rationale":"测试"}'''

        result = PlannerResult.from_llm_response("planner_a", response)

        self.assertEqual(result.planner_id, "planner_a")
        self.assertEqual(len(result.plan), 1)
        self.assertEqual(result.plan[0]["title"], "获取数据")
        self.assertEqual(result.rationale, "测试")

    def test_planner_result_to_dict(self):
        """测试规划结果转字典。"""
        from backend.src.agent.think.think_planning import PlannerResult

        result = PlannerResult(
            planner_id="planner_a",
            plan=[{"title": "步骤1", "brief": "测试", "allow": ["llm_call"]}],
            artifacts=["test.txt"],
            rationale="测试理由",
        )

        d = result.to_dict()

        self.assertEqual(d["planner_id"], "planner_a")
        self.assertEqual(len(d["plan"]), 1)
        self.assertEqual(d["rationale"], "测试理由")

    def test_think_plan_result_to_planning_result(self):
        """测试 ThinkPlanResult 转换为标准规划结果格式。"""
        from backend.src.agent.think import ThinkPlanResult

        result = ThinkPlanResult(
            plan_titles=["步骤1", "步骤2"],
            plan_briefs=["简介1", "简介2"],
            plan_allows=[["tool_call"], ["task_output"]],
            plan_artifacts=["output.txt"],
            winning_planner_id="planner_a",
        )

        planning_result = result.to_planning_result()

        self.assertEqual(planning_result["plan_titles"], ["步骤1", "步骤2"])
        self.assertEqual(planning_result["mode"], "think")
        self.assertEqual(planning_result["winning_planner_id"], "planner_a")
        self.assertEqual(len(planning_result["plan_items"]), 2)


class TestThinkExecution(unittest.TestCase):
    """测试 Think 模式执行。"""

    def test_infer_executor_assignments(self):
        """测试自动推断 Executor 分配。"""
        from backend.src.agent.think import infer_executor_assignments
        from backend.src.constants import EXECUTOR_ROLE_CODE

        plan_titles = [
            "tool_call:web_fetch 抓取数据",
            "file_write:output.txt 写入结果",
            "task_output 输出结论",
        ]
        plan_allows = [
            ["tool_call"],
            ["file_write"],
            ["task_output"],
        ]
        plan_artifacts = ["output.txt"]

        result = infer_executor_assignments(plan_titles, plan_allows, plan_artifacts)

        self.assertEqual(len(result.assignments), 3)

        # 所有步骤应该分配给 executor_code（默认）
        for assignment in result.assignments:
            self.assertEqual(assignment.executor, EXECUTOR_ROLE_CODE)

    def test_infer_executor_assignments_infers_dependencies_from_file_prefix_when_artifacts_empty(self):
        """回归：plan_artifacts 为空时，仍应基于 title 前缀推断 file_read 对 file_write 的依赖。"""
        from backend.src.agent.think import infer_executor_assignments

        plan_titles = [
            "file_write:out.txt 写入结果",
            "file_read:out.txt 读取结果",
            "task_output 输出结论",
        ]
        plan_allows = [
            ["file_write"],
            ["file_read"],
            ["task_output"],
        ]
        plan_artifacts: list[str] = []

        result = infer_executor_assignments(plan_titles, plan_allows, plan_artifacts)

        self.assertEqual(len(result.assignments), 3)
        self.assertEqual(result.assignments[0].depends_on, [])
        self.assertEqual(result.assignments[1].depends_on, [0])
        self.assertEqual(result.assignments[2].depends_on, [])

    def test_infer_executor_assignments_picks_latest_previous_file_write_for_same_path(self):
        """回归：同一路径多次 file_write 时，后续 file_read 应依赖最近一次写入。"""
        from backend.src.agent.think import infer_executor_assignments

        plan_titles = [
            "file_write:out.txt 写入 v1",
            "file_write:out.txt 写入 v2",
            "file_read:out.txt 读取结果",
        ]
        plan_allows = [["file_write"], ["file_write"], ["file_read"]]

        result = infer_executor_assignments(plan_titles, plan_allows, plan_artifacts=[])

        self.assertEqual(len(result.assignments), 3)
        self.assertEqual(result.assignments[0].depends_on, [])
        self.assertEqual(result.assignments[1].depends_on, [])
        self.assertEqual(result.assignments[2].depends_on, [1])

    def test_infer_executor_assignments_supports_quoted_path_with_spaces(self):
        """回归：带空格的路径应支持用引号包裹（避免 split 截断导致依赖推断失败）。"""
        from backend.src.agent.think import infer_executor_assignments

        plan_titles = [
            'file_write:"dir/my file.txt" 写入结果',
            'file_read:"dir/my file.txt" 读取结果',
        ]
        plan_allows = [["file_write"], ["file_read"]]

        result = infer_executor_assignments(plan_titles, plan_allows, plan_artifacts=[])

        self.assertEqual(len(result.assignments), 2)
        self.assertEqual(result.assignments[0].depends_on, [])
        self.assertEqual(result.assignments[1].depends_on, [0])

    def test_infer_executor_assignments_makes_file_read_depend_on_file_append_latest_modifier(self):
        """回归：file_append 也会修改文件内容，后续 file_read 应依赖 append（而非更早的 write）。"""
        from backend.src.agent.think import infer_executor_assignments

        plan_titles = [
            "file_write:out.txt 写入结果",
            "file_append:out.txt 追加结果",
            "file_read:out.txt 读取结果",
        ]
        plan_allows = [["file_write"], ["file_append"], ["file_read"]]

        result = infer_executor_assignments(plan_titles, plan_allows, plan_artifacts=[])

        self.assertEqual(len(result.assignments), 3)
        self.assertEqual(result.assignments[0].depends_on, [])
        self.assertEqual(result.assignments[1].depends_on, [0])
        self.assertEqual(result.assignments[2].depends_on, [1])

    def test_build_executor_assignments_payload(self):
        """测试可持久化的 executor_assignments 构造。"""
        from backend.src.agent.think.think_execution import build_executor_assignments_payload
        from backend.src.constants import EXECUTOR_ROLE_CODE, EXECUTOR_ROLE_DOC, EXECUTOR_ROLE_TEST

        payload = build_executor_assignments_payload(
            plan_titles=[
                "file_write:README.md 写文档",
                "file_write:main.py 写代码",
                "llm_call:验证结果",
            ],
            plan_allows=[
                ["file_write"],
                ["file_write"],
                ["llm_call"],
            ],
        )

        self.assertEqual(len(payload), 3)
        self.assertEqual(payload[0]["executor"], EXECUTOR_ROLE_DOC)
        self.assertEqual(payload[1]["executor"], EXECUTOR_ROLE_CODE)
        self.assertEqual(payload[2]["executor"], EXECUTOR_ROLE_TEST)

    def test_executor_manager(self):
        """测试 Executor 管理器。"""
        from backend.src.agent.think import (
            ExecutorManager,
            infer_executor_assignments,
            get_default_think_config,
        )

        config = get_default_think_config()
        assignments = infer_executor_assignments(
            plan_titles=["步骤1", "步骤2"],
            plan_allows=[["tool_call"], ["task_output"]],
            plan_artifacts=[],
        )

        manager = ExecutorManager(config, assignments)

        # 检查初始状态
        self.assertFalse(manager.is_all_completed(2))
        self.assertEqual(len(manager.get_all_completed_steps()), 0)

        # 获取执行概要
        summary = manager.get_execution_summary()
        self.assertIn("executor_code", summary)


class TestThinkReflection(unittest.TestCase):
    """测试 Think 模式反思机制。"""

    def test_failure_analysis_from_llm_response(self):
        """测试从 LLM 响应解析失败分析。"""
        from backend.src.agent.think.think_reflection import FailureAnalysis

        response = '''{"root_cause":"依赖缺失","evidence":["错误日志"],"fix_suggestion":"安装依赖","confidence":0.8}'''

        analysis = FailureAnalysis.from_llm_response("planner_a", response)

        self.assertEqual(analysis.planner_id, "planner_a")
        self.assertEqual(analysis.root_cause, "依赖缺失")
        self.assertEqual(len(analysis.evidence), 1)
        self.assertEqual(analysis.fix_suggestion, "安装依赖")
        self.assertEqual(analysis.confidence, 0.8)

    def test_merge_fix_steps_into_plan(self):
        """测试将修复步骤合并到计划。"""
        from backend.src.agent.think import merge_fix_steps_into_plan

        current_step_index = 1
        plan_titles = ["步骤1", "步骤2", "步骤3"]
        plan_briefs = ["简介1", "简介2", "简介3"]
        plan_allows = [["tool_call"], ["llm_call"], ["task_output"]]

        fix_steps = [
            {"title": "修复步骤", "brief": "修复", "allow": ["shell_command"]},
        ]

        new_titles, new_briefs, new_allows = merge_fix_steps_into_plan(
            current_step_index,
            plan_titles,
            plan_briefs,
            plan_allows,
            fix_steps,
        )

        # 应该在步骤2后插入修复步骤
        self.assertEqual(len(new_titles), 4)
        self.assertEqual(new_titles[0], "步骤1")
        self.assertEqual(new_titles[1], "步骤2")
        self.assertEqual(new_titles[2], "修复步骤")
        self.assertEqual(new_titles[3], "步骤3")


class TestThinkConstants(unittest.TestCase):
    """测试 Think 模式常量。"""

    def test_think_constants_exist(self):
        """测试 Think 模式常量是否存在。"""
        from backend.src.constants import (
            THINK_DEFAULT_PLANNER_COUNT,
            THINK_TIEBREAKER_INDEX,
            THINK_FIRST_VOTE_SELECT_RATIO,
            THINK_SECOND_VOTE_SELECT_COUNT,
            THINK_MERGE_STRATEGY_WINNER,
            THINK_MERGE_STRATEGY_BEST,
            STREAM_TAG_THINK,
            STREAM_TAG_PLANNER,
            STREAM_TAG_VOTE,
            STREAM_TAG_REFLECTION,
            THINK_PHASE_INITIAL,
            THINK_PHASE_FIRST_VOTE,
            THINK_PHASE_FINAL_VOTE,
            EXECUTOR_ROLE_CODE,
            EXECUTOR_ROLE_DOC,
            EXECUTOR_ROLE_TEST,
        )

        self.assertEqual(THINK_DEFAULT_PLANNER_COUNT, 3)
        self.assertEqual(THINK_TIEBREAKER_INDEX, 0)
        self.assertGreater(THINK_FIRST_VOTE_SELECT_RATIO, 0)
        self.assertGreater(THINK_SECOND_VOTE_SELECT_COUNT, 0)
        self.assertEqual(THINK_MERGE_STRATEGY_WINNER, "winner_takes_all")
        self.assertEqual(THINK_MERGE_STRATEGY_BEST, "best_of_each")
        self.assertIn("思考", STREAM_TAG_THINK)
        self.assertIn("规划者", STREAM_TAG_PLANNER)
        self.assertIn("投票", STREAM_TAG_VOTE)
        self.assertIn("反思", STREAM_TAG_REFLECTION)
        self.assertEqual(EXECUTOR_ROLE_CODE, "executor_code")
        self.assertEqual(EXECUTOR_ROLE_DOC, "executor_doc")
        self.assertEqual(EXECUTOR_ROLE_TEST, "executor_test")

    def test_think_prompt_templates_exist(self):
        """测试 Think 模式 Prompt 模板是否存在。"""
        from backend.src.constants import (
            THINK_INITIAL_PLANNING_PROMPT_TEMPLATE,
            THINK_VOTE_PROMPT_TEMPLATE,
            THINK_IMPROVE_PROMPT_TEMPLATE,
            THINK_ELABORATE_PROMPT_TEMPLATE,
            THINK_REFLECTION_ANALYZE_PROMPT_TEMPLATE,
            THINK_REFLECTION_VOTE_PROMPT_TEMPLATE,
            THINK_REFLECTION_FIX_PROMPT_TEMPLATE,
            THINK_EXECUTOR_ASSIGN_PROMPT_TEMPLATE,
        )

        # 验证模板包含必要的占位符
        self.assertIn("{message}", THINK_INITIAL_PLANNING_PROMPT_TEMPLATE)
        self.assertIn("{plans}", THINK_VOTE_PROMPT_TEMPLATE)
        self.assertIn("{original_plan}", THINK_IMPROVE_PROMPT_TEMPLATE)
        self.assertIn("{plan}", THINK_ELABORATE_PROMPT_TEMPLATE)
        self.assertIn("{error}", THINK_REFLECTION_ANALYZE_PROMPT_TEMPLATE)
        self.assertIn("{analyses}", THINK_REFLECTION_VOTE_PROMPT_TEMPLATE)
        self.assertIn("{analysis}", THINK_REFLECTION_FIX_PROMPT_TEMPLATE)
        self.assertIn("{plan}", THINK_EXECUTOR_ASSIGN_PROMPT_TEMPLATE)


if __name__ == "__main__":
    unittest.main()
