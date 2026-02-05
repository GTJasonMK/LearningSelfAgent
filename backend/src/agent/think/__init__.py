"""
Think 模式核心模块。

Think 模式实现多模型协作规划，包括：
- 多 Planner 头脑风暴
- 投票机制
- 多 Executor 分工执行
- 反思机制
"""

from backend.src.agent.think.think_config import (
    ThinkConfig,
    ThinkPlannerConfig,
    ThinkExecutorConfig,
    get_default_think_config,
    create_think_config_from_dict,
)
from backend.src.agent.think.think_voting import (
    VoteResult,
    PlanScore,
    AnalysisScore,
    ReflectionVoteResult,
    count_votes,
    count_total_scores,
    select_winners,
    select_winners_by_ratio,
    format_plans_for_voting,
    build_vote_prompt,
    build_reflection_vote_prompt,
)
from backend.src.agent.think.think_planning import (
    PlannerResult,
    ElaborationResult,
    ThinkPlanResult,
    run_think_planning,
    run_think_planning_sync,
)
from backend.src.agent.think.think_execution import (
    StepAssignment,
    ExecutorAssignmentResult,
    ExecutorContext,
    ExecutorManager,
    infer_executor_assignments,
    assign_executors_with_llm,
    get_executable_steps,
    group_steps_by_executor,
)
from backend.src.agent.think.think_reflection import (
    FailureAnalysis,
    FixStepsResult,
    ReflectionResult,
    run_reflection,
    merge_fix_steps_into_plan,
)

__all__ = [
    # 配置
    "ThinkConfig",
    "ThinkPlannerConfig",
    "ThinkExecutorConfig",
    "get_default_think_config",
    "create_think_config_from_dict",
    # 投票
    "VoteResult",
    "PlanScore",
    "AnalysisScore",
    "ReflectionVoteResult",
    "count_votes",
    "count_total_scores",
    "select_winners",
    "select_winners_by_ratio",
    "format_plans_for_voting",
    "build_vote_prompt",
    "build_reflection_vote_prompt",
    # 规划
    "PlannerResult",
    "ElaborationResult",
    "ThinkPlanResult",
    "run_think_planning",
    "run_think_planning_sync",
    # 执行
    "StepAssignment",
    "ExecutorAssignmentResult",
    "ExecutorContext",
    "ExecutorManager",
    "infer_executor_assignments",
    "assign_executors_with_llm",
    "get_executable_steps",
    "group_steps_by_executor",
    # 反思
    "FailureAnalysis",
    "FixStepsResult",
    "ReflectionResult",
    "run_reflection",
    "merge_fix_steps_into_plan",
]
