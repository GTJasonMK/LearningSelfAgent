# -*- coding: utf-8 -*-
"""
Think 模式常量。

包含：
- THINK_*: Think 模式配置参数
- THINK_PHASE_*: 头脑风暴阶段
- EXECUTOR_ROLE_*: Executor 角色
- THINK_MERGE_STRATEGY_*: 方案合并策略
"""

from typing import Final

# Think 模式基础配置
THINK_DEFAULT_PLANNER_COUNT: Final = 3
THINK_TIEBREAKER_INDEX: Final = 0

# 投票配置
THINK_FIRST_VOTE_SELECT_RATIO: Final = 0.34
THINK_SECOND_VOTE_SELECT_COUNT: Final = 2

# 知识检索限制
THINK_PLANNER_MAX_SKILLS: Final = 3
THINK_MERGED_MAX_SKILLS: Final = 6
THINK_PLANNER_MAX_SOLUTIONS: Final = 3
THINK_MERGED_MAX_SOLUTIONS: Final = 5
THINK_MAX_TOOLS: Final = 12
THINK_MERGED_MAX_GRAPH_NODES: Final = 10

# 技能自动生成
THINK_SKILL_AUTOGEN_MAX_SKILLS: Final = 3

# 反思配置
THINK_REFLECTION_MAX_ROUNDS: Final = 2

# 方案合并策略
THINK_MERGE_STRATEGY_WINNER: Final = "winner_takes_all"
THINK_MERGE_STRATEGY_BEST: Final = "best_of_each"
THINK_DEFAULT_MERGE_STRATEGY: Final = THINK_MERGE_STRATEGY_WINNER

# 步骤相似度阈值
THINK_STEP_SIMILARITY_THRESHOLD: Final = 0.7

# 头脑风暴阶段
THINK_PHASE_INITIAL: Final = "initial_planning"
THINK_PHASE_FIRST_VOTE: Final = "first_vote"
THINK_PHASE_IMPROVE: Final = "improve"
THINK_PHASE_SECOND_VOTE: Final = "second_vote"
THINK_PHASE_ELABORATE: Final = "elaborate"
THINK_PHASE_FINAL_VOTE: Final = "final_vote"

# Executor 角色
EXECUTOR_ROLE_CODE: Final = "executor_code"
EXECUTOR_ROLE_DOC: Final = "executor_doc"
EXECUTOR_ROLE_TEST: Final = "executor_test"
