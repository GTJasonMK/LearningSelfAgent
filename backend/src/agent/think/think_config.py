"""
Think 模式配置管理。

定义 Think 模式的配置数据结构，包括 Planner 配置、Executor 配置等。
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from backend.src.constants import (
    DEFAULT_LLM_MODEL,
    THINK_DEFAULT_PLANNER_COUNT,
    THINK_TIEBREAKER_INDEX,
    THINK_FIRST_VOTE_SELECT_RATIO,
    THINK_SECOND_VOTE_SELECT_COUNT,
    THINK_DEFAULT_MERGE_STRATEGY,
    THINK_STEP_SIMILARITY_THRESHOLD,
    THINK_MAX_TOOLS,
    THINK_MERGED_MAX_SKILLS,
    THINK_MERGED_MAX_GRAPH_NODES,
    THINK_REFLECTION_MAX_ROUNDS,
    EXECUTOR_ROLE_CODE,
    EXECUTOR_ROLE_DOC,
    EXECUTOR_ROLE_TEST,
)


@dataclass
class ThinkPlannerConfig:
    """单个 Planner 的配置。"""

    planner_id: str  # 规划者标识（如 planner_a, planner_b）
    model: str = DEFAULT_LLM_MODEL  # 使用的 LLM 模型
    role_description: str = ""  # 角色描述（如"偏技术实现"、"偏业务流程"）
    temperature: float = 0.7  # LLM 温度参数
    max_tokens: int = 4096  # 最大 token 数


@dataclass
class ThinkExecutorConfig:
    """单个 Executor 的配置。"""

    executor_id: str  # 执行者标识（如 executor_code）
    model: str = DEFAULT_LLM_MODEL  # 使用的 LLM 模型
    role: str = EXECUTOR_ROLE_CODE  # 角色类型
    temperature: float = 0.3  # 执行阶段使用较低温度
    max_tokens: int = 4096


@dataclass
class ThinkConfig:
    """Think 模式整体配置。"""

    # Planner 配置
    planners: List[ThinkPlannerConfig] = field(default_factory=list)

    # Executor 配置
    executors: Dict[str, ThinkExecutorConfig] = field(default_factory=dict)

    # 评估者模型
    evaluator_model: str = DEFAULT_LLM_MODEL

    # 投票相关配置
    tiebreaker_index: int = THINK_TIEBREAKER_INDEX
    first_vote_select_ratio: float = THINK_FIRST_VOTE_SELECT_RATIO
    second_vote_select_count: int = THINK_SECOND_VOTE_SELECT_COUNT

    # 合并策略
    merge_strategy: str = THINK_DEFAULT_MERGE_STRATEGY
    step_similarity_threshold: float = THINK_STEP_SIMILARITY_THRESHOLD

    # 检索限制
    max_tools: int = THINK_MAX_TOOLS
    max_skills: int = THINK_MERGED_MAX_SKILLS
    max_graph_nodes: int = THINK_MERGED_MAX_GRAPH_NODES

    # 反思机制
    reflection_max_rounds: int = THINK_REFLECTION_MAX_ROUNDS

    def get_planner_count(self) -> int:
        """获取 Planner 数量。"""
        return len(self.planners)

    def get_planner(self, planner_id: str) -> Optional[ThinkPlannerConfig]:
        """根据 ID 获取 Planner 配置。"""
        for p in self.planners:
            if p.planner_id == planner_id:
                return p
        return None

    def get_executor(self, role: str) -> Optional[ThinkExecutorConfig]:
        """根据角色获取 Executor 配置。"""
        return self.executors.get(role)


def get_default_think_config(
    base_model: str = DEFAULT_LLM_MODEL,
    planner_count: int = THINK_DEFAULT_PLANNER_COUNT,
) -> ThinkConfig:
    """
    获取默认的 Think 模式配置。

    参数:
        base_model: 基础模型名称
        planner_count: Planner 数量

    返回:
        ThinkConfig 实例
    """
    # 创建默认的 Planner 配置
    planner_roles = [
        ("planner_a", "偏技术实现", 0.7),
        ("planner_b", "偏业务流程", 0.8),
        ("planner_c", "偏创新方案", 0.9),
    ]

    planners = []
    for i in range(min(planner_count, len(planner_roles))):
        pid, role_desc, temp = planner_roles[i]
        planners.append(
            ThinkPlannerConfig(
                planner_id=pid,
                model=base_model,
                role_description=role_desc,
                temperature=temp,
            )
        )

    # 创建默认的 Executor 配置
    executors = {
        EXECUTOR_ROLE_CODE: ThinkExecutorConfig(
            executor_id=EXECUTOR_ROLE_CODE,
            model=base_model,
            role=EXECUTOR_ROLE_CODE,
            temperature=0.3,
        ),
        EXECUTOR_ROLE_DOC: ThinkExecutorConfig(
            executor_id=EXECUTOR_ROLE_DOC,
            model=base_model,
            role=EXECUTOR_ROLE_DOC,
            temperature=0.5,
        ),
        EXECUTOR_ROLE_TEST: ThinkExecutorConfig(
            executor_id=EXECUTOR_ROLE_TEST,
            model=base_model,
            role=EXECUTOR_ROLE_TEST,
            temperature=0.2,
        ),
    }

    return ThinkConfig(
        planners=planners,
        executors=executors,
        evaluator_model=base_model,
    )


def create_think_config_from_dict(config_dict: Dict, *, base_model: str = DEFAULT_LLM_MODEL) -> ThinkConfig:
    """
    从字典创建 Think 配置。

    参数:
        config_dict: 配置字典，格式如：
            {
                "agents": {
                    "planner_a": "gpt-4",
                    "planner_b": "claude-3",
                    "planner_c": "gemini-pro",
                    "executor_code": "gpt-4",
                    "executor_doc": "gpt-3.5",
                    "executor_test": "claude-3",
                    "evaluator": "gpt-4"
                },
                "voting": {
                    "first_vote_select_ratio": 0.34,
                    "second_vote_select_count": 2
                },
                "merge_strategy": "winner_takes_all"
            }
        base_model: 当配置中未显式指定某些模型时的兜底模型（例如 evaluator）。

    返回:
        ThinkConfig 实例
    """
    agents = config_dict.get("agents", {})
    voting = config_dict.get("voting", {})

    # 解析 Planner 配置
    planners = []
    planner_roles = {
        "planner_a": "偏技术实现",
        "planner_b": "偏业务流程",
        "planner_c": "偏创新方案",
    }

    for pid, role_desc in planner_roles.items():
        if pid in agents:
            planners.append(
                ThinkPlannerConfig(
                    planner_id=pid,
                    model=agents[pid],
                    role_description=role_desc,
                )
            )

    # 解析 Executor 配置
    executors = {}
    executor_roles = [EXECUTOR_ROLE_CODE, EXECUTOR_ROLE_DOC, EXECUTOR_ROLE_TEST]

    for role in executor_roles:
        if role in agents:
            executors[role] = ThinkExecutorConfig(
                executor_id=role,
                model=agents[role],
                role=role,
            )

    # 解析评估者模型：若未显式配置，则默认沿用 base_model（与 docs/agent 的“think 继承 base model”语义一致）。
    evaluator_model = agents.get("evaluator", base_model) or base_model

    return ThinkConfig(
        planners=planners,
        executors=executors,
        evaluator_model=evaluator_model,
        first_vote_select_ratio=voting.get(
            "first_vote_select_ratio", THINK_FIRST_VOTE_SELECT_RATIO
        ),
        second_vote_select_count=voting.get(
            "second_vote_select_count", THINK_SECOND_VOTE_SELECT_COUNT
        ),
        merge_strategy=config_dict.get("merge_strategy", THINK_DEFAULT_MERGE_STRATEGY),
    )
