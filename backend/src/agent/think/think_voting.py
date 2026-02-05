"""
Think 模式投票机制。

实现多模型投票选举，包括：
- 方案评分
- 票数统计
- 胜出者选择
- 平票处理
"""

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from backend.src.constants import (
    THINK_TIEBREAKER_INDEX,
    THINK_VOTE_PROMPT_TEMPLATE,
    THINK_REFLECTION_VOTE_PROMPT_TEMPLATE,
)
from backend.src.agent.json_utils import safe_json_parse


@dataclass
class PlanScore:
    """单个方案的评分详情。"""

    plan_id: int
    feasibility: int = 0  # 可行性
    completeness: int = 0  # 完整性
    efficiency: int = 0  # 效率
    risk: int = 0  # 风险控制
    total: int = 0  # 总分
    comment: str = ""  # 评语

    @classmethod
    def from_dict(cls, data: Dict) -> "PlanScore":
        """从字典创建评分对象。"""
        return cls(
            plan_id=data.get("plan_id", 0),
            feasibility=data.get("feasibility", 0),
            completeness=data.get("completeness", 0),
            efficiency=data.get("efficiency", 0),
            risk=data.get("risk", 0),
            total=data.get("total", 0),
            comment=data.get("comment", ""),
        )


@dataclass
class VoteResult:
    """单个投票者的投票结果。"""

    voter_id: str  # 投票者标识
    scores: List[PlanScore] = field(default_factory=list)  # 对各方案的评分
    vote_for: int = 0  # 投票给哪个方案 ID
    raw_response: str = ""  # LLM 原始响应

    @classmethod
    def from_llm_response(cls, voter_id: str, response: str) -> "VoteResult":
        """从 LLM 响应解析投票结果。"""
        result = cls(voter_id=voter_id, raw_response=response)

        parsed = safe_json_parse(response)
        if parsed is None:
            return result

        # 解析评分
        scores_data = parsed.get("scores", [])
        result.scores = [PlanScore.from_dict(s) for s in scores_data]

        # 解析投票选择
        result.vote_for = parsed.get("vote_for", 0)

        return result


@dataclass
class AnalysisScore:
    """反思分析的评分详情。"""

    analysis_id: int
    accuracy: int = 0  # 准确性
    actionability: int = 0  # 可操作性
    total: int = 0
    comment: str = ""

    @classmethod
    def from_dict(cls, data: Dict) -> "AnalysisScore":
        """从字典创建评分对象。"""
        return cls(
            analysis_id=data.get("analysis_id", 0),
            accuracy=data.get("accuracy", 0),
            actionability=data.get("actionability", 0),
            total=data.get("total", 0),
            comment=data.get("comment", ""),
        )


@dataclass
class ReflectionVoteResult:
    """反思阶段的投票结果。"""

    voter_id: str
    scores: List[AnalysisScore] = field(default_factory=list)
    vote_for: int = 0
    raw_response: str = ""

    @classmethod
    def from_llm_response(cls, voter_id: str, response: str) -> "ReflectionVoteResult":
        """从 LLM 响应解析投票结果。"""
        result = cls(voter_id=voter_id, raw_response=response)

        parsed = safe_json_parse(response)
        if parsed is None:
            return result

        scores_data = parsed.get("scores", [])
        result.scores = [AnalysisScore.from_dict(s) for s in scores_data]
        result.vote_for = parsed.get("vote_for", 0)

        return result


def count_votes(
    vote_results: List[VoteResult],
    candidate_count: int,
) -> Dict[int, int]:
    """
    统计各方案的得票数。

    参数:
        vote_results: 所有投票者的投票结果
        candidate_count: 候选方案数量

    返回:
        Dict[plan_id, vote_count] 各方案的得票数
    """
    votes = {i: 0 for i in range(candidate_count)}

    for result in vote_results:
        plan_id = result.vote_for
        if 0 <= plan_id < candidate_count:
            votes[plan_id] += 1

    return votes


def count_total_scores(
    vote_results: List[VoteResult],
    candidate_count: int,
) -> Dict[int, int]:
    """
    统计各方案的总评分。

    参数:
        vote_results: 所有投票者的投票结果
        candidate_count: 候选方案数量

    返回:
        Dict[plan_id, total_score] 各方案的总评分
    """
    scores = {i: 0 for i in range(candidate_count)}

    for result in vote_results:
        for score in result.scores:
            if 0 <= score.plan_id < candidate_count:
                scores[score.plan_id] += score.total

    return scores


def select_winners(
    vote_results: List[VoteResult],
    candidate_count: int,
    select_count: int = 1,
    tiebreaker_index: int = THINK_TIEBREAKER_INDEX,
) -> List[int]:
    """
    选出胜出的方案。

    参数:
        vote_results: 所有投票者的投票结果
        candidate_count: 候选方案数量
        select_count: 要选出的方案数量
        tiebreaker_index: 平票时由第几个投票者决定

    返回:
        胜出方案的 ID 列表（按得票从高到低排序）
    """
    if candidate_count == 0:
        return []

    # 统计得票
    votes = count_votes(vote_results, candidate_count)

    # 按得票数排序（得票相同时按总评分排序）
    scores = count_total_scores(vote_results, candidate_count)

    sorted_candidates = sorted(
        range(candidate_count),
        key=lambda x: (votes[x], scores[x]),
        reverse=True,
    )

    # 处理平票情况
    if select_count < len(sorted_candidates):
        # 检查是否有平票需要处理
        cutoff_votes = votes[sorted_candidates[select_count - 1]]
        tied_at_cutoff = [
            c for c in sorted_candidates[select_count - 1 :]
            if votes[c] == cutoff_votes
        ]

        if len(tied_at_cutoff) > 1 and vote_results:
            # 使用 tiebreaker 的投票偏好来决定
            tiebreaker_idx = min(tiebreaker_index, len(vote_results) - 1)
            tiebreaker_result = vote_results[tiebreaker_idx]

            # 根据 tiebreaker 的评分来排序平票候选
            tiebreaker_scores = {s.plan_id: s.total for s in tiebreaker_result.scores}
            tied_at_cutoff.sort(
                key=lambda x: tiebreaker_scores.get(x, 0),
                reverse=True,
            )

            # 重新构建排序结果
            non_tied = [c for c in sorted_candidates if c not in tied_at_cutoff]
            sorted_candidates = (
                non_tied[:select_count - 1] + tied_at_cutoff + non_tied[select_count - 1:]
            )

    return sorted_candidates[:select_count]


def select_winners_by_ratio(
    vote_results: List[VoteResult],
    candidate_count: int,
    select_ratio: float = 0.34,
    tiebreaker_index: int = THINK_TIEBREAKER_INDEX,
) -> List[int]:
    """
    按比例选出胜出的方案。

    参数:
        vote_results: 所有投票者的投票结果
        candidate_count: 候选方案数量
        select_ratio: 选出比例（如 0.34 表示选 1/3）
        tiebreaker_index: 平票时由第几个投票者决定

    返回:
        胜出方案的 ID 列表
    """
    select_count = max(1, int(candidate_count * select_ratio))
    return select_winners(
        vote_results,
        candidate_count,
        select_count,
        tiebreaker_index,
    )


def format_plans_for_voting(
    plans: List[Dict],
    include_rationale: bool = True,
) -> str:
    """
    格式化方案列表用于投票 Prompt。

    参数:
        plans: 方案列表，每个方案包含 plan、artifacts、rationale 等
        include_rationale: 是否包含设计理由

    返回:
        格式化的方案文本
    """
    lines = []

    for i, plan_data in enumerate(plans):
        lines.append(f"### 方案 {i} (plan_id={i})")

        plan_steps = plan_data.get("plan", [])
        artifacts = plan_data.get("artifacts", [])
        rationale = plan_data.get("rationale", "")

        # 输出步骤
        lines.append("步骤:")
        for j, step in enumerate(plan_steps):
            title = step.get("title", f"步骤{j+1}")
            brief = step.get("brief", "")
            allow = step.get("allow", [])
            lines.append(f"  {j+1}. {title} ({brief}) [allow: {', '.join(allow)}]")

        # 输出 artifacts
        if artifacts:
            lines.append(f"artifacts: {', '.join(artifacts)}")

        # 输出设计理由
        if include_rationale and rationale:
            lines.append(f"设计理由: {rationale}")

        lines.append("")

    return "\n".join(lines)


def format_analyses_for_voting(analyses: List[Dict]) -> str:
    """
    格式化反思分析列表用于投票 Prompt。

    参数:
        analyses: 分析列表

    返回:
        格式化的分析文本
    """
    lines = []

    for i, analysis in enumerate(analyses):
        lines.append(f"### 分析 {i} (analysis_id={i})")
        lines.append(f"根因: {analysis.get('root_cause', '未知')}")
        lines.append(f"证据: {', '.join(analysis.get('evidence', []))}")
        lines.append(f"修复建议: {analysis.get('fix_suggestion', '无')}")
        lines.append(f"置信度: {analysis.get('confidence', 0)}")
        lines.append("")

    return "\n".join(lines)


def build_vote_prompt(
    message: str,
    plans: List[Dict],
    can_vote_self: bool = True,
) -> str:
    """
    构建投票 Prompt。

    参数:
        message: 用户目标
        plans: 候选方案列表
        can_vote_self: 是否可以投票给自己的方案

    返回:
        格式化的 Prompt
    """
    self_vote_rule = (
        "你可以投票给任何方案（包括自己的）"
        if can_vote_self
        else "你不能投票给自己的方案"
    )

    plans_text = format_plans_for_voting(plans)

    return THINK_VOTE_PROMPT_TEMPLATE.format(
        message=message,
        plans=plans_text,
        self_vote_rule=self_vote_rule,
    )


def build_reflection_vote_prompt(analyses: List[Dict]) -> str:
    """
    构建反思投票 Prompt。

    参数:
        analyses: 候选分析列表

    返回:
        格式化的 Prompt
    """
    analyses_text = format_analyses_for_voting(analyses)

    return THINK_REFLECTION_VOTE_PROMPT_TEMPLATE.format(
        analyses=analyses_text,
    )
