"""
Think 模式反思机制。

当执行失败时，多模型协作分析失败原因并生成修复步骤。

反思流程：
1. 各模型发表意见（略答）
2. 第一轮投票
3. 入选者改进发言
4. 第二轮投票
5. 详细发言（详答）
6. 最终投票 -> 生成修复步骤
"""

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from backend.src.constants import (
    THINK_REFLECTION_ANALYZE_PROMPT_TEMPLATE,
    THINK_REFLECTION_VOTE_PROMPT_TEMPLATE,
    THINK_REFLECTION_FIX_PROMPT_TEMPLATE,
    STREAM_TAG_REFLECTION,
    STREAM_TAG_PLANNER,
    STREAM_TAG_VOTE,
    ACTION_TYPE_LLM_CALL,
    ACTION_TYPE_MEMORY_WRITE,
    ACTION_TYPE_TOOL_CALL,
    ACTION_TYPE_SHELL_COMMAND,
    ACTION_TYPE_FILE_WRITE,
    ACTION_TYPE_FILE_READ,
    ACTION_TYPE_HTTP_REQUEST,
    ACTION_TYPE_FILE_APPEND,
    ACTION_TYPE_FILE_LIST,
    ACTION_TYPE_FILE_DELETE,
    ACTION_TYPE_JSON_PARSE,
    ACTION_TYPE_USER_PROMPT,
)
from backend.src.actions.registry import normalize_action_type
from backend.src.agent.json_utils import safe_json_parse
from backend.src.agent.think.think_config import ThinkConfig
from backend.src.agent.think.think_voting import (
    ReflectionVoteResult,
    format_analyses_for_voting,
    build_reflection_vote_prompt,
)


# 允许的 action 类型（不含 task_output）
ALLOWED_FIX_ACTION_TYPES = [
    ACTION_TYPE_LLM_CALL,
    ACTION_TYPE_MEMORY_WRITE,
    ACTION_TYPE_TOOL_CALL,
    ACTION_TYPE_SHELL_COMMAND,
    ACTION_TYPE_FILE_WRITE,
    ACTION_TYPE_FILE_READ,
    ACTION_TYPE_HTTP_REQUEST,
    ACTION_TYPE_FILE_APPEND,
    ACTION_TYPE_FILE_LIST,
    ACTION_TYPE_FILE_DELETE,
    ACTION_TYPE_JSON_PARSE,
    ACTION_TYPE_USER_PROMPT,
]


@dataclass
class FailureAnalysis:
    """失败分析结果。"""

    planner_id: str
    root_cause: str = ""
    evidence: List[str] = field(default_factory=list)
    fix_suggestion: str = ""
    confidence: float = 0.0
    raw_response: str = ""

    def to_dict(self) -> Dict:
        """转换为字典。"""
        return {
            "planner_id": self.planner_id,
            "root_cause": self.root_cause,
            "evidence": self.evidence,
            "fix_suggestion": self.fix_suggestion,
            "confidence": self.confidence,
        }

    @classmethod
    def from_llm_response(cls, planner_id: str, response: str) -> "FailureAnalysis":
        """从 LLM 响应解析。"""
        result = cls(planner_id=planner_id, raw_response=response)

        parsed = safe_json_parse(response)
        if parsed is None:
            return result

        result.root_cause = parsed.get("root_cause", "")
        result.evidence = parsed.get("evidence", [])
        result.fix_suggestion = parsed.get("fix_suggestion", "")
        result.confidence = parsed.get("confidence", 0.0)

        return result


@dataclass
class FixStepsResult:
    """修复步骤生成结果。"""

    planner_id: str
    insert_steps: List[Dict] = field(default_factory=list)
    reason: str = ""
    raw_response: str = ""

    @classmethod
    def from_llm_response(cls, planner_id: str, response: str) -> "FixStepsResult":
        """从 LLM 响应解析。"""
        result = cls(planner_id=planner_id, raw_response=response)

        parsed = safe_json_parse(response)
        if parsed is None:
            return result

        result.insert_steps = parsed.get("insert_steps", [])
        result.reason = parsed.get("reason", "")

        return result


@dataclass
class ReflectionResult:
    """反思机制的最终结果。"""

    winning_analysis: Optional[FailureAnalysis] = None
    fix_steps: List[Dict] = field(default_factory=list)
    all_analyses: List[FailureAnalysis] = field(default_factory=list)
    vote_records: List[Dict] = field(default_factory=list)
    llm_record_ids: List[int] = field(default_factory=list)


def _build_analyze_prompt(
    error: str,
    observations: str,
    plan: str,
    done_steps: str,
) -> str:
    """构建失败分析 Prompt。"""
    return THINK_REFLECTION_ANALYZE_PROMPT_TEMPLATE.format(
        error=error,
        observations=observations,
        plan=plan,
        done_steps=done_steps,
    )


def _build_fix_prompt(
    analysis: Dict,
    plan: str,
    message: str,
    max_steps: int,
) -> str:
    """构建修复步骤生成 Prompt。"""
    action_types_line = ", ".join(ALLOWED_FIX_ACTION_TYPES)

    return THINK_REFLECTION_FIX_PROMPT_TEMPLATE.format(
        analysis=json.dumps(analysis, ensure_ascii=False, indent=2),
        plan=plan,
        message=message,
        max_steps=max_steps,
        action_types_line=action_types_line,
    )


def _count_reflection_votes(
    vote_results: List[ReflectionVoteResult],
    candidate_count: int,
) -> Dict[int, int]:
    """统计反思投票。"""
    votes = {i: 0 for i in range(candidate_count)}

    for result in vote_results:
        analysis_id = result.vote_for
        if 0 <= analysis_id < candidate_count:
            votes[analysis_id] += 1

    return votes


def _select_reflection_winner(
    vote_results: List[ReflectionVoteResult],
    candidate_count: int,
    tiebreaker_index: int = 0,
) -> int:
    """选出反思投票的胜出者。"""
    if candidate_count == 0:
        return 0

    votes = _count_reflection_votes(vote_results, candidate_count)

    # 按得票排序
    sorted_candidates = sorted(
        range(candidate_count),
        key=lambda x: votes[x],
        reverse=True,
    )

    # 处理平票
    if len(sorted_candidates) > 1:
        top_votes = votes[sorted_candidates[0]]
        tied = [c for c in sorted_candidates if votes[c] == top_votes]

        if len(tied) > 1 and vote_results:
            # 使用 tiebreaker 的投票
            tiebreaker_idx = min(tiebreaker_index, len(vote_results) - 1)
            return vote_results[tiebreaker_idx].vote_for

    return sorted_candidates[0]


# LLM 调用回调类型
LLMCallFunc = Callable[[str, str, Dict], Tuple[str, Optional[int]]]


def run_reflection(
    config: ThinkConfig,
    error: str,
    observations: str,
    plan_titles: List[str],
    done_step_indices: List[int],
    message: str,
    llm_call_func: LLMCallFunc,
    yield_progress: Optional[Callable[[str], None]] = None,
    max_fix_steps: int = 3,
) -> ReflectionResult:
    """
    执行反思机制。

    参数:
        config: Think 模式配置
        error: 失败信息
        observations: 最近的执行观测
        plan_titles: 计划步骤标题列表
        done_step_indices: 已完成步骤的索引
        message: 用户目标
        llm_call_func: LLM 调用函数
        yield_progress: 进度输出回调
        max_fix_steps: 最大修复步骤数

    返回:
        ReflectionResult 实例
    """
    result = ReflectionResult()

    def _progress(msg: str):
        if yield_progress:
            yield_progress(msg)

    planners = config.planners
    if not planners:
        _progress(f"{STREAM_TAG_REFLECTION} 错误：没有配置 Planner")
        return result

    # 构建计划和已完成步骤的描述
    plan_desc = "\n".join([f"{i}. {t}" for i, t in enumerate(plan_titles)])
    done_steps_desc = ", ".join([str(i) for i in done_step_indices]) or "（无）"

    # ========== 阶段 1：各模型分析失败原因 ==========
    _progress(f"{STREAM_TAG_REFLECTION} 阶段 1/3：失败原因分析")

    analyses: List[FailureAnalysis] = []

    for planner in planners:
        _progress(f"{STREAM_TAG_PLANNER} {planner.planner_id} 正在分析失败原因...")

        prompt = _build_analyze_prompt(
            error=error,
            observations=observations,
            plan=plan_desc,
            done_steps=done_steps_desc,
        )

        response, record_id = llm_call_func(
            prompt,
            planner.model,
            {"temperature": 0.5},
        )

        analysis = FailureAnalysis.from_llm_response(planner.planner_id, response)
        analyses.append(analysis)

        if record_id:
            result.llm_record_ids.append(record_id)

        _progress(f"{STREAM_TAG_PLANNER} {planner.planner_id} 分析：{analysis.root_cause[:50]}...")

    result.all_analyses = analyses

    # ========== 阶段 2：投票选出最准确的分析 ==========
    _progress(f"{STREAM_TAG_REFLECTION} 阶段 2/3：投票选出最佳分析")

    analyses_for_vote = [a.to_dict() for a in analyses]
    vote_results: List[ReflectionVoteResult] = []

    for planner in planners:
        _progress(f"{STREAM_TAG_VOTE} {planner.planner_id} 正在投票...")

        vote_prompt = build_reflection_vote_prompt(analyses_for_vote)

        response, record_id = llm_call_func(
            vote_prompt,
            planner.model,
            {"temperature": 0.3},
        )

        vote_result = ReflectionVoteResult.from_llm_response(planner.planner_id, response)
        vote_results.append(vote_result)

        if record_id:
            result.llm_record_ids.append(record_id)

    # 选出胜出者
    winner_idx = _select_reflection_winner(
        vote_results,
        len(analyses),
        config.tiebreaker_index,
    )

    result.vote_records.append({
        "phase": "reflection_vote",
        "votes": _count_reflection_votes(vote_results, len(analyses)),
        "winner": winner_idx,
    })

    result.winning_analysis = analyses[winner_idx]

    _progress(f"{STREAM_TAG_VOTE} 投票结果：{result.winning_analysis.planner_id} 的分析被选中")

    # ========== 阶段 3：胜出者生成修复步骤 ==========
    _progress(f"{STREAM_TAG_REFLECTION} 阶段 3/3：生成修复步骤")

    winning_planner = planners[winner_idx]
    _progress(f"{STREAM_TAG_PLANNER} {winning_planner.planner_id} 正在生成修复步骤...")

    fix_prompt = _build_fix_prompt(
        analysis=result.winning_analysis.to_dict(),
        plan=plan_desc,
        message=message,
        max_steps=max_fix_steps,
    )

    response, record_id = llm_call_func(
        fix_prompt,
        winning_planner.model,
        {"temperature": 0.5},
    )

    fix_result = FixStepsResult.from_llm_response(winning_planner.planner_id, response)

    if record_id:
        result.llm_record_ids.append(record_id)

    result.fix_steps = fix_result.insert_steps

    _progress(
        f"{STREAM_TAG_REFLECTION} 反思完成，生成了 {len(result.fix_steps)} 个修复步骤：{fix_result.reason}"
    )

    return result


def merge_fix_steps_into_plan(
    current_step_index: int,
    plan_titles: List[str],
    plan_briefs: List[str],
    plan_allows: List[List[str]],
    fix_steps: List[Dict],
) -> Tuple[List[str], List[str], List[List[str]]]:
    """
    将修复步骤合并到计划中。

    参数:
        current_step_index: 当前失败的步骤索引
        plan_titles: 原计划标题列表
        plan_briefs: 原计划简介列表
        plan_allows: 原计划允许的 action 类型
        fix_steps: 要插入的修复步骤

    返回:
        (new_titles, new_briefs, new_allows) 合并后的计划
    """

    allowed_set = {t for t in (ALLOWED_FIX_ACTION_TYPES or []) if isinstance(t, str) and t}

    def _sanitize_allow(value) -> List[str]:
        if value is None:
            raw_values: List = []
        elif isinstance(value, str):
            raw_values = [value]
        elif isinstance(value, list):
            raw_values = value
        else:
            raw_values = []

        out: List[str] = []
        for item in raw_values:
            normalized = normalize_action_type(str(item or ""))
            if not normalized:
                continue
            if normalized not in allowed_set:
                continue
            if normalized not in out:
                out.append(normalized)
        return out

    def _infer_allow_from_title(title: str) -> List[str]:
        raw = str(title or "").strip()
        if not raw:
            return [ACTION_TYPE_LLM_CALL]
        prefix_map = {
            "file_write": ACTION_TYPE_FILE_WRITE,
            "tool_call": ACTION_TYPE_TOOL_CALL,
            "shell_command": ACTION_TYPE_SHELL_COMMAND,
            "script_run": ACTION_TYPE_SHELL_COMMAND,
            "llm_call": ACTION_TYPE_LLM_CALL,
            "memory_write": ACTION_TYPE_MEMORY_WRITE,
            "file_read": ACTION_TYPE_FILE_READ,
            "file_list": ACTION_TYPE_FILE_LIST,
            "file_append": ACTION_TYPE_FILE_APPEND,
            "file_delete": ACTION_TYPE_FILE_DELETE,
            "http_request": ACTION_TYPE_HTTP_REQUEST,
            "json_parse": ACTION_TYPE_JSON_PARSE,
            "user_prompt": ACTION_TYPE_USER_PROMPT,
        }
        for prefix, action_type in prefix_map.items():
            if raw.startswith(prefix + ":") or raw.startswith(prefix + "："):
                return [action_type] if action_type in allowed_set else [ACTION_TYPE_LLM_CALL]
        return [ACTION_TYPE_LLM_CALL]

    # 在失败步骤后插入修复步骤
    insert_pos = current_step_index + 1

    new_titles = plan_titles[:insert_pos]
    new_briefs = plan_briefs[:insert_pos]
    new_allows = plan_allows[:insert_pos]

    for step in fix_steps:
        if not isinstance(step, dict):
            continue
        title = str(step.get("title") or "修复步骤").strip() or "修复步骤"
        brief = str(step.get("brief") or "修复").strip() or "修复"
        allow = _sanitize_allow(step.get("allow"))
        if not allow:
            allow = _infer_allow_from_title(title)
        new_titles.append(title)
        new_briefs.append(brief)
        new_allows.append(allow)

    # 添加原计划中失败步骤之后的步骤
    new_titles.extend(plan_titles[insert_pos:])
    new_briefs.extend(plan_briefs[insert_pos:])
    new_allows.extend(plan_allows[insert_pos:])

    return new_titles, new_briefs, new_allows
