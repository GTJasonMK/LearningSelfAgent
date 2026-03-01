"""
Think 模式多模型规划（头脑风暴流程）。

实现六阶段头脑风暴流程：
1. 初始规划生成（并行）
2. 第一轮投票
3. 观点改进
4. 第二轮投票
5. 详细阐述
6. 最终投票
"""

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple

from backend.src.actions.registry import list_action_types, normalize_action_type
from backend.src.agent.plan_utils import (
    _fallback_brief_from_title,
    drop_non_artifact_file_write_steps,
    repair_plan_artifacts_with_file_write_steps,
)
from backend.src.constants import (
    THINK_INITIAL_PLANNING_PROMPT_TEMPLATE,
    THINK_VOTE_PROMPT_TEMPLATE,
    THINK_IMPROVE_PROMPT_TEMPLATE,
    THINK_ELABORATE_PROMPT_TEMPLATE,
    THINK_PHASE_INITIAL,
    THINK_PHASE_FIRST_VOTE,
    THINK_PHASE_IMPROVE,
    THINK_PHASE_SECOND_VOTE,
    THINK_PHASE_ELABORATE,
    THINK_PHASE_FINAL_VOTE,
    STREAM_TAG_THINK,
    STREAM_TAG_PLANNER,
    STREAM_TAG_VOTE,
    ACTION_TYPE_LLM_CALL,
    ACTION_TYPE_MEMORY_WRITE,
    ACTION_TYPE_TASK_OUTPUT,
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
from backend.src.agent.json_utils import safe_json_parse
from backend.src.agent.think.think_config import ThinkConfig, ThinkPlannerConfig
from backend.src.agent.think.think_voting import (
    VoteResult,
    count_votes,
    select_winners,
    select_winners_by_ratio,
    format_plans_for_voting,
    build_vote_prompt,
)


# 允许的 action 类型列表（与 actions.registry 保持单一来源，避免静态漂移）。
ALLOWED_ACTION_TYPES = list_action_types()


def normalize_plan_data_for_execution(
    plan_data: Dict[str, Any],
    *,
    max_steps: int,
) -> Tuple[List[str], List[str], List[List[str]], List[str], Optional[str]]:
    """
    将 Planner 输出的 plan 数据归一化为“可执行计划”。

    目的：
    - 避免 Planner 输出缺字段/allow 为空导致执行阶段失控或直接失败；
    - 确保 task_output 存在且位于最后一步（与文档约束一致）；
    - 保持 deterministic：尽量用规则兜底，而不是再次调用 LLM。
    """
    if not isinstance(plan_data, dict):
        return [], [], [], [], "plan_data 不是对象"

    try:
        max_steps_value = int(max_steps)
    except Exception:
        max_steps_value = 0
    if max_steps_value <= 0:
        max_steps_value = 30

    allowed_set = {t for t in ALLOWED_ACTION_TYPES if isinstance(t, str) and t}

    def _sanitize_allow_list(raw) -> List[str]:
        if raw is None:
            return []
        if isinstance(raw, str):
            values = [raw]
        elif isinstance(raw, list):
            values = raw
        else:
            return []
        out: List[str] = []
        for v in values:
            normalized = normalize_action_type(str(v or ""))
            if normalized and normalized in allowed_set and normalized not in out:
                out.append(normalized)
        return out

    def _infer_allow_from_title(title: str) -> List[str]:
        raw = str(title or "").strip()
        if not raw:
            return []
        # 约定：title 前缀指明动作类型（file_write: / tool_call: / shell_command: ...）
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
            "task_output": ACTION_TYPE_TASK_OUTPUT,
            "user_prompt": ACTION_TYPE_USER_PROMPT,
        }
        for prefix, action_type in prefix_map.items():
            if raw.startswith(prefix + ":") or raw.startswith(prefix + "："):
                return [action_type]
        # 启发式：明显的“输出结论”语义
        if "输出" in raw or "结论" in raw or "总结" in raw:
            return [ACTION_TYPE_TASK_OUTPUT]
        # 启发式：验证/测试更倾向于可执行动作
        if any(k in raw.lower() for k in ("验证", "测试", "check", "test", "verify", "validate")):
            return [ACTION_TYPE_SHELL_COMMAND]
        # 默认兜底：llm_call（可用于解释/归纳/生成文本）
        return [ACTION_TYPE_LLM_CALL]

    # 读取 steps
    items_raw = plan_data.get("plan")
    if not isinstance(items_raw, list):
        items_raw = plan_data.get("steps")
    if not isinstance(items_raw, list):
        return [], [], [], [], "plan/steps 不是列表"

    steps: List[Dict[str, Any]] = []
    for item in items_raw:
        if isinstance(item, str):
            title = str(item or "").strip()
            if not title:
                continue
            allow_list = _infer_allow_from_title(title)
            if not allow_list:
                allow_list = [ACTION_TYPE_LLM_CALL]
            steps.append(
                {
                    "title": title,
                    "brief": _fallback_brief_from_title(title),
                    "allow": allow_list,
                }
            )
            continue

        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        if not title:
            continue
        brief = str(item.get("brief") or item.get("short") or item.get("summary") or "").strip()
        allow_raw = item.get("allow")
        if allow_raw is None:
            allow_raw = item.get("allowed") or item.get("allowed_actions")
        allow_list = _sanitize_allow_list(allow_raw)
        if not allow_list:
            allow_list = _infer_allow_from_title(title)
        if not allow_list:
            allow_list = [ACTION_TYPE_LLM_CALL]
        steps.append(
            {
                "title": title,
                "brief": brief or _fallback_brief_from_title(title),
                "allow": allow_list,
            }
        )

    # 截断到 max_steps
    if len(steps) > max_steps_value:
        steps = steps[:max_steps_value]

    # 产物
    artifacts: List[str] = []
    raw_artifacts = plan_data.get("artifacts")
    if isinstance(raw_artifacts, list):
        for a in raw_artifacts:
            rel = str(a or "").strip()
            if rel and rel not in artifacts:
                artifacts.append(rel)

    # 确保 task_output 存在且为最后一步
    output_indices = [
        i
        for i, step in enumerate(steps)
        if ACTION_TYPE_TASK_OUTPUT in set(step.get("allow") or [])
    ]
    if output_indices:
        output_idx = output_indices[-1]
        if output_idx != len(steps) - 1:
            step = steps.pop(output_idx)
            steps.append(step)
    else:
        output_step = {
            "title": "task_output 输出最终结果",
            "brief": "输出",
            "allow": [ACTION_TYPE_TASK_OUTPUT],
        }
        if len(steps) < max_steps_value:
            steps.append(output_step)
        elif steps:
            steps[-1] = output_step
        else:
            steps = [output_step]

    titles: List[str] = []
    briefs: List[str] = []
    allows: List[List[str]] = []
    for step in steps:
        title = str(step.get("title") or "").strip()
        if not title:
            continue
        brief = str(step.get("brief") or "").strip() or _fallback_brief_from_title(title)
        allow_list = step.get("allow") if isinstance(step.get("allow"), list) else []
        allow_sanitized: List[str] = []
        for a in allow_list:
            normalized = normalize_action_type(str(a or ""))
            if normalized and normalized in allowed_set and normalized not in allow_sanitized:
                allow_sanitized.append(normalized)
        if not allow_sanitized:
            allow_sanitized = _infer_allow_from_title(title) or [ACTION_TYPE_LLM_CALL]
        titles.append(title)
        briefs.append(brief)
        allows.append(allow_sanitized)

    if not titles:
        return [], [], [], artifacts, "计划为空"

    # artifacts/file_write 对齐：与 do 模式保持一致，减少执行阶段被 artifacts 门闩打断/反复 replan。
    if artifacts:
        (
            repaired_titles,
            repaired_briefs,
            repaired_allows,
            repaired_artifacts,
            repair_err,
            _patched_count,
        ) = repair_plan_artifacts_with_file_write_steps(
            titles=list(titles),
            briefs=list(briefs),
            allows=list(allows),
            artifacts=list(artifacts),
            max_steps=max_steps_value,
        )
        if repair_err:
            return [], [], [], [], f"artifacts/file_write 不一致：{repair_err}"

        # 移除无效 file_write 步骤（仅保留 artifacts 文件与实验目录写入）。
        repaired_titles, repaired_briefs, repaired_allows, _removed = drop_non_artifact_file_write_steps(
            titles=repaired_titles,
            briefs=repaired_briefs,
            allows=repaired_allows,
            artifacts=repaired_artifacts,
        )

        titles = repaired_titles
        briefs = repaired_briefs
        allows = repaired_allows
        artifacts = repaired_artifacts

        # artifacts 任务：在 task_output 之前必须出现一次“验证步骤”（shell/tool + 标题含关键词）。
        # 这样可以满足 artifacts 门闩对“验证成功”的要求，避免执行到输出阶段才触发 replan。
        output_idx = len(titles) - 1  # normalize 后保证 task_output 为最后一步
        keywords = ("验证", "校验", "检查", "自测", "verify", "validate", "check", "test")

        def _is_validation_step(idx: int) -> bool:
            if idx < 0 or idx >= len(titles):
                return False
            allow_set = set(allows[idx] or [])
            if ACTION_TYPE_SHELL_COMMAND not in allow_set and ACTION_TYPE_TOOL_CALL not in allow_set:
                return False
            title_text = str(titles[idx] or "")
            return any(k in title_text for k in keywords)

        has_validation = any(_is_validation_step(i) for i in range(max(0, output_idx)))
        if not has_validation and max_steps_value and len(titles) < int(max_steps_value):
            titles.insert(output_idx, "shell_command:验证产物是否已生成")
            briefs.insert(output_idx, "验证")
            allows.insert(output_idx, [ACTION_TYPE_SHELL_COMMAND])

    return titles, briefs, allows, artifacts, None


@dataclass
class PlannerResult:
    """单个 Planner 的规划结果。"""

    planner_id: str
    plan: List[Dict] = field(default_factory=list)  # 步骤列表
    artifacts: List[str] = field(default_factory=list)
    rationale: str = ""
    improvements: str = ""  # 改进说明（第二轮时）
    raw_response: str = ""
    llm_record_id: Optional[int] = None

    def to_dict(self) -> Dict:
        """转换为字典。"""
        return {
            "planner_id": self.planner_id,
            "plan": self.plan,
            "artifacts": self.artifacts,
            "rationale": self.rationale,
            "improvements": self.improvements,
        }

    @classmethod
    def from_llm_response(
        cls,
        planner_id: str,
        response: str,
        llm_record_id: Optional[int] = None,
    ) -> "PlannerResult":
        """从 LLM 响应解析规划结果。"""
        result = cls(
            planner_id=planner_id,
            raw_response=response,
            llm_record_id=llm_record_id,
        )

        parsed = safe_json_parse(response)
        if parsed is None:
            return result

        result.plan = parsed.get("plan", [])
        result.artifacts = parsed.get("artifacts", [])
        result.rationale = parsed.get("rationale", "")
        result.improvements = parsed.get("improvements", "")

        return result


@dataclass
class ElaborationResult:
    """详细阐述结果。"""

    planner_id: str
    step_rationales: List[Dict] = field(default_factory=list)
    dependencies: List[Dict] = field(default_factory=list)
    overall_confidence: float = 0.0
    key_assumptions: List[str] = field(default_factory=list)
    raw_response: str = ""

    @classmethod
    def from_llm_response(cls, planner_id: str, response: str) -> "ElaborationResult":
        """从 LLM 响应解析。"""
        result = cls(planner_id=planner_id, raw_response=response)

        parsed = safe_json_parse(response)
        if parsed is None:
            return result

        result.step_rationales = parsed.get("step_rationales", [])
        result.dependencies = parsed.get("dependencies", [])
        result.overall_confidence = parsed.get("overall_confidence", 0.0)
        result.key_assumptions = parsed.get("key_assumptions", [])

        return result


@dataclass
class ThinkPlanResult:
    """Think 模式规划的最终结果。"""

    # 最终采用的方案
    plan_titles: List[str] = field(default_factory=list)
    plan_briefs: List[str] = field(default_factory=list)
    plan_allows: List[List[str]] = field(default_factory=list)
    plan_artifacts: List[str] = field(default_factory=list)

    # 最终方案的详细信息
    winning_planner_id: str = ""
    winning_plan_data: Dict = field(default_factory=dict)
    elaboration: Optional[ElaborationResult] = None

    # 落选方案（供参考）
    alternative_plans: List[Dict] = field(default_factory=list)

    # 投票记录
    vote_records: List[Dict] = field(default_factory=list)

    # LLM 调用记录
    llm_record_ids: List[int] = field(default_factory=list)

    def to_planning_result(self) -> Dict:
        """转换为与 do 模式兼容的规划结果格式。"""
        plan_items = []
        for i, title in enumerate(self.plan_titles):
            brief = self.plan_briefs[i] if i < len(self.plan_briefs) else ""
            allow = self.plan_allows[i] if i < len(self.plan_allows) else []
            plan_items.append({
                "title": title,
                "brief": brief,
                "allow": allow,
                "status": "pending",
            })

        return {
            "plan_titles": self.plan_titles,
            "plan_briefs": self.plan_briefs,
            "plan_allows": self.plan_allows,
            "plan_artifacts": self.plan_artifacts,
            "plan_items": plan_items,
            "plan_llm_id": self.llm_record_ids[-1] if self.llm_record_ids else None,
            # Think 模式特有字段
            "mode": "think",
            "winning_planner_id": self.winning_planner_id,
            "alternative_plans": self.alternative_plans,
            "vote_records": self.vote_records,
            # docs/agent 命名对齐：提供别名字段，避免上下游约定漂移
            "plan_alternatives": self.alternative_plans,
            "plan_votes": self.vote_records,
        }


# LLM 调用回调类型
LLMCallFunc = Callable[[str, str, Dict], Tuple[str, Optional[int]]]


def _build_initial_planning_prompt(
    message: str,
    workdir: str,
    planner_config: ThinkPlannerConfig,
    graph_hint: str,
    skills_hint: str,
    solutions_hint: str,
    tools_hint: str,
    max_steps: int,
) -> str:
    """构建初始规划 Prompt。"""
    action_types_line = ", ".join(ALLOWED_ACTION_TYPES)

    return THINK_INITIAL_PLANNING_PROMPT_TEMPLATE.format(
        message=message,
        workdir=workdir,
        planner_role=planner_config.role_description,
        graph=graph_hint or "（无）",
        skills=skills_hint or "（无）",
        solutions=solutions_hint or "（无）",
        tools=tools_hint or "（无）",
        max_steps=max_steps,
        action_types_line=action_types_line,
    )


def _build_improve_prompt(
    message: str,
    workdir: str,
    original_plan: Dict,
    winning_plan: Dict,
) -> str:
    """构建方案改进 Prompt。"""
    return THINK_IMPROVE_PROMPT_TEMPLATE.format(
        message=message,
        workdir=workdir,
        original_plan=json.dumps(original_plan, ensure_ascii=False, indent=2),
        winning_plan=json.dumps(winning_plan, ensure_ascii=False, indent=2),
    )


def _build_elaborate_prompt(message: str, plan: Dict) -> str:
    """构建详细阐述 Prompt。"""
    return THINK_ELABORATE_PROMPT_TEMPLATE.format(
        message=message,
        plan=json.dumps(plan, ensure_ascii=False, indent=2),
    )


def run_think_planning_sync(
    config: ThinkConfig,
    message: str,
    workdir: str,
    graph_hint: str,
    skills_hint: str,
    solutions_hint: str,
    tools_hint: str,
    max_steps: int,
    llm_call_func: LLMCallFunc,
    yield_progress: Optional[Callable[[str], None]] = None,
    planner_hints: Optional[Dict[str, Dict[str, str]]] = None,
) -> ThinkPlanResult:
    """
    同步执行 Think 模式规划（六阶段头脑风暴）。

    参数:
        config: Think 模式配置
        message: 用户目标
        workdir: 工作目录
        graph_hint: 图谱检索结果
        skills_hint: 技能检索结果
        tools_hint: 工具列表
        max_steps: 最大步骤数
        llm_call_func: LLM 调用函数，签名为 (prompt, model, parameters) -> (response, record_id)
        yield_progress: 进度输出回调

    返回:
        ThinkPlanResult 实例
    """
    result = ThinkPlanResult()

    def _progress(msg: str):
        if yield_progress:
            yield_progress(msg)

    planners = config.planners
    if not planners:
        _progress(f"{STREAM_TAG_THINK} 错误：没有配置 Planner")
        return result

    # ========== 阶段 1：初始规划生成 ==========
    _progress(f"{STREAM_TAG_THINK} 阶段 1/6：初始规划生成")

    planner_results: List[PlannerResult] = []

    for planner in planners:
        _progress(f"{STREAM_TAG_PLANNER} {planner.planner_id}（{planner.role_description}）正在生成方案...")

        planner_skills_hint = skills_hint
        planner_solutions_hint = solutions_hint
        planner_tools_hint = tools_hint
        if isinstance(planner_hints, dict):
            slot = planner_hints.get(planner.planner_id)
            if isinstance(slot, dict):
                if isinstance(slot.get("skills_hint"), str) and str(slot.get("skills_hint") or "").strip():
                    planner_skills_hint = str(slot.get("skills_hint") or "").strip()
                if isinstance(slot.get("solutions_hint"), str) and str(slot.get("solutions_hint") or "").strip():
                    planner_solutions_hint = str(slot.get("solutions_hint") or "").strip()
                if isinstance(slot.get("tools_hint"), str) and str(slot.get("tools_hint") or "").strip():
                    planner_tools_hint = str(slot.get("tools_hint") or "").strip()

        prompt = _build_initial_planning_prompt(
            message=message,
            workdir=workdir,
            planner_config=planner,
            graph_hint=graph_hint,
            skills_hint=planner_skills_hint,
            solutions_hint=planner_solutions_hint,
            tools_hint=planner_tools_hint,
            max_steps=max_steps,
        )

        response, record_id = llm_call_func(
            prompt,
            planner.model,
            {"temperature": planner.temperature},
        )

        planner_result = PlannerResult.from_llm_response(
            planner.planner_id,
            response,
            record_id,
        )
        planner_results.append(planner_result)

        if record_id:
            result.llm_record_ids.append(record_id)

        _progress(f"{STREAM_TAG_PLANNER} {planner.planner_id} 生成了 {len(planner_result.plan)} 步方案")

    # ========== 阶段 2：第一轮投票 ==========
    _progress(f"{STREAM_TAG_THINK} 阶段 2/6：第一轮投票")

    plans_for_vote = [pr.to_dict() for pr in planner_results]
    vote_results_round1: List[VoteResult] = []

    for i, planner in enumerate(planners):
        _progress(f"{STREAM_TAG_VOTE} {planner.planner_id} 正在投票...")

        # 第一轮可以投给自己
        vote_prompt = build_vote_prompt(message, plans_for_vote, can_vote_self=True)

        response, record_id = llm_call_func(
            vote_prompt,
            planner.model,
            {"temperature": 0.3},  # 投票用低温度
        )

        vote_result = VoteResult.from_llm_response(planner.planner_id, response)
        vote_results_round1.append(vote_result)

        if record_id:
            result.llm_record_ids.append(record_id)

    # 选出得票最高的方案
    winners_round1 = select_winners_by_ratio(
        vote_results_round1,
        len(planner_results),
        config.first_vote_select_ratio,
        config.tiebreaker_index,
    )

    result.vote_records.append({
        "round": 1,
        "phase": THINK_PHASE_FIRST_VOTE,
        "votes": count_votes(vote_results_round1, len(planner_results)),
        "winners": winners_round1,
    })

    _progress(f"{STREAM_TAG_VOTE} 第一轮投票结果：方案 {winners_round1} 入选")

    # ========== 阶段 3：观点改进 ==========
    _progress(f"{STREAM_TAG_THINK} 阶段 3/6：观点改进")

    winning_plan_data = planner_results[winners_round1[0]].to_dict() if winners_round1 else {}
    improved_results: List[PlannerResult] = []

    for i, planner_result in enumerate(planner_results):
        if i in winners_round1:
            # 入选者保持原方案
            improved_results.append(planner_result)
        else:
            # 未入选者改进方案
            planner = planners[i]
            _progress(f"{STREAM_TAG_PLANNER} {planner.planner_id} 正在改进方案...")

            improve_prompt = _build_improve_prompt(
                message=message,
                workdir=workdir,
                original_plan=planner_result.to_dict(),
                winning_plan=winning_plan_data,
            )

            response, record_id = llm_call_func(
                improve_prompt,
                planner.model,
                {"temperature": planner.temperature},
            )

            improved = PlannerResult.from_llm_response(
                planner.planner_id,
                response,
                record_id,
            )
            improved_results.append(improved)

            if record_id:
                result.llm_record_ids.append(record_id)

            _progress(f"{STREAM_TAG_PLANNER} {planner.planner_id} 改进完成：{improved.improvements[:30]}...")

    # ========== 阶段 4：第二轮投票 ==========
    _progress(f"{STREAM_TAG_THINK} 阶段 4/6：第二轮投票")

    plans_for_vote_round2 = [pr.to_dict() for pr in improved_results]
    vote_results_round2: List[VoteResult] = []

    for i, planner in enumerate(planners):
        _progress(f"{STREAM_TAG_VOTE} {planner.planner_id} 正在投票...")

        # 第二轮不能投给自己
        vote_prompt = build_vote_prompt(message, plans_for_vote_round2, can_vote_self=False)

        response, record_id = llm_call_func(
            vote_prompt,
            planner.model,
            {"temperature": 0.3},
        )

        vote_result = VoteResult.from_llm_response(planner.planner_id, response)
        vote_results_round2.append(vote_result)

        if record_id:
            result.llm_record_ids.append(record_id)

    # 选出得票最高的 2 个方案
    winners_round2 = select_winners(
        vote_results_round2,
        len(improved_results),
        config.second_vote_select_count,
        config.tiebreaker_index,
    )

    result.vote_records.append({
        "round": 2,
        "phase": THINK_PHASE_SECOND_VOTE,
        "votes": count_votes(vote_results_round2, len(improved_results)),
        "winners": winners_round2,
    })

    _progress(f"{STREAM_TAG_VOTE} 第二轮投票结果：方案 {winners_round2} 入选")

    # ========== 阶段 5：详细阐述 ==========
    _progress(f"{STREAM_TAG_THINK} 阶段 5/6：详细阐述")

    elaborations: List[ElaborationResult] = []

    for winner_idx in winners_round2:
        planner = planners[winner_idx]
        plan_data = improved_results[winner_idx].to_dict()

        _progress(f"{STREAM_TAG_PLANNER} {planner.planner_id} 正在详细阐述...")

        elaborate_prompt = _build_elaborate_prompt(message, plan_data)

        response, record_id = llm_call_func(
            elaborate_prompt,
            planner.model,
            {"temperature": 0.5},
        )

        elaboration = ElaborationResult.from_llm_response(planner.planner_id, response)
        elaborations.append(elaboration)

        if record_id:
            result.llm_record_ids.append(record_id)

        _progress(
            f"{STREAM_TAG_PLANNER} {planner.planner_id} 阐述完成，置信度: {elaboration.overall_confidence:.2f}"
        )

    # ========== 阶段 6：最终投票 ==========
    _progress(f"{STREAM_TAG_THINK} 阶段 6/6：最终投票")

    # 只对入选的 2 个方案进行最终投票
    final_plans = [improved_results[idx].to_dict() for idx in winners_round2]
    vote_results_final: List[VoteResult] = []

    for planner in planners:
        _progress(f"{STREAM_TAG_VOTE} {planner.planner_id} 正在进行最终投票...")

        vote_prompt = build_vote_prompt(message, final_plans, can_vote_self=False)

        response, record_id = llm_call_func(
            vote_prompt,
            planner.model,
            {"temperature": 0.2},  # 最终投票用最低温度
        )

        vote_result = VoteResult.from_llm_response(planner.planner_id, response)
        vote_results_final.append(vote_result)

        if record_id:
            result.llm_record_ids.append(record_id)

    # 选出最终胜出者
    final_winner_local = select_winners(
        vote_results_final,
        len(final_plans),
        1,
        config.tiebreaker_index,
    )[0]

    # 映射回原始索引
    final_winner_idx = winners_round2[final_winner_local]

    result.vote_records.append({
        "round": 3,
        "phase": THINK_PHASE_FINAL_VOTE,
        "votes": count_votes(vote_results_final, len(final_plans)),
        "winner": final_winner_idx,
    })

    _progress(f"{STREAM_TAG_VOTE} 最终投票结果：方案 {final_winner_idx} 胜出")

    # ========== 构建最终结果 ==========
    final_plan_result = improved_results[final_winner_idx]
    final_plan_data = final_plan_result.to_dict()

    result.winning_planner_id = final_plan_result.planner_id
    result.winning_plan_data = final_plan_data
    result.elaboration = elaborations[final_winner_local] if final_winner_local < len(elaborations) else None

    # 执行前归一化：把 Planner 输出修复成“可执行计划”（与 do 模式约束对齐）。
    # 说明：这里必须做兜底修复，否则 allow 缺失 / task_output 不在最后 会导致执行阶段跑偏或直接失败。
    titles, briefs, allows, artifacts, norm_err = normalize_plan_data_for_execution(
        final_plan_data,
        max_steps=max_steps,
    )
    if norm_err:
        _progress(f"{STREAM_TAG_THINK} 规划输出不合法：{norm_err}")
        return result

    result.plan_titles = titles
    result.plan_briefs = briefs
    result.plan_allows = allows
    result.plan_artifacts = artifacts

    # 保存落选方案
    for i, pr in enumerate(improved_results):
        if i != final_winner_idx:
            result.alternative_plans.append(pr.to_dict())

    _progress(f"{STREAM_TAG_THINK} 规划完成，采用 {result.winning_planner_id} 的方案，共 {len(result.plan_titles)} 步")

    return result


def run_think_planning(
    config: ThinkConfig,
    message: str,
    workdir: str,
    graph_hint: str,
    skills_hint: str,
    solutions_hint: str,
    tools_hint: str,
    max_steps: int,
    llm_call_func: LLMCallFunc,
    planner_hints: Optional[Dict[str, Dict[str, str]]] = None,
) -> Generator[str, None, ThinkPlanResult]:
    """
    Generator 版本的 Think 模式规划，支持流式输出进度。

    用法:
        gen = run_think_planning(...)
        try:
            while True:
                progress = next(gen)
                print(progress)
        except StopIteration as e:
            result = e.value
    """
    progress_messages: List[str] = []

    def collect_progress(msg: str):
        progress_messages.append(msg)

    result = run_think_planning_sync(
        config=config,
        message=message,
        workdir=workdir,
        graph_hint=graph_hint,
        skills_hint=skills_hint,
        solutions_hint=solutions_hint,
        tools_hint=tools_hint,
        max_steps=max_steps,
        llm_call_func=llm_call_func,
        yield_progress=collect_progress,
        planner_hints=planner_hints,
    )

    # 逐条输出进度消息
    for msg in progress_messages:
        yield msg

    return result
