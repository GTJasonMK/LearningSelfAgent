"""
Think 模式多 Executor 执行。

实现：
- 步骤分配给不同的 Executor
- 并行执行策略（基于依赖关系）
- Executor 上下文管理
"""

import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from backend.src.constants import (
    THINK_EXECUTOR_ASSIGN_PROMPT_TEMPLATE,
    EXECUTOR_ROLE_CODE,
    EXECUTOR_ROLE_DOC,
    EXECUTOR_ROLE_TEST,
    ACTION_TYPE_FILE_WRITE,
    ACTION_TYPE_FILE_READ,
    ACTION_TYPE_FILE_APPEND,
    ACTION_TYPE_FILE_DELETE,
    ACTION_TYPE_SHELL_COMMAND,
    ACTION_TYPE_TOOL_CALL,
    ACTION_TYPE_LLM_CALL,
    ACTION_TYPE_TASK_OUTPUT,
    STREAM_TAG_EXECUTOR,
)
from backend.src.agent.json_utils import safe_json_parse
from backend.src.agent.think.think_config import ThinkConfig, ThinkExecutorConfig


@dataclass
class StepAssignment:
    """步骤分配信息。"""

    step_index: int
    executor: str  # executor_code / executor_doc / executor_test
    reason: str = ""
    depends_on: List[int] = field(default_factory=list)


@dataclass
class ExecutorAssignmentResult:
    """Executor 分配结果。"""

    assignments: List[StepAssignment] = field(default_factory=list)
    dependencies: List[Dict] = field(default_factory=list)  # 步骤间依赖关系
    raw_response: str = ""

    @classmethod
    def from_llm_response(cls, response: str) -> "ExecutorAssignmentResult":
        """从 LLM 响应解析。"""
        result = cls(raw_response=response)

        parsed = safe_json_parse(response)
        if parsed is None:
            return result

        # 解析分配
        for a in parsed.get("assignments", []):
            result.assignments.append(
                StepAssignment(
                    step_index=a.get("step_index", 0),
                    executor=a.get("executor", EXECUTOR_ROLE_CODE),
                    reason=a.get("reason", ""),
                )
            )

        # 解析依赖关系
        result.dependencies = parsed.get("dependencies", [])

        # 将依赖关系合并到分配中
        dep_map: Dict[int, List[int]] = {}
        for dep in result.dependencies:
            step_idx = dep.get("step_index", 0)
            depends_on = dep.get("depends_on", [])
            dep_map[step_idx] = depends_on

        for assignment in result.assignments:
            assignment.depends_on = dep_map.get(assignment.step_index, [])

        return result

    def get_executor_for_step(self, step_index: int) -> str:
        """获取某步骤的 Executor。"""
        for a in self.assignments:
            if a.step_index == step_index:
                return a.executor
        return EXECUTOR_ROLE_CODE  # 默认使用代码执行者

    def get_dependencies_for_step(self, step_index: int) -> List[int]:
        """获取某步骤的依赖。"""
        for a in self.assignments:
            if a.step_index == step_index:
                return a.depends_on
        return []


def _infer_executor_from_allow(allow: List[str], title: str) -> str:
    """
    根据 allow 和 title 推断 Executor 角色。

    规则：
    - file_write（代码文件）→ executor_code
    - file_write（文档/说明）→ executor_doc
    - shell_command → executor_code
    - tool_call → executor_code
    - llm_call（验证类）→ executor_test
    - task_output → executor_code（默认）
    """
    title_lower = title.lower()

    # 检查是否是文档类
    doc_keywords = ["文档", "说明", "readme", "doc", "markdown", ".md"]
    is_doc = any(kw in title_lower for kw in doc_keywords)

    # 检查是否是测试/验证类
    test_keywords = ["验证", "测试", "检查", "校验", "test", "verify", "check"]
    is_test = any(kw in title_lower for kw in test_keywords)

    if ACTION_TYPE_FILE_WRITE in allow:
        if is_doc:
            return EXECUTOR_ROLE_DOC
        return EXECUTOR_ROLE_CODE

    if ACTION_TYPE_SHELL_COMMAND in allow or ACTION_TYPE_TOOL_CALL in allow:
        return EXECUTOR_ROLE_CODE

    if ACTION_TYPE_LLM_CALL in allow:
        if is_test:
            return EXECUTOR_ROLE_TEST
        return EXECUTOR_ROLE_CODE

    if ACTION_TYPE_TASK_OUTPUT in allow:
        return EXECUTOR_ROLE_CODE

    return EXECUTOR_ROLE_CODE


def infer_executor_assignments(
    plan_titles: List[str],
    plan_allows: List[List[str]],
    plan_artifacts: List[str],
) -> ExecutorAssignmentResult:
    """
    根据计划步骤推断 Executor 分配（无需 LLM）。

    参数:
        plan_titles: 步骤标题列表
        plan_allows: 每步允许的 action 类型
        plan_artifacts: 预期产出文件

    返回:
        ExecutorAssignmentResult 实例
    """
    result = ExecutorAssignmentResult()

    def _normalize_path_token(path: str) -> str:
        """
        归一化标题/Artifacts 中的路径 token，便于推断依赖关系（docs/agent）。
        - 去引号/反引号
        - 去掉前导 ./（保持相对路径语义）
        - 统一斜杠方向
        """
        value = str(path or "").strip().strip("`'\"").strip()
        value = value.replace("\\", "/")
        while value.startswith("./"):
            value = value[2:]
        return value.strip()

    def _extract_prefixed_path(title: str, prefix: str) -> Optional[str]:
        """
        从标题前缀中提取目标路径：
        - 约定：file_write:相对路径 / file_read:相对路径 等
        - 支持带空格的路径（需要被单/双引号或反引号包裹）
        - 支持中文冒号（：）
        """
        if not isinstance(title, str):
            return None
        raw = title.strip()
        prefix_lower = str(prefix or "").strip().lower().strip()
        if not prefix_lower:
            return None
        prefix_base = prefix_lower.rstrip(":：").strip()
        if not prefix_base:
            return None
        match = re.match(
            rf"^{re.escape(prefix_base)}[:：]\s*(\"[^\"]+\"|'[^']+'|`[^`]+`|\S+)",
            raw,
            flags=re.IGNORECASE,
        )
        if not match:
            return None
        token = str(match.group(1) or "").strip()
        if (
            (token.startswith("\"") and token.endswith("\""))
            or (token.startswith("'") and token.endswith("'"))
            or (token.startswith("`") and token.endswith("`"))
        ):
            token = token[1:-1].strip()
        token = token.rstrip(",;)")
        token = _normalize_path_token(token)
        return token if token else None

    # 追踪文件产出
    file_producers: Dict[str, int] = {}  # 文件路径 -> 产出该文件的步骤索引
    artifacts_raw = [str(a or "").strip() for a in (plan_artifacts or []) if str(a or "").strip()]
    artifacts_norm = [_normalize_path_token(a) for a in artifacts_raw]

    for i, (title, allow) in enumerate(zip(plan_titles, plan_allows)):
        executor = _infer_executor_from_allow(allow, title)
        text_norm = str(title or "").replace("\\", "/")

        # 推断依赖关系
        depends_on: List[int] = []

        # 依赖推断（docs/agent）：
        # 1) 从 plan_artifacts 的引用推断（更通用，支持 shell_command/tool_call 等标题引用文件）。
        for raw_artifact, norm_artifact in zip(artifacts_raw, artifacts_norm):
            if not norm_artifact:
                continue
            hit = (raw_artifact and raw_artifact in title) or (norm_artifact and norm_artifact in text_norm)
            if not hit:
                continue
            if norm_artifact in file_producers:
                producer_idx = int(file_producers[norm_artifact])
                if producer_idx < i and producer_idx not in depends_on:
                    depends_on.append(producer_idx)

        # 2) 从 title 前缀推断（即使 plan_artifacts 为空也可工作）：
        #    file_read/file_append/file_delete 对同路径的 file_write 存在硬依赖。
        if ACTION_TYPE_FILE_READ in (allow or []):
            target = _extract_prefixed_path(title, "file_read:")
            if target and target in file_producers:
                producer_idx = int(file_producers[target])
                if producer_idx < i and producer_idx not in depends_on:
                    depends_on.append(producer_idx)
        if ACTION_TYPE_FILE_APPEND in (allow or []):
            target = _extract_prefixed_path(title, "file_append:")
            if target and target in file_producers:
                producer_idx = int(file_producers[target])
                if producer_idx < i and producer_idx not in depends_on:
                    depends_on.append(producer_idx)
        if ACTION_TYPE_FILE_DELETE in (allow or []):
            target = _extract_prefixed_path(title, "file_delete:")
            if target and target in file_producers:
                producer_idx = int(file_producers[target])
                if producer_idx < i and producer_idx not in depends_on:
                    depends_on.append(producer_idx)

        # 当前步骤产出/修改文件：更新 producer（必须在“依赖推断之后”执行）
        if ACTION_TYPE_FILE_WRITE in (allow or []):
            produced = _extract_prefixed_path(title, "file_write:")
            if produced:
                file_producers[str(produced)] = int(i)
            for raw_artifact, norm_artifact in zip(artifacts_raw, artifacts_norm):
                if not norm_artifact:
                    continue
                hit = (raw_artifact and raw_artifact in title) or (norm_artifact and norm_artifact in text_norm)
                if hit:
                    file_producers[str(norm_artifact)] = int(i)
        if ACTION_TYPE_FILE_APPEND in (allow or []):
            produced = _extract_prefixed_path(title, "file_append:")
            if produced:
                file_producers[str(produced)] = int(i)
        if ACTION_TYPE_FILE_DELETE in (allow or []):
            produced = _extract_prefixed_path(title, "file_delete:")
            if produced:
                file_producers[str(produced)] = int(i)

        result.assignments.append(
            StepAssignment(
                step_index=i,
                executor=executor,
                reason=f"根据 allow={allow} 和 title 自动推断",
                depends_on=depends_on,
            )
        )

    return result


def assign_executors_with_llm(
    plan_titles: List[str],
    plan_allows: List[List[str]],
    plan_artifacts: List[str],
    llm_call_func: Callable[[str, str, Dict], Tuple[str, Optional[int]]],
    model: str,
) -> ExecutorAssignmentResult:
    """
    使用 LLM 进行 Executor 分配。

    参数:
        plan_titles: 步骤标题列表
        plan_allows: 每步允许的 action 类型
        plan_artifacts: 预期产出文件
        llm_call_func: LLM 调用函数
        model: 使用的模型

    返回:
        ExecutorAssignmentResult 实例
    """
    # 构建计划描述
    plan_desc_lines = []
    for i, (title, allow) in enumerate(zip(plan_titles, plan_allows)):
        plan_desc_lines.append(f"{i}. {title} [allow: {', '.join(allow)}]")

    plan_desc = "\n".join(plan_desc_lines)
    artifacts_desc = ", ".join(plan_artifacts) if plan_artifacts else "（无）"

    prompt = THINK_EXECUTOR_ASSIGN_PROMPT_TEMPLATE.format(
        plan=plan_desc,
        artifacts=artifacts_desc,
    )

    response, _ = llm_call_func(prompt, model, {"temperature": 0.3})

    result = ExecutorAssignmentResult.from_llm_response(response)

    # 如果 LLM 分配失败，回退到推断
    if not result.assignments:
        return infer_executor_assignments(plan_titles, plan_allows, plan_artifacts)

    return result


def get_executable_steps(
    total_steps: int,
    completed_steps: Set[int],
    assignments: ExecutorAssignmentResult,
) -> List[int]:
    """
    获取当前可执行的步骤（依赖已满足）。

    参数:
        total_steps: 总步骤数
        completed_steps: 已完成的步骤索引集合
        assignments: Executor 分配结果

    返回:
        可执行的步骤索引列表
    """
    executable = []

    for i in range(total_steps):
        if i in completed_steps:
            continue

        # 检查依赖是否都已完成
        deps = assignments.get_dependencies_for_step(i)
        if all(d in completed_steps for d in deps):
            executable.append(i)

    return executable


def group_steps_by_executor(
    step_indices: List[int],
    assignments: ExecutorAssignmentResult,
) -> Dict[str, List[int]]:
    """
    按 Executor 分组步骤。

    参数:
        step_indices: 步骤索引列表
        assignments: Executor 分配结果

    返回:
        Dict[executor_role, List[step_indices]]
    """
    groups: Dict[str, List[int]] = {}

    for idx in step_indices:
        executor = assignments.get_executor_for_step(idx)
        if executor not in groups:
            groups[executor] = []
        groups[executor].append(idx)

    return groups


def build_executor_assignments_payload(
    *,
    plan_titles: List[str],
    plan_allows: List[List[str]],
) -> List[dict]:
    """
    构造可持久化的 executor_assignments（写入 task_runs.agent_state）。

    说明：
    - 该字段用于中断恢复/评估/审计（docs/agent 约定）；
    - 执行阶段本身仍可使用 allow+title 进行动态推断，但持久化信息有助于复盘与 UI 展示；
    - 当 plan 被反思/插入步骤修改后，应重新生成并持久化，避免 executor 漂移。
    """
    payload: List[dict] = []
    for i, title in enumerate(plan_titles or []):
        allow = plan_allows[i] if 0 <= i < len(plan_allows or []) else []
        role = _infer_executor_from_allow(allow or [], str(title or ""))
        payload.append(
            {
                "step_order": int(i) + 1,
                "executor": role,
                "allow": list(allow or []),
            }
        )
    return payload


@dataclass
class ExecutorContext:
    """Executor 执行上下文。"""

    executor_id: str
    config: ThinkExecutorConfig
    assigned_steps: List[int] = field(default_factory=list)
    completed_steps: List[int] = field(default_factory=list)
    current_step: Optional[int] = None
    observations: List[Dict] = field(default_factory=list)

    def is_idle(self) -> bool:
        """检查是否空闲。"""
        return self.current_step is None

    def has_pending_steps(self) -> bool:
        """检查是否有待执行步骤。"""
        return len(self.assigned_steps) > len(self.completed_steps)

    def get_next_step(self) -> Optional[int]:
        """获取下一个待执行步骤。"""
        for step in self.assigned_steps:
            if step not in self.completed_steps and step != self.current_step:
                return step
        return None

    def start_step(self, step_index: int):
        """开始执行步骤。"""
        self.current_step = step_index

    def complete_step(self, step_index: int, observation: Dict):
        """完成步骤。"""
        if step_index == self.current_step:
            self.current_step = None
        if step_index not in self.completed_steps:
            self.completed_steps.append(step_index)
        self.observations.append({
            "step_index": step_index,
            **observation,
        })


class ExecutorManager:
    """Executor 管理器。"""

    def __init__(self, config: ThinkConfig, assignments: ExecutorAssignmentResult):
        self.config = config
        self.assignments = assignments
        self.executors: Dict[str, ExecutorContext] = {}

        # 初始化 Executor 上下文
        for role, exec_config in config.executors.items():
            self.executors[role] = ExecutorContext(
                executor_id=role,
                config=exec_config,
            )

        # 分配步骤
        for assignment in assignments.assignments:
            executor = self.executors.get(assignment.executor)
            if executor:
                executor.assigned_steps.append(assignment.step_index)

    def get_executor(self, role: str) -> Optional[ExecutorContext]:
        """获取指定角色的 Executor。"""
        return self.executors.get(role)

    def get_executor_for_step(self, step_index: int) -> Optional[ExecutorContext]:
        """获取负责某步骤的 Executor。"""
        role = self.assignments.get_executor_for_step(step_index)
        return self.executors.get(role)

    def get_idle_executors(self) -> List[ExecutorContext]:
        """获取所有空闲的 Executor。"""
        return [e for e in self.executors.values() if e.is_idle()]

    def get_all_completed_steps(self) -> Set[int]:
        """获取所有已完成的步骤。"""
        completed = set()
        for executor in self.executors.values():
            completed.update(executor.completed_steps)
        return completed

    def is_all_completed(self, total_steps: int) -> bool:
        """检查是否所有步骤都已完成。"""
        return len(self.get_all_completed_steps()) >= total_steps

    def get_execution_summary(self) -> Dict:
        """获取执行概要。"""
        return {
            role: {
                "assigned": len(ctx.assigned_steps),
                "completed": len(ctx.completed_steps),
                "current": ctx.current_step,
            }
            for role, ctx in self.executors.items()
        }
