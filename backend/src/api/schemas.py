from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class TaskCreate(BaseModel):
    title: str
    expectation_id: Optional[int] = None


class TaskUpdate(BaseModel):
    status: Optional[str] = None
    title: Optional[str] = None


class TaskStepCreate(BaseModel):
    title: str
    status: Optional[str] = None
    detail: Optional[str] = None
    step_order: Optional[int] = None
    run_id: Optional[int] = None


class TaskStepUpdate(BaseModel):
    title: Optional[str] = None
    status: Optional[str] = None
    detail: Optional[str] = None
    result: Optional[str] = None
    error: Optional[str] = None
    step_order: Optional[int] = None
    run_id: Optional[int] = None


class TaskExecuteRequest(BaseModel):
    run_summary: Optional[str] = None
    max_retries: Optional[int] = None
    on_failure: Optional[str] = None


class TaskOutputCreate(BaseModel):
    output_type: str
    content: str
    run_id: Optional[int] = None


class TaskRunCreate(BaseModel):
    status: Optional[str] = None
    summary: Optional[str] = None


class TaskRunUpdate(BaseModel):
    status: Optional[str] = None
    summary: Optional[str] = None


class PromptTemplateCreate(BaseModel):
    name: str
    template: str
    description: Optional[str] = None


class PromptTemplateUpdate(BaseModel):
    name: Optional[str] = None
    template: Optional[str] = None
    description: Optional[str] = None


class LLMCallCreate(BaseModel):
    prompt: Optional[str] = None
    template_id: Optional[int] = None
    variables: Optional[dict] = None
    provider: Optional[str] = None
    model: Optional[str] = None
    parameters: Optional[dict] = None
    task_id: Optional[int] = None
    run_id: Optional[int] = None
    dry_run: Optional[bool] = None


class LLMChatMessage(BaseModel):
    role: Literal["system", "user", "assistant"]
    content: str


class LLMChatStreamRequest(BaseModel):
    # 兼容：message 为单句输入；messages 为完整上下文（优先使用 messages）
    message: Optional[str] = None
    messages: Optional[List[LLMChatMessage]] = None
    provider: Optional[str] = None
    model: Optional[str] = None
    parameters: Optional[dict] = None


class ChatMessageCreate(BaseModel):
    role: Literal["system", "user", "assistant"]
    content: str
    task_id: Optional[int] = None
    run_id: Optional[int] = None
    metadata: Optional[dict] = None


class AgentCommandStreamRequest(BaseModel):
    """
    桌宠指令执行：自然语言 -> 生成 steps -> 执行并以 SSE 流式回传进度。
    """

    message: str
    max_steps: Optional[int] = None
    model: Optional[str] = None
    parameters: Optional[dict] = None
    dry_run: Optional[bool] = None
    mode: Optional[str] = None  # 执行模式：do（默认）/ think（多模型协作）/ auto（自动升降级 do↔think）
    think_config: Optional[dict] = None  # Think 模式配置（可选）


class AgentCommandResumeStreamRequest(BaseModel):
    """
    桌宠指令继续执行：用于在 run 进入 waiting（需要用户补充信息）后，继续后续 plan/ReAct。
    """

    run_id: int
    message: str
    prompt_token: Optional[str] = None
    session_key: Optional[str] = None


class AgentEvaluateStreamRequest(BaseModel):
    """
    评估 Agent：对某次 run 的执行过程做审查，产出问题清单/改进建议，并可维护 0..N 个 skills。
    """

    run_id: int
    message: Optional[str] = None
    model: Optional[str] = None
    parameters: Optional[dict] = None


class AgentRouteRequest(BaseModel):
    """
    桌宠自动模式选择：让 LLM 判断该走 chat / do（plan+ReAct） / think（多模型协作）。
    """

    message: str
    model: Optional[str] = None
    parameters: Optional[dict] = None


class ExpectationCreate(BaseModel):
    goal: str
    criteria: List[str] = Field(default_factory=list)


class EvalCreate(BaseModel):
    status: str
    score: Optional[float] = None
    notes: Optional[str] = None
    task_id: Optional[int] = None
    expectation_id: Optional[int] = None


class MemoryCreate(BaseModel):
    content: str
    memory_type: Optional[str] = None
    tags: Optional[List[str]] = None
    task_id: Optional[int] = None


class MemoryUpdate(BaseModel):
    content: Optional[str] = None
    memory_type: Optional[str] = None
    tags: Optional[List[str]] = None
    task_id: Optional[int] = None


class SkillCreate(BaseModel):
    name: str
    description: Optional[str] = None
    scope: Optional[str] = None
    category: Optional[str] = None
    tags: Optional[List[str]] = None
    triggers: Optional[List[str]] = None
    aliases: Optional[List[str]] = None
    source_path: Optional[str] = None
    prerequisites: Optional[List[str]] = None
    inputs: Optional[List[str]] = None
    outputs: Optional[List[str]] = None
    steps: Optional[List[str]] = None
    failure_modes: Optional[List[str]] = None
    validation: Optional[List[str]] = None
    version: Optional[str] = None
    task_id: Optional[int] = None


class SkillUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    scope: Optional[str] = None
    category: Optional[str] = None
    tags: Optional[List[str]] = None
    triggers: Optional[List[str]] = None
    aliases: Optional[List[str]] = None
    source_path: Optional[str] = None
    prerequisites: Optional[List[str]] = None
    inputs: Optional[List[str]] = None
    outputs: Optional[List[str]] = None
    steps: Optional[List[str]] = None
    failure_modes: Optional[List[str]] = None
    validation: Optional[List[str]] = None
    version: Optional[str] = None
    task_id: Optional[int] = None


class SkillValidationCreate(BaseModel):
    status: str
    notes: Optional[str] = None
    task_id: Optional[int] = None
    run_id: Optional[int] = None


class GraphNodeCreate(BaseModel):
    label: str
    node_type: Optional[str] = None
    attributes: Optional[dict] = None
    task_id: Optional[int] = None
    evidence: Optional[str] = None


class GraphNodeUpdate(BaseModel):
    label: Optional[str] = None
    node_type: Optional[str] = None
    attributes: Optional[dict] = None
    task_id: Optional[int] = None
    evidence: Optional[str] = None


class GraphEdgeCreate(BaseModel):
    source: int
    target: int
    relation: str
    confidence: Optional[float] = None
    evidence: Optional[str] = None


class GraphEdgeUpdate(BaseModel):
    relation: Optional[str] = None
    confidence: Optional[float] = None
    evidence: Optional[str] = None


class ConfigUpdate(BaseModel):
    tray_enabled: Optional[bool] = None
    pet_enabled: Optional[bool] = None
    panel_enabled: Optional[bool] = None


class LLMConfigUpdate(BaseModel):
    provider: Optional[str] = None
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    model: Optional[str] = None


class PermissionsUpdate(BaseModel):
    allowed_paths: Optional[List[str]] = None
    allowed_ops: Optional[List[str]] = None
    disabled_actions: Optional[List[str]] = None
    disabled_tools: Optional[List[str]] = None


class UpdateRequest(BaseModel):
    notes: Optional[str] = None


class LLMRecordCreate(BaseModel):
    prompt: str
    response: str
    task_id: Optional[int] = None
    run_id: Optional[int] = None


class ToolCreate(BaseModel):
    name: str
    description: str
    version: str
    metadata: Optional[dict] = None


class ToolUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    version: Optional[str] = None
    change_notes: Optional[str] = None
    metadata: Optional[dict] = None


class ToolCallCreate(BaseModel):
    tool_id: Optional[int] = None
    tool_name: Optional[str] = None
    tool_description: Optional[str] = None
    tool_version: Optional[str] = None
    tool_metadata: Optional[dict] = None
    task_id: Optional[int] = None
    skill_id: Optional[int] = None
    run_id: Optional[int] = None
    reuse: Optional[bool] = None
    reuse_status: Optional[str] = None
    reuse_notes: Optional[str] = None
    input: str
    output: str


class ToolReuseValidation(BaseModel):
    reuse_status: str
    reuse_notes: Optional[str] = None


class SearchRecordCreate(BaseModel):
    query: str
    sources: List[str]
    result_count: int
    task_id: Optional[int] = None


class MaintenanceCleanupRequest(BaseModel):
    mode: Optional[str] = None
    tables: Optional[List[str]] = None
    retention_days: Optional[int] = None
    before: Optional[str] = None
    limit: Optional[int] = None
    dry_run: Optional[bool] = None


class MaintenanceKnowledgeRollbackRequest(BaseModel):
    """
    知识治理：一键回滚/废弃某次 run 产生的知识（skills/tools）。

    说明：
    - dry_run=True：只返回影响面预览，不写库不落盘。
    """

    run_id: int
    dry_run: Optional[bool] = None
    include_skills: Optional[bool] = None
    include_tools: Optional[bool] = None
    draft_skill_target_status: Optional[str] = None
    approved_skill_target_status: Optional[str] = None
    tool_target_status: Optional[str] = None
    reason: Optional[str] = None


class MaintenanceKnowledgeAutoDeprecateRequest(BaseModel):
    """
    知识治理：按“最近成功率/复用验证”信号自动废弃低质量知识（可 dry_run 预览）。

    说明：
    - 仅处理“有验证信号”的知识：tool_call_records.reuse_status in (pass/fail)；
    - 避免误伤：默认要求 min_calls>0 且 success_rate < 阈值才会降级。
    """

    since_days: Optional[int] = None
    min_calls: Optional[int] = None
    success_rate_threshold: Optional[float] = None
    dry_run: Optional[bool] = None
    include_skills: Optional[bool] = None
    include_tools: Optional[bool] = None
    reason: Optional[str] = None


class MaintenanceKnowledgeRollbackVersionRequest(BaseModel):
    """
    知识治理：一键回滚到上一版本（skills/tools）。

    说明：
    - 依赖版本快照：skills 使用 skill_version_records，tools 使用 tool_version_records.previous_snapshot；
    - dry_run=True：仅预览，不写库不落盘。
    """

    kind: Literal["skill", "tool"]
    id: int
    dry_run: Optional[bool] = None
    reason: Optional[str] = None


class MaintenanceKnowledgeValidateTagsRequest(BaseModel):
    """
    知识治理：校验/修复 skills_items.tags（docs/agent 标签规范）。
    """

    dry_run: Optional[bool] = None
    fix: Optional[bool] = None
    strict_keys: Optional[bool] = None
    include_draft: Optional[bool] = None
    limit: Optional[int] = None


class MaintenanceKnowledgeDedupeSkillsRequest(BaseModel):
    """
    知识治理：去重 + 版本合并（同 scope/name）。
    """

    dry_run: Optional[bool] = None
    include_draft: Optional[bool] = None
    merge_across_domains: Optional[bool] = None
    reason: Optional[str] = None


class CleanupJobCreate(BaseModel):
    name: str
    mode: Optional[str] = None
    tables: Optional[List[str]] = None
    retention_days: Optional[int] = None
    before: Optional[str] = None
    limit: Optional[int] = None
    interval_minutes: Optional[int] = None
    enabled: Optional[bool] = None


class CleanupJobUpdate(BaseModel):
    name: Optional[str] = None
    mode: Optional[str] = None
    tables: Optional[List[str]] = None
    retention_days: Optional[int] = None
    before: Optional[str] = None
    limit: Optional[int] = None
    interval_minutes: Optional[int] = None
    enabled: Optional[bool] = None


class DomainCreate(BaseModel):
    domain_id: str
    name: str
    parent_id: Optional[str] = None
    description: Optional[str] = None
    keywords: Optional[List[str]] = None


class DomainUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    keywords: Optional[List[str]] = None
    status: Optional[str] = None
