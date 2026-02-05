# -*- coding: utf-8 -*-
"""
Agent 配置常量。

包含：
- 数据库和环境变量配置
- Agent 执行参数
- 检索参数
- 工具和技能配置
- 知识图谱配置
"""

from typing import Final, Tuple

# 数据库和环境变量
DB_ENV_VAR: Final = "AGENT_DB_PATH"
DB_RELATIVE_PATH: Final = ("..", "data", "agent.db")
PROMPT_ENV_VAR: Final = "AGENT_PROMPT_ROOT"

# 应用信息
APP_TITLE: Final = "智能体 API"

# 分页默认值
SINGLETON_ROW_ID: Final = 1
SINGLE_ROW_LIMIT: Final = 1
DEFAULT_PAGE_OFFSET: Final = 0
DEFAULT_PAGE_LIMIT: Final = 100
DEFAULT_RECORDS_EXPORT_LIMIT: Final = 10000
DEFAULT_CLEANUP_LIMIT: Final = 500
DEFAULT_CLEANUP_JOB_INTERVAL_MINUTES: Final = 60
CHAT_MESSAGES_MAX_LIMIT: Final = 200

# Agent 执行参数
AGENT_DEFAULT_MAX_STEPS: Final = 30
AGENT_EXPERIMENT_DIR_REL: Final = "backend/.agent/workspace"
AGENT_PLAN_RESERVED_STEPS: Final = 4
AGENT_PLAN_BRIEF_MAX_CHARS: Final = 10
AGENT_ROUTE_MAX_MESSAGE_CHARS: Final = 2000

# Agent 心跳和超时
AGENT_PLAN_HEARTBEAT_INTERVAL_SECONDS: Final = 2
AGENT_PLAN_MAX_WAIT_SECONDS: Final = 120
AGENT_STREAM_PUMP_POLL_INTERVAL_SECONDS: Final = 1
AGENT_STREAM_PUMP_IDLE_TIMEOUT_SECONDS: Final = 300

# SSE plan 事件节流（plan 事件 payload 可能很大，频繁广播会导致前端渲染抖动）
# - <=0 表示不节流
AGENT_SSE_PLAN_MIN_INTERVAL_SECONDS: Final = 0.2

# ReAct/do 执行：task_runs.agent_state/agent_plan 落库节流
# 说明：ReAct 循环在 before_step/after_step 等位置会频繁 update_task_run，容易放大 SQLite 写入与锁竞争；
# done/running 等非关键状态允许按时间窗口合并落盘，但 waiting/failed/done/stopped 等关键状态仍需立即落盘。
AGENT_REACT_PERSIST_MIN_INTERVAL_SECONDS: Final = 0.5

# Think 并行执行：状态落盘节流
# 说明：并行步骤可能在短时间内密集完成，若每步都 update_task_run 会造成 SQLite 写入放大与锁竞争。
# 该阈值用于限制 persist_loop_state 的最小间隔（秒），但 waiting/failed 等关键状态仍应立即落盘。
AGENT_THINK_PARALLEL_PERSIST_MIN_INTERVAL_SECONDS: Final = 0.5

# LLM 并发限制（同步调用，含规划/执行/反思等后台线程）
# 说明：Think 并行执行可能触发多线程同时调用 LLM，容易遇到供应商限流（429）/连接抖动；
# 通过全局与“按 provider+model”两级信号量限流，把峰值并发压到可控范围。
# - <=0 表示不限制
AGENT_LLM_MAX_CONCURRENCY_GLOBAL: Final = 8
AGENT_LLM_MAX_CONCURRENCY_PER_MODEL: Final = 4

# Agent Shell 命令和 HTTP 超时
AGENT_SHELL_COMMAND_DEFAULT_TIMEOUT_MS: Final = 20000
HTTP_REQUEST_DEFAULT_TIMEOUT_MS: Final = 20000

# Agent ReAct 参数
AGENT_REACT_OBSERVATION_MAX_CHARS: Final = 4000
AGENT_REACT_ACTION_RETRY_MAX_ATTEMPTS: Final = 2
AGENT_REACT_ARTIFACT_AUTOFIX_MAX_ATTEMPTS: Final = 2
AGENT_REACT_REPLAN_MAX_ATTEMPTS: Final = 2

# 评估与知识沉淀门槛（P1）
# 说明：
# - pass 门槛：用于判断“任务是否完成到可交付”的最低标准；
# - distill 门槛：用于判断“是否值得沉淀到知识库（skills/solutions/tools/graph）”，可以做到 pass 但不沉淀。
# - 分数范围约定为 0..100。
AGENT_REVIEW_PASS_SCORE_THRESHOLD: Final = 80
AGENT_REVIEW_DISTILL_SCORE_THRESHOLD: Final = 90
AGENT_REVIEW_DISTILL_STATUS_ALLOW: Final = "allow"
AGENT_REVIEW_DISTILL_STATUS_DENY: Final = "deny"
AGENT_REVIEW_DISTILL_STATUS_MANUAL: Final = "manual"

# 任务反馈
AGENT_TASK_FEEDBACK_KIND: Final = "task_feedback"
AGENT_TASK_FEEDBACK_STEP_TITLE: Final = "确认满意度"
AGENT_TASK_FEEDBACK_STEP_BRIEF: Final = "确认满意度"
AGENT_TASK_FEEDBACK_QUESTION: Final = (
    "请问你对这次任务的结果满意吗？\n"
    "- 满意：任务将标记为完成\n"
    "- 不满意：我会根据你的反馈继续改进"
)

# 领域检索参数
AGENT_DOMAIN_PICK_CANDIDATE_LIMIT: Final = 20
AGENT_DOMAIN_PICK_MAX_DOMAINS: Final = 3

# 图谱检索参数
AGENT_GRAPH_PICK_CANDIDATE_LIMIT: Final = 30
AGENT_GRAPH_PICK_MAX_NODES: Final = 6
AGENT_GRAPH_PROMPT_SNIPPET_MAX_CHARS: Final = 180
GRAPH_EDGE_REQUIRED_NODE_COUNT: Final = 2
GRAPH_LLM_MAX_CHARS: Final = 8000

# 技能检索参数
AGENT_SKILL_PICK_CANDIDATE_LIMIT: Final = 30
AGENT_SKILL_PICK_MAX_SKILLS: Final = 3

# 方案检索参数（Solution = skills_items.skill_type='solution'）
AGENT_SOLUTION_PICK_CANDIDATE_LIMIT: Final = 30
AGENT_SOLUTION_PICK_MAX_SOLUTIONS: Final = 3

# 知识检索 re-rank（P1：知识库质量）
# 说明：用于把“被复用次数/最近成功率”等信号融入候选排序，降低错误/低价值知识的曝光概率。
AGENT_KNOWLEDGE_RERANK_RECENT_DAYS: Final = 30
AGENT_KNOWLEDGE_RERANK_WEIGHT_BASE: Final = 0.7
AGENT_KNOWLEDGE_RERANK_WEIGHT_SUCCESS: Final = 0.2
AGENT_KNOWLEDGE_RERANK_WEIGHT_REUSE: Final = 0.1
AGENT_KNOWLEDGE_RERANK_REUSE_CALLS_CAP: Final = 10

# 检索阶段 LLM 结果缓存（P2：成本与策略）
# 说明：
# - 缓存“graph/domain/skills/solutions/memory pick”等 temperature=0 的选择结果，减少重复调用与耗时；
# - 仅做短 TTL 的内存缓存（不落库），并以 DB 路径作为 key 维度的一部分，避免测试/多实例串扰。
# - <=0 表示禁用
AGENT_RETRIEVAL_LLM_CACHE_TTL_SECONDS: Final = 600
AGENT_RETRIEVAL_LLM_CACHE_MAX_ENTRIES: Final = 2048

# 记忆检索参数
AGENT_MEMORY_PICK_CANDIDATE_LIMIT: Final = 30
AGENT_MEMORY_PICK_MAX_ITEMS: Final = 5
AGENT_MEMORY_PROMPT_SNIPPET_MAX_CHARS: Final = 240

# 技能自动生成参数
AGENT_RUN_SKILL_AUTOGEN_MAX_SKILLS: Final = 2
AGENT_RUN_SKILL_AUTOGEN_EXISTING_SKILLS_LIMIT: Final = 20
AGENT_RUN_SKILL_AUTOGEN_TEXT_SNIPPET_MAX_CHARS: Final = 4000

# 知识充分性判断
KNOWLEDGE_SUFFICIENCY_MIN_SKILLS: Final = 1
KNOWLEDGE_SUFFICIENCY_MIN_GRAPH_NODES: Final = 0

# 知识来源
SOURCE_MEMORY: Final = "memory"
SOURCE_SKILLS: Final = "skills"
SOURCE_GRAPH: Final = "graph"

# 注入权重
INJECTION_WEIGHT_MEMORY: Final = 0.9
INJECTION_WEIGHT_SKILL: Final = 0.8
INJECTION_WEIGHT_GRAPH: Final = 0.7

# 工具配置
DEFAULT_DISABLED_TOOLS: Final[Tuple] = ()
DEFAULT_SKILL_VERSION: Final = "0.1.0"
DEFAULT_TOOL_VERSION: Final = "0.1.0"
AUTO_TOOL_PREFIX: Final = "auto_tool"
AUTO_TOOL_DESCRIPTION_TEMPLATE: Final = "自动生成工具：{step_title}"
TOOL_METADATA_SOURCE_AUTO: Final = "auto"
TOOL_VERSION_CHANGE_NOTE_AUTO: Final = "自动同步版本"
TOOL_METADATA_APPROVAL_KEY: Final = "approval"
AUTO_SKILL_SUFFIX: Final = "技能"

# 内置工具
TOOL_NAME_WEB_FETCH: Final = "web_fetch"
TOOL_DESCRIPTION_WEB_FETCH: Final = "抓取指定 URL 内容（curl -sL）"
TOOL_VERSION_WEB_FETCH: Final = "0.1.0"
TOOL_WEB_FETCH_TIMEOUT_MS: Final = 15000
TOOL_WEB_FETCH_ARGS_TEMPLATE: Final[Tuple] = ("curl", "-sL", "{input}")

# Prompt 模板配置
PROMPT_TEMPLATE_NAME_MAX_CHARS: Final = 80
PROMPT_TEMPLATE_AUTO_RECOVER_PREFIX: Final = "auto_recovered_template_"

# 技能分类
SKILL_DEFAULT_CATEGORY: Final = "misc"
SKILL_CATEGORY_CHOICES: Final[Tuple] = (
    # 类目采用“点分层级”，便于前缀筛选：tool -> tool.*，tool.web -> tool.web.*
    # 注意：该列表会影响 skills_upsert 对 category 的合法性校验；必须与技能生成/落盘路径保持一致。
    "tool.web",
    "tool.shell",
    "data.extract",
    "data.transform",
    "agent.workflow",
    "debug.ipc",
    "debug.frontend",
    "dev.fs",
    "dev.git",
    "misc",
)
SKILL_SCOPE_TOOL_PREFIX: Final = "tool:"
