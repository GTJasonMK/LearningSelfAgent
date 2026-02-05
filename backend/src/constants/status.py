# -*- coding: utf-8 -*-
"""
状态码常量。

包含：
- RUN_STATUS_*: 任务运行状态
- STEP_STATUS_*: 步骤状态
- STATUS_*: 通用状态
- EVAL_STATUS_*: 评估状态
- GRAPH_EXTRACT_STATUS_*: 图谱抽取状态
- CLEANUP_*: 清理任务状态
- UPDATE_STATUS_*: 更新状态
- TOOL_APPROVAL_STATUS_*: 工具审批状态
"""

from typing import Final

# 通用状态
HEALTH_STATUS_OK: Final = "ok"
STATUS_QUEUED: Final = "queued"
STATUS_RUNNING: Final = "running"
STATUS_WAITING: Final = "waiting"
STATUS_DONE: Final = "done"
STATUS_CANCELLED: Final = "cancelled"
STATUS_FAILED: Final = "failed"
STATUS_STOPPED: Final = "stopped"

# 任务运行状态
RUN_STATUS_PLANNED: Final = "planned"
RUN_STATUS_RUNNING: Final = "running"
RUN_STATUS_WAITING: Final = "waiting"
RUN_STATUS_DONE: Final = "done"
RUN_STATUS_FAILED: Final = "failed"
RUN_STATUS_STOPPED: Final = "stopped"

# 步骤状态
STEP_STATUS_PLANNED: Final = "planned"
STEP_STATUS_RUNNING: Final = "running"
STEP_STATUS_WAITING: Final = "waiting"
STEP_STATUS_DONE: Final = "done"
STEP_STATUS_FAILED: Final = "failed"
STEP_STATUS_SKIPPED: Final = "skipped"

# 评估状态
EVAL_STATUS_UNKNOWN: Final = "unknown"
EVAL_PASS_RATE_THRESHOLD: Final = 0.6

# 图谱抽取状态
GRAPH_EXTRACT_STATUS_QUEUED: Final = "queued"
GRAPH_EXTRACT_STATUS_RUNNING: Final = "running"
GRAPH_EXTRACT_STATUS_DONE: Final = "done"
GRAPH_EXTRACT_STATUS_FAILED: Final = "failed"
GRAPH_EXTRACT_MAX_ATTEMPTS: Final = 3

# 清理任务状态
CLEANUP_MODE_DELETE: Final = "delete"
CLEANUP_MODE_ARCHIVE: Final = "archive"
CLEANUP_JOB_STATUS_ENABLED: Final = "enabled"
CLEANUP_JOB_STATUS_DISABLED: Final = "disabled"

# 更新状态
UPDATE_STATUS_QUEUED: Final = "queued"
UPDATE_STATUS_RESTARTING: Final = "restarting"
UPDATE_STATUS_FAILED: Final = "failed"

# 工具审批状态
TOOL_APPROVAL_STATUS_DRAFT: Final = "draft"
TOOL_APPROVAL_STATUS_APPROVED: Final = "approved"
TOOL_APPROVAL_STATUS_REJECTED: Final = "rejected"

# SQL 布尔值
SQL_BOOL_TRUE: Final = 1
SQL_BOOL_FALSE: Final = 0
