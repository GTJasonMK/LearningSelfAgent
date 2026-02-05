# -*- coding: utf-8 -*-
"""
杂项配置常量。

包含：
- SQL 布尔值
- 清理配置
- 记忆配置
- 图谱配置
- 验证状态
- 权限配置
- 前端配置
"""

from typing import Final, Tuple

# SQL 布尔值
SQL_BOOL_TRUE: Final = 1
SQL_BOOL_FALSE: Final = 0

# 清理模式
CLEANUP_MODE_DELETE: Final = "delete"
CLEANUP_MODE_ARCHIVE: Final = "archive"
CLEANUP_JOB_STATUS_ENABLED: Final = "enabled"
CLEANUP_JOB_STATUS_DISABLED: Final = "disabled"
CLEANUP_RUN_STATUS_SUCCESS: Final = "success"
CLEANUP_RUN_STATUS_FAILED: Final = "failed"

# 记忆类型
MEMORY_TYPE_SHORT_TERM: Final = "short_term"
MEMORY_TYPE_LONG_TERM: Final = "long_term"
DEFAULT_MEMORY_TYPE: Final = MEMORY_TYPE_SHORT_TERM
MEMORY_AUTO_TASK_RESULT_MAX_CHARS: Final = 400
MEMORY_TAG_AUTO: Final = "auto"
MEMORY_TAG_TASK_RESULT: Final = "task_result"

# 图谱抽取状态
GRAPH_EXTRACT_STATUS_QUEUED: Final = "queued"
GRAPH_EXTRACT_STATUS_RUNNING: Final = "running"
GRAPH_EXTRACT_STATUS_DONE: Final = "done"
GRAPH_EXTRACT_STATUS_FAILED: Final = "failed"
GRAPH_EXTRACT_MAX_ATTEMPTS: Final = 3

# 验证状态
SKILL_VALIDATION_STATUS_PASS: Final = "pass"
SKILL_VALIDATION_STATUS_FAIL: Final = "fail"
SKILL_VALIDATION_STATUS_UNKNOWN: Final = "unknown"
TOOL_REUSE_STATUS_PASS: Final = "pass"
TOOL_REUSE_STATUS_FAIL: Final = "fail"
TOOL_REUSE_STATUS_UNKNOWN: Final = "unknown"
TOOL_APPROVAL_STATUS_DRAFT: Final = "draft"
TOOL_APPROVAL_STATUS_APPROVED: Final = "approved"
TOOL_APPROVAL_STATUS_REJECTED: Final = "rejected"

# 权限操作类型
OP_READ: Final = "read"
OP_WRITE: Final = "write"
OP_EXEC: Final = "execute"
DEFAULT_ALLOWED_PATHS: Final[Tuple] = ()
DEFAULT_ALLOWED_OPS: Final = (OP_READ, OP_WRITE, OP_EXEC)
DEFAULT_DISABLED_ACTIONS: Final[Tuple] = ()

# 前端配置默认值
DEFAULT_TRAY_ENABLED: Final = True
DEFAULT_PET_ENABLED: Final = True
DEFAULT_PANEL_ENABLED: Final = True

# 调试输出最大长度
AGENT_DEBUG_OUTPUT_MAX_CHARS: Final = 1200

# 任务输出类型（扩展）
TASK_OUTPUT_TYPE_USER_ANSWER: Final = "user_answer"
TASK_OUTPUT_TYPE_DEBUG: Final = "debug"
