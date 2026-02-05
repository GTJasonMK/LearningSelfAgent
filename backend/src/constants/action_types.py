# -*- coding: utf-8 -*-
"""
行动类型常量。

定义 Agent 可执行的所有 action 类型。
"""

from typing import Final

# 核心行动类型
ACTION_TYPE_LLM_CALL: Final = "llm_call"
ACTION_TYPE_MEMORY_WRITE: Final = "memory_write"
ACTION_TYPE_TASK_OUTPUT: Final = "task_output"
ACTION_TYPE_TOOL_CALL: Final = "tool_call"
ACTION_TYPE_SHELL_COMMAND: Final = "shell_command"
ACTION_TYPE_USER_PROMPT: Final = "user_prompt"

# 文件操作
ACTION_TYPE_FILE_WRITE: Final = "file_write"
ACTION_TYPE_FILE_READ: Final = "file_read"
ACTION_TYPE_FILE_APPEND: Final = "file_append"
ACTION_TYPE_FILE_LIST: Final = "file_list"
ACTION_TYPE_FILE_DELETE: Final = "file_delete"

# 网络操作
ACTION_TYPE_HTTP_REQUEST: Final = "http_request"

# 数据处理
ACTION_TYPE_JSON_PARSE: Final = "json_parse"

# 任务输出类型
TASK_OUTPUT_TYPE_TEXT: Final = "text"
TASK_OUTPUT_TYPE_FILE: Final = "file"
TASK_OUTPUT_TYPE_USER_PROMPT: Final = "user_prompt"
