# -*- coding: utf-8 -*-
"""
LLM 配置常量。

包含：
- LLM_STATUS_*: LLM 调用状态
- LLM_PROVIDER_*: LLM 提供者
- CHAT_ROLE_*: 聊天角色
- DEFAULT_LLM_MODEL: 默认模型
"""

from typing import Final

# LLM 调用状态
LLM_STATUS_RUNNING: Final = "running"
LLM_STATUS_SUCCESS: Final = "success"
LLM_STATUS_ERROR: Final = "error"
LLM_STATUS_DRY_RUN: Final = "dry_run"

# LLM 提供者
LLM_PROVIDER_OPENAI: Final = "openai"

# 默认模型
DEFAULT_LLM_MODEL: Final = "gpt-4o-mini"

# Chat 角色
CHAT_ROLE_SYSTEM: Final = "system"
CHAT_ROLE_USER: Final = "user"
CHAT_ROLE_ASSISTANT: Final = "assistant"
