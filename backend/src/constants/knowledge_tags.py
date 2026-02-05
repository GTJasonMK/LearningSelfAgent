# -*- coding: utf-8 -*-
"""
知识治理：标签规范（skills_items.tags）。

说明：
- tags 用于检索/溯源（docs/agent），应保持“可读、可审计、可稳定匹配”；
- 本模块仅定义最小规则与默认阈值，具体校验/修复逻辑在 services 层实现。
"""

from typing import Final, Tuple


# tags 列表最大条数（避免技能卡膨胀导致检索污染）
SKILL_TAG_MAX_ITEMS: Final = 64

# 单个 tag 最大长度（避免异常长文本写入 tags）
SKILL_TAG_MAX_LEN: Final = 128

# key:value 形式允许的 key 前缀（默认宽松：未知 key 可在治理工具中提示）
SKILL_TAG_ALLOWED_KEY_PREFIXES: Final[Tuple[str, ...]] = (
    "domain",
    "task",
    "run",
    "mode",
    "tool",
    "skill",
    "ref_solution",
    "tool_name",
)

# 需要是正整数的 key 前缀
SKILL_TAG_INT_KEY_PREFIXES: Final[Tuple[str, ...]] = (
    "task",
    "run",
    "tool",
    "skill",
    "ref_solution",
)

# mode 允许值（与 route/do/think 口径对齐）
SKILL_TAG_MODE_VALUES: Final[Tuple[str, ...]] = ("do", "think", "chat")

