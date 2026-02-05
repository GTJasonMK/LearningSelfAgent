"""
提示词/技能文件库（文件系统层）。

说明：
- backend/prompt/system/: 内置系统提示词（推动流程的 prompt）
- backend/prompt/skills/: AI 总结的技能库（按 category 分目录）

后端会将 skills 文件同步到 SQLite 的 skills_items 表，供 Agent 执行时检索与复用。
"""

