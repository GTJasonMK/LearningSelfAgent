# -*- coding: utf-8 -*-
"""
流式输出标签和 SSE 类型常量。

包含：
- STREAM_TAG_*: SSE 流式输出的文本标签
- SSE_TYPE_*: SSE 事件类型
"""

from typing import Final

# SSE 流式输出文本标签
STREAM_TAG_DOMAIN: Final = "【领域】"
STREAM_TAG_SKILLS: Final = "【技能】"
STREAM_TAG_SOLUTIONS: Final = "【方案】"
STREAM_TAG_PLAN: Final = "【规划】"
STREAM_TAG_GRAPH: Final = "【图谱】"
STREAM_TAG_MEMORY: Final = "【记忆】"
STREAM_TAG_TASK: Final = "【任务】"
STREAM_TAG_EXEC: Final = "【执行】"
STREAM_TAG_STEP: Final = "【步骤】"
STREAM_TAG_ASK: Final = "【询问】"
STREAM_TAG_RESULT: Final = "【结果】"
STREAM_TAG_OK: Final = "【完成】"
STREAM_TAG_FAIL: Final = "【失败】"
STREAM_TAG_SKIP: Final = "【跳过】"
STREAM_TAG_KNOWLEDGE: Final = "【知识】"

# Think 模式标签
STREAM_TAG_THINK: Final = "【思考】"
STREAM_TAG_PLANNER: Final = "【规划者】"
STREAM_TAG_VOTE: Final = "【投票】"
STREAM_TAG_REFLECTION: Final = "【反思】"
STREAM_TAG_EXECUTOR: Final = "【执行者】"

# SSE 事件类型
SSE_TYPE_DONE: Final = "done"
SSE_TYPE_RUN_CREATED: Final = "run_created"
SSE_TYPE_PLAN: Final = "plan"
SSE_TYPE_NEED_INPUT: Final = "need_input"
SSE_TYPE_MEMORY_ITEM: Final = "memory_item"

# 流式结果预览
STREAM_RESULT_PREVIEW_MAX_CHARS: Final = 240
