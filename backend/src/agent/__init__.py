"""
Agent 领域逻辑（Plan-ReAct、检索、计划修复等）。

说明：
- API 层（backend/src/api/）只负责 HTTP/SSE 协议与参数校验；
- 具体的 Agent 规划/执行逻辑下沉到本包，便于复用与测试。
"""

