"""
通用工具与基础能力（与 FastAPI 路由层解耦）。

说明：
- 该包用于承载跨层复用的纯工具函数，避免 services/agent/actions 反向依赖 api.utils；
- 这里放“可复用的通用逻辑”，路由层相关的权限/HTTP 细节仍应留在 api 层。
"""

