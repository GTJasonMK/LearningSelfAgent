"""Knowledge 查询服务聚合入口（兼容层）。

说明：
- 该模块保留既有导入路径：`backend.src.services.knowledge.knowledge_query`；
- 具体实现已按领域拆分到 `backend.src.services.knowledge.query.*`；
- API/runner 可以继续从本模块导入，无需修改调用方。
"""

from backend.src.services.knowledge.query import *  # noqa: F401,F403
