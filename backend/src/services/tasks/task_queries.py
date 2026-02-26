"""任务查询服务聚合入口（兼容层）。

说明：
- 该模块保留既有导入路径：`backend.src.services.tasks.task_queries`；
- 具体实现已按领域拆分到 `backend.src.services.tasks.query.*`；
- 现有 API/runner 调用方无需修改。
"""

from backend.src.services.tasks.query import *  # noqa: F401,F403
