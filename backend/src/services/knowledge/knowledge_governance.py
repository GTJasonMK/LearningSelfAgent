"""Knowledge 治理服务聚合入口（兼容层）。

说明：
- 该模块保留既有导入路径：`backend.src.services.knowledge.knowledge_governance`；
- 具体实现按职责拆分到 `backend.src.services.knowledge.governance.*`；
- API/调用方无需修改导入路径。
"""

from backend.src.services.knowledge.governance import *  # noqa: F401,F403
