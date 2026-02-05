from fastapi import APIRouter

from backend.src.api.knowledge.records.routes_graph_extract_records import (
    router as graph_extract_records_router,
)
from backend.src.api.knowledge.records.routes_llm_records import (
    router as llm_records_router,
)
from backend.src.api.knowledge.records.routes_recent_records import (
    router as recent_records_router,
)
from backend.src.api.knowledge.records.routes_reuse_summary import (
    router as reuse_summary_router,
)
from backend.src.api.knowledge.records.routes_search_records import (
    router as search_records_router,
)
from backend.src.api.knowledge.records.routes_task_records import (
    router as task_records_router,
)
from backend.src.api.knowledge.records.routes_tool_records import (
    router as tool_records_router,
)

router = APIRouter()

# 说明：records 端点较多，拆分到 backend/src/api/knowledge/records/ 下，避免 routes_records.py 过大难维护。
router.include_router(llm_records_router)
router.include_router(tool_records_router)
router.include_router(reuse_summary_router)
router.include_router(task_records_router)
router.include_router(graph_extract_records_router)
router.include_router(search_records_router)
router.include_router(recent_records_router)

