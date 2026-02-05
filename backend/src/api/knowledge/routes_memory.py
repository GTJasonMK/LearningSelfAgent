from fastapi import APIRouter

from backend.src.api.knowledge.memory.routes_graph import router as graph_router
from backend.src.api.knowledge.memory.routes_items import router as items_router
from backend.src.api.knowledge.memory.routes_skill_validations import (
    router as skill_validations_router,
)
from backend.src.api.knowledge.memory.routes_skills import router as skills_router

router = APIRouter()

# 说明：/memory 下接口按领域拆分（items / skills / graph），避免单文件堆积导致维护困难。
router.include_router(items_router)
router.include_router(skills_router)
router.include_router(skill_validations_router)
router.include_router(graph_router)

