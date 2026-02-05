from fastapi import APIRouter

from backend.src.api.tasks.routes_task_execute import router as task_execute_router
from backend.src.api.tasks.routes_task_outputs import router as task_outputs_router
from backend.src.api.tasks.routes_task_runs import router as task_runs_router
from backend.src.api.tasks.routes_task_steps import router as task_steps_router
from backend.src.api.tasks.routes_tasks_core import router as tasks_core_router

router = APIRouter()
router.include_router(tasks_core_router)
router.include_router(task_steps_router)
router.include_router(task_execute_router)
router.include_router(task_outputs_router)
router.include_router(task_runs_router)
