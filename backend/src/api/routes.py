from fastapi import APIRouter

from backend.src.api.agent.routes_agent import router as agent_router
from backend.src.api.agent.routes_agent_evaluate import router as agent_evaluate_router
from backend.src.api.agent.routes_agent_resume import router as agent_resume_router
from backend.src.api.agent.routes_agent_reviews import router as agent_reviews_router
from backend.src.api.agent.routes_agent_runs import router as agent_runs_router
from backend.src.api.knowledge.routes_chat import router as chat_router
from backend.src.api.knowledge.routes_domains import router as domains_router
from backend.src.api.knowledge.routes_memory import router as memory_router
from backend.src.api.knowledge.routes_prompts import router as prompts_router
from backend.src.api.knowledge.routes_records import router as records_router
from backend.src.api.knowledge.routes_search import router as search_router
from backend.src.api.knowledge.routes_skills import router as skills_router
from backend.src.api.knowledge.routes_tools import router as tools_router
from backend.src.api.system.routes_config import router as config_router
from backend.src.api.system.routes_expectations import router as expectations_router
from backend.src.api.system.routes_maintenance import router as maintenance_router
from backend.src.api.system.routes_metrics import router as metrics_router
from backend.src.api.system.routes_update import router as update_router
from backend.src.api.tasks.routes_tasks import router as tasks_router

router = APIRouter()
router.include_router(agent_router)
router.include_router(agent_evaluate_router)
router.include_router(agent_runs_router)
router.include_router(agent_reviews_router)
router.include_router(agent_resume_router)
router.include_router(chat_router)
router.include_router(tasks_router)
router.include_router(prompts_router)
router.include_router(expectations_router)
router.include_router(memory_router)
router.include_router(maintenance_router)
router.include_router(metrics_router)
router.include_router(records_router)
router.include_router(config_router)
router.include_router(update_router)
router.include_router(tools_router)
router.include_router(search_router)
router.include_router(skills_router)
router.include_router(domains_router)
