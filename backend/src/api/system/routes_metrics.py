from fastapi import APIRouter

from backend.src.services.metrics.agent_metrics import compute_agent_metrics

router = APIRouter()


@router.get("/metrics/agent")
def metrics_agent(since_days: int = 30) -> dict:
    """
    Agent 指标聚合（P3：可观测性）。
    """
    return compute_agent_metrics(since_days=since_days)

