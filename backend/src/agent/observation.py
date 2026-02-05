from backend.src.constants import AGENT_REACT_OBSERVATION_MAX_CHARS


def _truncate_observation(text: str) -> str:
    """
    将观测（stdout/JSON/长文本）截断到固定长度，避免塞爆 ReAct prompt。
    """
    value = (text or "").strip()
    if not value:
        return ""
    if len(value) <= AGENT_REACT_OBSERVATION_MAX_CHARS:
        return value
    return value[:AGENT_REACT_OBSERVATION_MAX_CHARS] + "..."

