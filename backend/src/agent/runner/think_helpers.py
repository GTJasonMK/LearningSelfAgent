from __future__ import annotations

from typing import Callable, Dict, List, Optional, Tuple

from backend.src.services.llm.llm_client import call_openai


def build_plan_briefs_from_items(*, plan_titles: List[str], plan_items: List[dict]) -> List[str]:
    """
    从 plan_items 中提取 brief，缺失时回退为标题前缀。
    """
    briefs: List[str] = []
    for i, title in enumerate(plan_titles or []):
        brief = ""
        if 0 <= i < len(plan_items) and isinstance(plan_items[i], dict):
            brief = str(plan_items[i].get("brief") or "").strip()
        if not brief:
            brief = str(title or "").strip()[:10]
        briefs.append(brief)
    return briefs


def create_llm_call_func(
    *,
    base_model: str,
    base_parameters: dict,
    on_error: Optional[Callable[[str], None]] = None,
) -> Callable[[str, str, dict], Tuple[str, Optional[int]]]:
    """
    构造 Think 规划/反思共用的 LLM 调用函数。
    """

    def llm_call(prompt: str, call_model: str, call_params: dict) -> Tuple[str, Optional[int]]:
        merged_params: Dict = {**(base_parameters or {}), **(call_params or {})}
        text, record_id, err = call_openai(prompt, call_model or base_model, merged_params)
        if err:
            if callable(on_error):
                try:
                    on_error(str(err))
                except Exception:
                    pass
            return "", None
        return text or "", record_id

    return llm_call


def create_step_llm_config_resolver(
    *,
    base_model: str,
    think_config: object,
    role_resolver: Callable[[int, str, List[str]], str],
    fallback_role: str = "executor_code",
) -> Callable[[int, str, List[str]], Tuple[str, Dict]]:
    """
    构造每步 LLM 选模/参数覆盖解析器（do/think/resume 共用）。
    """

    def resolve(step_order: int, title: str, allow: List[str]) -> Tuple[str, Dict]:
        resolved_model = str(base_model or "").strip()
        overrides: Dict = {}

        try:
            role = str(role_resolver(int(step_order or 0), str(title or ""), list(allow or [])) or "").strip()
        except Exception:
            role = ""

        exec_cfg = None
        try:
            if role:
                exec_cfg = think_config.get_executor(role)
            if exec_cfg is None and isinstance(fallback_role, str) and fallback_role.strip():
                exec_cfg = think_config.get_executor(fallback_role.strip())
        except Exception:
            exec_cfg = None

        if exec_cfg:
            if isinstance(getattr(exec_cfg, "model", None), str) and str(exec_cfg.model).strip():
                resolved_model = str(exec_cfg.model).strip()
            if getattr(exec_cfg, "temperature", None) is not None:
                overrides["temperature"] = float(exec_cfg.temperature)
            if getattr(exec_cfg, "max_tokens", None) is not None:
                overrides["max_tokens"] = int(exec_cfg.max_tokens)
        return resolved_model, overrides

    return resolve
