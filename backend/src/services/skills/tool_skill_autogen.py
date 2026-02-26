import json
import logging
from typing import Any, Dict, Optional
from backend.src.common.utils import coerce_str_list, extract_json_object, now_iso, parse_json_value, truncate_text
from backend.src.constants import (
    DEFAULT_SKILL_VERSION,
    SKILL_DEFAULT_CATEGORY,
    SKILL_SCOPE_TOOL_PREFIX,
)
from backend.src.repositories.skills_repo import SkillCreateParams, create_skill
from backend.src.prompt.system_prompts import load_system_prompt
from backend.src.services.llm.llm_client import call_openai, resolve_default_model
from backend.src.services.skills.skills_publish import classify_and_publish_skill
from backend.src.services.knowledge.skill_tag_policy import normalize_skill_tags
from backend.src.storage import get_connection

logger = logging.getLogger(__name__)
def _tool_scope(tool_id: int) -> str:
    return f"{SKILL_SCOPE_TOOL_PREFIX}{int(tool_id)}"


def find_tool_skill_id(tool_id: int) -> Optional[int]:
    scope = _tool_scope(tool_id)
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id FROM skills_items WHERE scope = ? ORDER BY id ASC LIMIT 1",
            (scope,),
        ).fetchone()
    if not row:
        return None
    try:
        return int(row["id"])
    except Exception:
        return None


def autogen_tool_skill_from_call(
    *,
    tool_id: int,
    tool_input: str,
    tool_output: str,
    task_id: Optional[int] = None,
    run_id: Optional[int] = None,
    model: Optional[str] = None,
) -> dict:
    """
    将“新创建的工具”总结成技能卡，并写入 skills_items + backend/prompt/skills。

    设计目标：
    - Agent 在执行中临时创建的新工具，不应只存在于本次输出；
      应该沉淀为可检索的 skill，供下次任务优先复用。
    - 该流程依赖 LLM 与系统提示词：若缺失/失败应显式报错，避免“假成功”。
    """
    try:
        tool_id_value = int(tool_id)
    except Exception:
        return {"ok": False, "error": "tool_id_invalid"}

    existing_id = find_tool_skill_id(tool_id_value)
    if existing_id is not None:
        return {"ok": True, "status": "exists", "skill_id": existing_id}

    with get_connection() as conn:
        tool_row = conn.execute(
            "SELECT * FROM tools_items WHERE id = ?",
            (tool_id_value,),
        ).fetchone()
    if not tool_row:
        return {"ok": False, "error": "tool_not_found"}

    tool_name = str(tool_row["name"] or "").strip()
    tool_description = str(tool_row["description"] or "").strip()
    tool_version = str(tool_row["version"] or "").strip()
    metadata_obj = parse_json_value(tool_row["metadata"]) if tool_row["metadata"] else None

    tool_payload = {
        "tool": {
            "id": tool_id_value,
            "name": tool_name,
            "description": tool_description,
            "version": tool_version,
            "metadata": metadata_obj,
        },
        "example": {
            "input": truncate_text(str(tool_input or ""), 600),
            "output": truncate_text(str(tool_output or ""), 1200),
        },
    }

    prompt = load_system_prompt("skill_from_tool")
    if not prompt:
        return {"ok": False, "error": "prompt_not_found", "prompt": "skill_from_tool"}

    meta: Dict[str, Any] = {}
    model_value = (model or "").strip() or resolve_default_model()
    text, _, err = call_openai(
        prompt.format(tool=json.dumps(tool_payload, ensure_ascii=False)),
        model_value,
        {"temperature": 0.2},
    )
    if err or not text:
        return {"ok": False, "error": f"llm_failed:{err or 'empty_response'}", "model": model_value}
    obj = extract_json_object(text)
    if not isinstance(obj, dict):
        return {"ok": False, "error": "invalid_json", "model": model_value}
    meta = obj

    name = str(meta.get("name") or "").strip()
    description = str(meta.get("description") or "").strip()
    steps = coerce_str_list(meta.get("steps"), max_items=10)
    validation = coerce_str_list(meta.get("validation"), max_items=6)
    failure_modes = coerce_str_list(meta.get("failure_modes"), max_items=6)
    tags = coerce_str_list(meta.get("tags"), max_items=12)
    triggers = coerce_str_list(meta.get("triggers"), max_items=18)
    aliases = coerce_str_list(meta.get("aliases"), max_items=6)

    if not name:
        return {"ok": False, "error": "missing_field:name", "model": model_value}
    if not description:
        return {"ok": False, "error": "missing_field:description", "model": model_value}
    if not steps:
        return {"ok": False, "error": "missing_field:steps", "model": model_value}
    if not validation:
        return {"ok": False, "error": "missing_field:validation", "model": model_value}

    created_at = now_iso()
    scope = _tool_scope(tool_id_value)
    # 默认归到 tool.shell：后续 classify_and_publish_skill 会进一步修正
    category = SKILL_DEFAULT_CATEGORY
    if isinstance(metadata_obj, dict):
        exec_type = str((metadata_obj.get("exec") or {}).get("type") or "").strip().lower()
        if exec_type == "shell":
            category = "tool.shell"

    # 关联标签规范（docs/agent）：补齐可审计溯源标签
    tags = tags + [f"tool:{tool_id_value}"]
    if task_id is not None:
        tags.append(f"task:{int(task_id)}")
    if run_id is not None:
        tags.append(f"run:{int(run_id)}")
    tags.append("domain:misc")

    # tags 规范化：避免 LLM 输出污染检索索引
    tags, tag_issues = normalize_skill_tags(tags, strict_keys=False)
    if tag_issues:
        logger.warning("tool_skill_autogen: tag normalization issues for tool %d: %s", tool_id_value, tag_issues)

    # 创建前二次检查（防止 LLM 调用期间另一个线程已创建同 scope 的技能）
    recheck_id = find_tool_skill_id(tool_id_value)
    if recheck_id is not None:
        return {"ok": True, "status": "exists", "skill_id": recheck_id}

    skill_id = create_skill(
        SkillCreateParams(
            name=name,
            created_at=created_at,
            description=description,
            scope=scope,
            category=category,
            tags=tags,
            triggers=triggers,
            aliases=aliases,
            source_path=None,
            prerequisites=[],
            inputs=[truncate_text(str(tool_input or ""), 600)],
            outputs=[truncate_text(str(tool_output or ""), 1200)],
            steps=steps,
            failure_modes=failure_modes,
            validation=validation,
            version=DEFAULT_SKILL_VERSION,
            task_id=task_id,
            domain_id="misc",
            skill_type="methodology",
            status="approved",
            source_task_id=int(task_id) if task_id is not None else None,
            source_run_id=int(run_id) if run_id is not None else None,
        )
    )

    publish = classify_and_publish_skill(skill_id=skill_id, model=model_value)
    return {
        "ok": True,
        "status": "created",
        "skill_id": skill_id,
        "tool_id": tool_id_value,
        "run_id": run_id,
        "publish": publish,
    }
