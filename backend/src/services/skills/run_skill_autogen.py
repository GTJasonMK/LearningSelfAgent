import json
from typing import Any, Dict, List, Optional

from backend.src.common.utils import (
    action_type_from_step_detail,
    extract_json_object,
    json_preview,
    now_iso,
    truncate_text,
)
from backend.src.constants import (
    ACTION_TYPE_FILE_WRITE,
    ACTION_TYPE_SHELL_COMMAND,
    ACTION_TYPE_TOOL_CALL,
    AGENT_RUN_SKILL_AUTOGEN_EXISTING_SKILLS_LIMIT,
    AGENT_RUN_SKILL_AUTOGEN_MAX_SKILLS,
    AGENT_RUN_SKILL_AUTOGEN_TEXT_SNIPPET_MAX_CHARS,
    ERROR_MESSAGE_LLM_CALL_FAILED,
    SKILL_CATEGORY_CHOICES,
    THINK_SKILL_AUTOGEN_MAX_SKILLS,
)
from backend.src.prompt.system_prompts import load_system_prompt
from backend.src.services.llm.llm_client import call_openai, resolve_default_model
from backend.src.services.skills.skills_publish import publish_skill_file
from backend.src.services.skills.skills_upsert import upsert_skill_from_agent_payload
from backend.src.storage import get_connection

def _compact_plan(plan_text: str) -> dict:
    obj = extract_json_object(plan_text or "") or {}
    if not isinstance(obj, dict):
        return {}
    # 控制体积：只保留标题/allow/artifacts，避免塞入完整 state
    return {
        "titles": obj.get("titles"),
        "allows": obj.get("allows"),
        "artifacts": obj.get("artifacts"),
    }


def _extract_step_action_type(detail_text: Optional[str]) -> Optional[str]:
    return action_type_from_step_detail(detail_text)


def _compact_steps(step_rows: List[dict]) -> List[dict]:
    compact: List[dict] = []
    for row in step_rows:
        title = str((row or {}).get("title") or "").strip()
        status = str((row or {}).get("status") or "").strip()
        action_type = _extract_step_action_type((row or {}).get("detail"))
        error = truncate_text(str((row or {}).get("error") or ""), 200)
        result = json_preview((row or {}).get("result"), 260)
        compact.append(
            {
                "title": title,
                "status": status,
                "action_type": action_type,
                "result_preview": result,
                "error_preview": error,
            }
        )
        if len(compact) >= 60:
            break
    return compact


def _compact_outputs(output_rows: List[dict]) -> List[dict]:
    compact: List[dict] = []
    for row in output_rows:
        out_type = str((row or {}).get("output_type") or "").strip()
        content = truncate_text(str((row or {}).get("content") or ""), 600)
        if content:
            compact.append({"type": out_type, "content_preview": content})
        if len(compact) >= 40:
            break
    return compact


def _compact_tool_calls(tool_rows: List[dict]) -> List[dict]:
    compact: List[dict] = []
    for row in tool_rows:
        compact.append(
            {
                "tool_id": row.get("tool_id"),
                "tool_name": row.get("tool_name"),
                "reuse": bool(row.get("reuse")),
                "reuse_status": row.get("reuse_status"),
                "input_preview": truncate_text(str(row.get("input") or ""), 260),
                "output_preview": truncate_text(str(row.get("output") or ""), 360),
            }
        )
        if len(compact) >= 30:
            break
    return compact


def _load_existing_skills(limit: int) -> List[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT id, name, category, description FROM skills_items ORDER BY id DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
    items: List[dict] = []
    for row in rows or []:
        items.append(
            {
                "id": int(row["id"]),
                "name": str(row["name"] or "").strip(),
                "category": str(row["category"] or "").strip(),
                "description": truncate_text(str(row["description"] or ""), 120),
            }
        )
    return items


def _should_autogen_from_actions(step_actions: List[Optional[str]]) -> bool:
    """
    仅在存在“工具/命令/写文件”等可迁移操作时，才尝试抽象技能。
    纯 llm_call/task_output 之类的 run 往往是普通对话，不值得沉淀技能，避免技能库膨胀。
    """
    for action in step_actions:
        if action in {ACTION_TYPE_TOOL_CALL, ACTION_TYPE_SHELL_COMMAND, ACTION_TYPE_FILE_WRITE}:
            return True
    return False


def autogen_skills_from_run(
    *,
    task_id: int,
    run_id: int,
    model: Optional[str] = None,
    parameters: Optional[dict] = None,
) -> dict:
    """
    从一次 run 的记录中抽象 0..N 个“可复用技能卡”（patterns），并 upsert 到 skills_items。

    重要约束：
    - 只沉淀“可迁移方法论”，不要把原始 steps/outputs 当技能步骤粘贴。
    - 最多生成少量技能，避免技能库膨胀。
    - LLM 不可用/输出不合法时直接跳过（不阻塞主流程）。
    """
    try:
        task_id_value = int(task_id)
        run_id_value = int(run_id)
    except Exception:
        return {"ok": False, "error": "task_id/run_id_invalid"}

    with get_connection() as conn:
        task_row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id_value,)).fetchone()
        run_row = conn.execute("SELECT * FROM task_runs WHERE id = ?", (run_id_value,)).fetchone()
        if not task_row or not run_row:
            return {"ok": False, "error": "task_or_run_not_found"}

        step_rows = conn.execute(
            "SELECT step_order, title, status, detail, result, error FROM task_steps WHERE task_id = ? AND run_id = ? "
            "ORDER BY step_order IS NULL, step_order ASC, id ASC",
            (task_id_value, run_id_value),
        ).fetchall()
        output_rows = conn.execute(
            "SELECT output_type, content FROM task_outputs WHERE task_id = ? AND run_id = ? ORDER BY id ASC",
            (task_id_value, run_id_value),
        ).fetchall()
        tool_rows = conn.execute(
            "SELECT r.tool_id, t.name AS tool_name, r.input, r.output, r.reuse, r.reuse_status "
            "FROM tool_call_records r LEFT JOIN tools_items t ON t.id = r.tool_id "
            "WHERE r.run_id = ? ORDER BY r.id ASC LIMIT 50",
            (run_id_value,),
        ).fetchall()

    state_obj = extract_json_object(str(run_row["agent_state"] or "")) if run_row else None
    if not isinstance(state_obj, dict):
        state_obj = {}

    mode = str(state_obj.get("mode") or "").strip().lower()
    if mode not in {"think", "do"}:
        mode = "do"

    steps_compact = _compact_steps([dict(r) for r in (step_rows or [])])
    outputs_compact = _compact_outputs([dict(r) for r in (output_rows or [])])
    tools_compact = _compact_tool_calls([dict(r) for r in (tool_rows or [])])

    step_actions = [it.get("action_type") for it in steps_compact]
    if not _should_autogen_from_actions(step_actions):
        return {"ok": True, "status": "skipped_no_transferable_actions"}

    existing_skills = _load_existing_skills(AGENT_RUN_SKILL_AUTOGEN_EXISTING_SKILLS_LIMIT)
    task_title = str(task_row["title"] or "").strip()
    run_meta = {
        "run_id": run_id_value,
        "status": str(run_row["status"] or "").strip(),
        "started_at": run_row["started_at"],
        "finished_at": run_row["finished_at"],
        "summary": run_row["summary"],
        "updated_at": run_row["updated_at"],
        "mode": mode,
    }
    if mode == "think":
        vote_records = state_obj.get("vote_records")
        if vote_records is None:
            vote_records = state_obj.get("plan_votes")
        alternative_plans = state_obj.get("alternative_plans")
        if alternative_plans is None:
            alternative_plans = state_obj.get("plan_alternatives")
        run_meta["think"] = {
            "think_config": state_obj.get("think_config"),
            "winning_planner_id": state_obj.get("winning_planner_id"),
            "vote_records": vote_records,
            "alternative_plans": alternative_plans,
            "reflection_count": state_obj.get("reflection_count"),
            "reflection_records": state_obj.get("reflection_records"),
            "executor_assignments": state_obj.get("executor_assignments"),
        }
    plan_compact = _compact_plan(str(run_row["agent_plan"] or ""))

    prompt = load_system_prompt("skill_from_run")
    if not prompt:
        return {"ok": True, "status": "skipped_prompt_missing"}

    max_skills = int(AGENT_RUN_SKILL_AUTOGEN_MAX_SKILLS or 2)
    if mode == "think":
        max_skills = int(THINK_SKILL_AUTOGEN_MAX_SKILLS or 3)
        if max_skills < int(AGENT_RUN_SKILL_AUTOGEN_MAX_SKILLS or 2):
            max_skills = int(AGENT_RUN_SKILL_AUTOGEN_MAX_SKILLS or 2)

    skill_categories_text = "\n".join(f"- {c}" for c in SKILL_CATEGORY_CHOICES)
    prompt_text = prompt.format(
        skill_categories=skill_categories_text,
        max_skills=max_skills,
        existing_skills=truncate_text(
            json.dumps(existing_skills, ensure_ascii=False),
            AGENT_RUN_SKILL_AUTOGEN_TEXT_SNIPPET_MAX_CHARS,
        ),
        task_title=truncate_text(task_title, 200),
        run_meta=truncate_text(
            json.dumps(run_meta, ensure_ascii=False),
            AGENT_RUN_SKILL_AUTOGEN_TEXT_SNIPPET_MAX_CHARS,
        ),
        plan=truncate_text(
            json.dumps(plan_compact, ensure_ascii=False),
            AGENT_RUN_SKILL_AUTOGEN_TEXT_SNIPPET_MAX_CHARS,
        ),
        steps=truncate_text(
            json.dumps(steps_compact, ensure_ascii=False),
            AGENT_RUN_SKILL_AUTOGEN_TEXT_SNIPPET_MAX_CHARS,
        ),
        outputs=truncate_text(
            json.dumps(outputs_compact, ensure_ascii=False),
            AGENT_RUN_SKILL_AUTOGEN_TEXT_SNIPPET_MAX_CHARS,
        ),
        tool_calls=truncate_text(
            json.dumps(tools_compact, ensure_ascii=False),
            AGENT_RUN_SKILL_AUTOGEN_TEXT_SNIPPET_MAX_CHARS,
        ),
    )

    model_value = (model or "").strip() or resolve_default_model()
    # Think 模式：默认使用 evaluator 模型做技能抽象（更“审查视角”，与 docs/agent 对齐）。
    if mode == "think":
        base_model = str(state_obj.get("model") or "").strip() or model_value
        raw_cfg = state_obj.get("think_config")
        try:
            from backend.src.agent.think import create_think_config_from_dict, get_default_think_config

            think_cfg = (
                create_think_config_from_dict(raw_cfg, base_model=base_model)
                if isinstance(raw_cfg, dict) and raw_cfg
                else get_default_think_config(base_model=base_model)
            )
            evaluator_model = str(getattr(think_cfg, "evaluator_model", "") or "").strip()
            model_value = evaluator_model or base_model
        except Exception:
            model_value = base_model
    params = parameters or {"temperature": 0.2}

    text, _, err = call_openai(prompt_text, model_value, params)
    if err or not text:
        return {"ok": False, "error": f"{ERROR_MESSAGE_LLM_CALL_FAILED}:{err or 'empty_response'}"}

    obj = extract_json_object(text or "")
    if not isinstance(obj, dict):
        return {"ok": True, "status": "skipped_invalid_json"}

    skills_raw = obj.get("skills")
    if not isinstance(skills_raw, list) or not skills_raw:
        return {"ok": True, "status": "no_skills"}

    applied: List[dict] = []
    seen_keys = set()
    for skill in skills_raw[:max_skills]:
        if not isinstance(skill, dict):
            continue
        name = str(skill.get("name") or "").strip()
        category = str(skill.get("category") or "").strip()
        key = f"{name}::{category}"
        if key in seen_keys:
            continue
        seen_keys.add(key)

        skill_id, upsert_status, upsert_err = upsert_skill_from_agent_payload(
            skill,
            task_id=task_id_value,
            run_id=run_id_value,
        )
        source_path = None
        publish_err = None
        if upsert_status in {"created", "updated"} and skill_id:
            source_path, publish_err = publish_skill_file(int(skill_id))

        applied.append(
            {
                "skill_id": int(skill_id) if skill_id else None,
                "status": upsert_status,
                "name": name,
                "category": category,
                "source_path": source_path,
                "error": publish_err or upsert_err,
            }
        )

    return {
        "ok": True,
        "status": "applied" if applied else "no_valid_skills",
        "model": model_value,
        "skills": applied,
        "updated_at": now_iso(),
    }
