import json
from typing import Any, Dict, List, Optional, Tuple

from backend.src.actions.registry import normalize_action_type
from backend.src.common.utils import (
    action_type_from_step_detail,
    bump_semver_patch,
    extract_json_object,
    now_iso,
    parse_json_list,
    truncate_text,
)
from backend.src.constants import AGENT_TASK_FEEDBACK_STEP_TITLE, DEFAULT_SKILL_VERSION
from backend.src.repositories.skills_repo import (
    SkillCreateParams,
    create_skill,
    update_skill,
    update_skill_status,
)
from backend.src.repositories.task_runs_repo import get_task_run
from backend.src.repositories.task_steps_repo import list_task_steps_for_run
from backend.src.repositories.tool_call_records_repo import list_tool_calls_with_tool_name_by_run
from backend.src.services.skills.skills_publish import publish_skill_file
from backend.src.storage import get_connection


def _coerce_int_list(value: Any, limit: int = 32) -> List[int]:
    out: List[int] = []
    seen = set()
    if not isinstance(value, list):
        return out
    for item in value:
        try:
            iv = int(item)
        except Exception:
            continue
        if iv <= 0 or iv in seen:
            continue
        seen.add(iv)
        out.append(iv)
        if len(out) >= limit:
            break
    return out


def _extract_action_type_from_step_detail(detail_text: Optional[str]) -> Optional[str]:
    return action_type_from_step_detail(detail_text)


def _infer_action_type_from_title(title: Optional[str]) -> Optional[str]:
    """
    尝试从 step title 推断 action.type（兼容早期未落 detail 的数据）。
    约定：title 以 `action_type:` 作为前缀，例如 `file_write:xxx`。
    """
    raw = str(title or "").strip()
    if not raw or ":" not in raw:
        return None
    prefix = raw.split(":", 1)[0]
    return normalize_action_type(prefix)


def _load_agent_plan_and_state(*, run_id: int) -> Tuple[dict, dict]:
    run_row = get_task_run(run_id=int(run_id))
    if not run_row:
        return {}, {}
    plan_obj = extract_json_object(run_row["agent_plan"] or "") if run_row else None
    if not isinstance(plan_obj, dict):
        plan_obj = {}
    state_obj = extract_json_object(run_row["agent_state"] or "") if run_row else None
    if not isinstance(state_obj, dict):
        state_obj = {}
    return plan_obj, state_obj


def _find_solution_row_by_run_id(*, run_id: int):
    """
    查询同一 run 的 solution 记录：
    - 优先返回 draft（用于“草稿方案 → 实际执行覆盖 → approved”的闭环）
    - 若无 draft，再返回最新的一条
    """
    try:
        rid = int(run_id)
    except Exception:
        return None
    if rid <= 0:
        return None
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM skills_items WHERE skill_type = 'solution' AND source_run_id = ? AND status = 'draft' ORDER BY id DESC LIMIT 1",
            (rid,),
        ).fetchone()
        if row:
            return row
        return conn.execute(
            "SELECT * FROM skills_items WHERE skill_type = 'solution' AND source_run_id = ? ORDER BY id DESC LIMIT 1",
            (rid,),
        ).fetchone()


def find_solution_id_by_run_id(*, run_id: int) -> Optional[int]:
    """
    以 source_run_id 作为幂等键：一个 run 最多生成一条 solution（避免重复沉淀）。
    """
    try:
        rid = int(run_id)
    except Exception:
        return None
    if rid <= 0:
        return None
    row = _find_solution_row_by_run_id(run_id=rid)
    if not row:
        return None
    try:
        return int(row["id"])
    except Exception:
        return None


def autogen_solution_from_run(
    *,
    task_id: int,
    run_id: int,
    force: bool = False,
) -> dict:
    """
    从一次 Agent run 的执行记录中生成“方案（Solution）”，并写入：
    - skills_items（skill_type='solution'）
    - backend/prompt/skills（发布 .md 文件，作为可编辑/可恢复的“灵魂存档”）

    设计目标：
    - 不依赖外部 LLM：即使未配置模型，也能生成最小可检索的方案；
    - 以 source_run_id 做幂等：默认每个 run 只生成一条方案。
    """
    tid = int(task_id)
    rid = int(run_id)
    if tid <= 0 or rid <= 0:
        return {"ok": False, "error": "invalid_task_or_run_id"}

    # 仅为 Agent run 生成方案，避免把普通 task/run 混入“方案知识库”
    run_row = get_task_run(run_id=rid)
    if not run_row:
        return {"ok": False, "error": "run_not_found", "run_id": rid}
    run_summary = str(run_row["summary"] or "").strip()
    if not run_summary.startswith("agent_"):
        return {"ok": True, "status": "skipped_not_agent_run", "run_id": rid, "summary": run_summary}

    existing_row = _find_solution_row_by_run_id(run_id=rid)
    existing_id = None
    existing_status = ""
    existing_scope = ""
    existing_tags: List[str] = []
    if existing_row:
        try:
            existing_id = int(existing_row["id"])
        except Exception:
            existing_id = None
        existing_status = str(existing_row["status"] or "").strip().lower()
        existing_scope = str(existing_row["scope"] or "").strip()
        try:
            existing_tags = [str(x) for x in (parse_json_list(existing_row["tags"]) or [])]
        except Exception:
            existing_tags = []

    # docs/agent：Create 流程 A
    # - 规划阶段可能已创建 draft solution；后处理阶段需要用真实执行记录覆盖并升级为 approved
    # - 如果是“旧 bug/遗留数据”导致 draft solution 被提前标记为 approved，也应视为可覆盖对象
    existing_is_draft_like = bool(
        existing_row
        and (
            existing_status == "draft"
            or existing_scope.startswith("solution:draft")
            or ("draft_solution" in set(existing_tags))
        )
    )

    if existing_id is not None and not bool(force) and not existing_is_draft_like:
        return {"ok": True, "status": "exists", "skill_id": existing_id, "run_id": rid}

    plan_obj = extract_json_object(run_row["agent_plan"] or "") if run_row else None
    if not isinstance(plan_obj, dict):
        plan_obj = {}
    state_obj = extract_json_object(run_row["agent_state"] or "") if run_row else None
    if not isinstance(state_obj, dict):
        state_obj = {}

    mode = str(state_obj.get("mode") or "").strip().lower() or "do"
    domain_ids = state_obj.get("domain_ids")
    domain_id = "misc"
    if isinstance(domain_ids, list) and domain_ids:
        domain_id = str(domain_ids[0] or "").strip() or "misc"

    message = str(state_obj.get("message") or "").strip()
    skill_ids = _coerce_int_list(state_obj.get("skill_ids"), limit=24)
    solution_ids = _coerce_int_list(state_obj.get("solution_ids"), limit=12)
    # 避免“自引用”噪音：当复用/覆盖同一条 solution 记录时，不应把自身写入 ref_solution 标签
    if existing_id is not None and solution_ids:
        try:
            eid = int(existing_id)
        except Exception:
            eid = None
        if isinstance(eid, int) and eid > 0:
            solution_ids = [sid for sid in solution_ids if int(sid) != eid]

    plan_artifacts = plan_obj.get("artifacts")
    artifacts: List[str] = []
    if isinstance(plan_artifacts, list):
        for item in plan_artifacts:
            text = str(item or "").strip()
            if text:
                artifacts.append(text)
            if len(artifacts) >= 50:
                break

    # 1) 采样执行步骤（来自 task_steps）：尽量保留“来源可追溯”的最小结构
    step_rows = list_task_steps_for_run(task_id=int(tid), run_id=int(rid))
    steps: List[dict] = []
    for row in step_rows or []:
        title = str(row.get("title") or "").strip() if isinstance(row, dict) else str(row["title"] or "").strip()
        if not title:
            continue
        if title == AGENT_TASK_FEEDBACK_STEP_TITLE:
            # 反馈步骤不属于“方案执行流程”
            continue
        detail_text = None
        if isinstance(row, dict):
            detail_text = row.get("detail")
        else:
            detail_text = row["detail"]
        action_type = _extract_action_type_from_step_detail(detail_text) or _infer_action_type_from_title(title)
        action_type = normalize_action_type(action_type) if action_type else None
        allow: List[str] = [action_type] if action_type else []
        steps.append({"title": title, "allow": allow})
        if len(steps) >= 80:
            break

    # 2) 工具调用（来自 tool_call_records）：用于 tags 关联与后续“按方案提取工具”
    tool_rows = list_tool_calls_with_tool_name_by_run(run_id=rid, limit=60)
    tool_ids: List[int] = []
    tool_names: List[str] = []
    seen_tool_ids = set()
    seen_tool_names = set()
    for row in tool_rows or []:
        try:
            tool_id = int(row.get("tool_id")) if isinstance(row, dict) else int(row["tool_id"])
        except Exception:
            tool_id = None
        if tool_id and tool_id > 0 and tool_id not in seen_tool_ids:
            seen_tool_ids.add(tool_id)
            tool_ids.append(tool_id)
        tool_name = str((row.get("tool_name") if isinstance(row, dict) else row["tool_name"]) or "").strip()
        if tool_name and tool_name not in seen_tool_names:
            seen_tool_names.add(tool_name)
            tool_names.append(tool_name)
        if len(tool_ids) >= 24 and len(tool_names) >= 24:
            break

    # tags：用于检索与溯源
    tags: List[str] = [
        "solution",
        f"task:{tid}",
        f"run:{rid}",
        f"mode:{mode}",
        f"domain:{domain_id}",
    ]
    for sid in skill_ids:
        tags.append(f"skill:{sid}")
    for sol_id in solution_ids:
        tags.append(f"ref_solution:{sol_id}")
    for tool_id in tool_ids:
        tags.append(f"tool:{tool_id}")
    for name in tool_names[:12]:
        tags.append(f"tool_name:{name}")

    # 生成名称：尽量可读，避免过长
    base_name = message or f"task#{tid}"
    base_name = truncate_text(base_name.replace("\r", " ").replace("\n", " ").strip(), 60)
    name = f"{base_name}-方案#{rid}"

    description = truncate_text(message or base_name, 400) if message or base_name else None

    # outputs：复用 artifacts（方案的典型“产出”）
    outputs: List[Any] = artifacts
    if not outputs and tool_names:
        outputs = [f"调用工具: {', '.join(tool_names[:8])}"]

    # 若已存在同 run 的 solution：
    # - draft：覆盖并升级（与 docs/agent 对齐）
    # - approved：默认跳过；force=True 则覆盖更新（避免重复插入）
    if existing_id is not None and (bool(force) or existing_is_draft_like):
        next_version = bump_semver_patch(
            str(existing_row["version"] or DEFAULT_SKILL_VERSION),
            default_version=str(DEFAULT_SKILL_VERSION or "0.1.0"),
        )
        updated = update_skill(
            skill_id=int(existing_id),
            name=name,
            description=description,
            scope=f"solution:run:{rid}",
            category="solution",
            tags=tags,
            triggers=[],
            aliases=[],
            prerequisites=[],
            inputs=[truncate_text(message, 400)] if message else [],
            outputs=outputs,
            steps=steps,
            failure_modes=[],
            validation=[],
            version=next_version,
            task_id=tid,
            domain_id=domain_id,
            skill_type="solution",
            status="draft" if existing_is_draft_like else "approved",
            source_task_id=tid,
            source_run_id=rid,
        )
        if not updated:
            return {"ok": False, "error": "update_existing_failed", "skill_id": int(existing_id), "run_id": rid}

        _ = update_skill_status(skill_id=int(existing_id), status="approved")
        source_path, publish_err = publish_skill_file(int(existing_id))
        if publish_err:
            return {
                "ok": False,
                "error": f"publish_failed:{publish_err}",
                "skill_id": int(existing_id),
                "run_id": rid,
            }

        return {
            "ok": True,
            "status": "upgraded" if existing_is_draft_like else "updated",
            "skill_id": int(existing_id),
            "run_id": rid,
            "domain_id": domain_id,
            "source_path": source_path,
        }

    created_at = now_iso()
    params = SkillCreateParams(
        name=name,
        description=description,
        scope=f"solution:run:{rid}",
        category="solution",
        tags=tags,
        triggers=[],
        aliases=[],
        prerequisites=[],
        inputs=[truncate_text(message, 400)] if message else [],
        outputs=outputs,
        steps=steps,
        failure_modes=[],
        validation=[],
        version=str(DEFAULT_SKILL_VERSION or "0.1.0"),
        task_id=tid,
        created_at=created_at,
        domain_id=domain_id,
        skill_type="solution",
        status="approved",
        source_task_id=tid,
        source_run_id=rid,
    )
    skill_id = create_skill(params)

    source_path, publish_err = publish_skill_file(int(skill_id))
    if publish_err:
        return {
            "ok": False,
            "error": f"publish_failed:{publish_err}",
            "skill_id": int(skill_id),
            "run_id": rid,
        }

    return {
        "ok": True,
        "status": "created",
        "skill_id": int(skill_id),
        "run_id": rid,
        "domain_id": domain_id,
        "source_path": source_path,
    }
