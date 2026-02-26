from typing import Any, Dict, List, Optional, Tuple

from backend.src.common.utils import (
    bump_semver_patch,
    coerce_str_list,
    dedupe_keep_order,
    now_iso,
    parse_json_list,
)
from backend.src.constants import (
    DEFAULT_SKILL_VERSION,
    SKILL_CATEGORY_CHOICES,
    SKILL_DEFAULT_CATEGORY,
    SKILL_SCOPE_TOOL_PREFIX,
)
from backend.src.repositories.skills_repo import (
    SkillCreateParams,
    create_skill,
    get_skill,
    update_skill,
    update_skill_status,
)
from backend.src.services.knowledge.skill_tag_policy import normalize_skill_tags
from backend.src.storage import get_connection

_SKILL_LIST_FIELDS = (
    "tags",
    "triggers",
    "aliases",
    "prerequisites",
    "inputs",
    "outputs",
    "steps",
    "failure_modes",
    "validation",
)


def _coerce_any_list(value: Any, max_items: int = 64) -> List[Any]:
    """
    inputs/outputs/steps 允许 dict 或 str 等结构化对象：只保证是 list。
    """
    if value is None:
        return []
    if isinstance(value, list):
        return value[:max_items]
    return [value]


def _normalize_category(category: Optional[str]) -> str:
    value = str(category or "").strip()
    if value in set(SKILL_CATEGORY_CHOICES):
        return value
    return SKILL_DEFAULT_CATEGORY


def _skill_type_where_params(skill_type_value: str) -> Tuple[str, List[Any]]:
    if skill_type_value == "methodology":
        return "(skill_type = 'methodology' OR skill_type IS NULL)", []
    return "skill_type = ?", [skill_type_value]


def _select_skill_id_by_where(
    conn,
    *,
    where_prefix: str,
    where_params: List[Any],
    skill_type_value: str,
) -> Optional[int]:
    skill_type_where, skill_type_params = _skill_type_where_params(skill_type_value)
    row = conn.execute(
        f"SELECT id FROM skills_items WHERE {where_prefix} AND {skill_type_where} ORDER BY id ASC LIMIT 1",
        (*where_params, *skill_type_params),
    ).fetchone()
    return int(row["id"]) if row else None


def _normalize_skill_list_fields(values: Dict[str, List[Any]]) -> Dict[str, List[Any]]:
    """
    对技能 list 字段做统一去重，避免 create/update 路径重复实现。
    """
    out: Dict[str, List[Any]] = {}
    for field in _SKILL_LIST_FIELDS:
        out[field] = dedupe_keep_order(list(values.get(field) or []))
    return out


def _merge_skill_row_list_fields(row, incoming: Dict[str, List[Any]]) -> Dict[str, List[Any]]:
    """
    合并 DB 旧值与请求新值，并统一去重。
    """
    merged: Dict[str, List[Any]] = {}
    for field in _SKILL_LIST_FIELDS:
        merged[field] = dedupe_keep_order(parse_json_list(row[field]) + list(incoming.get(field) or []))
    return merged


def _find_existing_skill_id(
    *,
    name: str,
    category: str,
    scope: Optional[str],
    skill_type: str,
    domain_id: Optional[str],
) -> Optional[int]:
    """
    最小去重策略（按优先级）：
    1) scope 唯一（例如 tool:{tool_id}）
    2) name（优先同 domain_id + category，再回退到 name 全局）
    """
    scope_value = str(scope or "").strip() or None
    skill_type_value = str(skill_type or "").strip().lower() or "methodology"
    domain_value = str(domain_id or "").strip() or None
    with get_connection() as conn:
        if scope_value:
            found_id = _select_skill_id_by_where(
                conn,
                where_prefix="scope = ?",
                where_params=[scope_value],
                skill_type_value=skill_type_value,
            )
            if found_id is not None:
                return found_id

        # 兼容旧策略：优先 name + category
        found_id = _select_skill_id_by_where(
            conn,
            where_prefix="name = ? AND category = ?",
            where_params=[name, category],
            skill_type_value=skill_type_value,
        )
        if found_id is not None:
            return found_id

        # 新策略：同名技能更倾向于“版本合并”，避免不同 category 产生重复知识
        if domain_value:
            found_id = _select_skill_id_by_where(
                conn,
                where_prefix="name = ? AND domain_id = ?",
                where_params=[name, domain_value],
                skill_type_value=skill_type_value,
            )
            if found_id is not None:
                return found_id

        return _select_skill_id_by_where(
            conn,
            where_prefix="name = ?",
            where_params=[name],
            skill_type_value=skill_type_value,
        )


def upsert_skill_from_agent_payload(
    skill: Dict[str, Any],
    *,
    task_id: Optional[int],
    run_id: Optional[int],
) -> Tuple[Optional[int], str, Optional[str]]:
    """
    由 Agent（Eval/后处理）产出的 skill payload 写入/更新 skills_items。

    返回：(skill_id, status, error)
    - status: created/updated/skipped/invalid
    """
    if not isinstance(skill, dict):
        return None, "invalid", "skill 不是对象"

    mode = str(skill.get("mode") or "").strip().lower()
    if mode in {"skip", "ignored"}:
        return None, "skipped", None

    name = str(skill.get("name") or "").strip()
    if not name:
        return None, "invalid", "skill.name 不能为空"

    scope = str(skill.get("scope") or "").strip() or None
    category = _normalize_category(skill.get("category"))
    description = str(skill.get("description") or "").strip() or None

    # Phase2：domain/skill_type/status/source 溯源
    domain_id = str(skill.get("domain_id") or "").strip() or "misc"
    skill_type = str(skill.get("skill_type") or "").strip().lower()
    if not skill_type:
        # 约定：Solution 不由本 upsert 生成；但允许兼容旧数据写入
        if scope and scope.startswith("solution:"):
            skill_type = "solution"
        else:
            skill_type = "methodology"
    if skill_type not in {"methodology", "solution"}:
        skill_type = "methodology"

    status_value = str(skill.get("status") or "").strip().lower() or "approved"
    if status_value not in {"draft", "approved", "deprecated", "abandoned"}:
        status_value = "approved"

    tags = coerce_str_list(skill.get("tags"))
    triggers = coerce_str_list(skill.get("triggers"))
    aliases = coerce_str_list(skill.get("aliases"))
    prerequisites = coerce_str_list(skill.get("prerequisites"))

    inputs = _coerce_any_list(skill.get("inputs"))
    outputs = _coerce_any_list(skill.get("outputs"))
    steps = _coerce_any_list(skill.get("steps"))
    failure_modes = coerce_str_list(skill.get("failure_modes"))
    validation = coerce_str_list(skill.get("validation"))

    # 关联标签规范（docs/agent）：保持最小且可审计的溯源标签
    if domain_id:
        tags.append(f"domain:{domain_id}")
    if task_id is not None:
        tags.append(f"task:{int(task_id)}")
    if run_id is not None:
        tags.append(f"run:{int(run_id)}")
    if scope and scope.startswith(SKILL_SCOPE_TOOL_PREFIX):
        try:
            tool_id = int(scope[len(SKILL_SCOPE_TOOL_PREFIX) :])
        except Exception:
            tool_id = None
        if isinstance(tool_id, int) and tool_id > 0:
            tags.append(f"tool:{tool_id}")

    # tags 规范化（避免 LLM 输出污染 tags 索引）
    tags, _issues = normalize_skill_tags(tags, strict_keys=False)
    incoming_lists = _normalize_skill_list_fields(
        {
            "tags": tags,
            "triggers": triggers,
            "aliases": aliases,
            "prerequisites": prerequisites,
            "inputs": inputs,
            "outputs": outputs,
            "steps": steps,
            "failure_modes": failure_modes,
            "validation": validation,
        }
    )

    existing_id = _find_existing_skill_id(
        name=name,
        category=category,
        scope=scope,
        skill_type=skill_type,
        domain_id=domain_id,
    )
    if existing_id is None:
        created_at = now_iso()
        skill_id = create_skill(
            SkillCreateParams(
                name=name,
                description=description,
                scope=scope,
                category=category,
                tags=incoming_lists["tags"],
                triggers=incoming_lists["triggers"],
                aliases=incoming_lists["aliases"],
                source_path=None,
                prerequisites=incoming_lists["prerequisites"],
                inputs=incoming_lists["inputs"],
                outputs=incoming_lists["outputs"],
                steps=incoming_lists["steps"],
                failure_modes=incoming_lists["failure_modes"],
                validation=incoming_lists["validation"],
                version=str(skill.get("version") or DEFAULT_SKILL_VERSION).strip() or DEFAULT_SKILL_VERSION,
                task_id=task_id,
                created_at=created_at,
                domain_id=domain_id,
                skill_type=skill_type,
                status=status_value,
                source_task_id=int(task_id) if task_id is not None else None,
                source_run_id=int(run_id) if run_id is not None else None,
            )
        )
        return int(skill_id), "created", None

    row = get_skill(skill_id=int(existing_id))
    if not row:
        return None, "invalid", "skill_not_found_after_lookup"

    merged = {
        "description": description or (row["description"] or None),
        "scope": scope or (row["scope"] or None),
        "category": category or (row["category"] or SKILL_DEFAULT_CATEGORY),
        **_merge_skill_row_list_fields(row, incoming_lists),
    }

    next_version = bump_semver_patch(
        row["version"] or DEFAULT_SKILL_VERSION,
        default_version=DEFAULT_SKILL_VERSION,
    )
    existing_task_id = row["task_id"]
    if existing_task_id is None and task_id is not None:
        existing_task_id = task_id

    existing_domain_id = str(row["domain_id"] or "").strip() if row["domain_id"] is not None else ""
    next_domain_id = domain_id or existing_domain_id or "misc"

    existing_status = str(row["status"] or "").strip().lower() if row["status"] is not None else ""
    next_status = status_value
    if existing_status and existing_status in {"deprecated", "abandoned"} and status_value == "approved":
        # 允许通过 upsert 重新启用（后续可由人工再废弃）
        next_status = "approved"
    elif existing_status:
        # 默认保持现有状态（避免误把 deprecated 直接改回 approved）
        next_status = existing_status

    existing_source_task_id = row["source_task_id"]
    existing_source_run_id = row["source_run_id"]
    if existing_source_task_id is None and task_id is not None:
        existing_source_task_id = int(task_id)
    if existing_source_run_id is None and run_id is not None:
        existing_source_run_id = int(run_id)

    updated = update_skill(
        skill_id=int(existing_id),
        name=name,
        description=merged["description"],
        scope=merged["scope"],
        category=merged["category"],
        tags=merged["tags"],
        triggers=merged["triggers"],
        aliases=merged["aliases"],
        prerequisites=merged["prerequisites"],
        inputs=merged["inputs"],
        outputs=merged["outputs"],
        steps=merged["steps"],
        failure_modes=merged["failure_modes"],
        validation=merged["validation"],
        version=next_version,
        task_id=existing_task_id,
        domain_id=next_domain_id,
        skill_type=skill_type,
        status=next_status,
        source_task_id=int(existing_source_task_id) if existing_source_task_id is not None else None,
        source_run_id=int(existing_source_run_id) if existing_source_run_id is not None else None,
    )
    if not updated:
        return None, "invalid", "update_failed"

    # status 更新独立门闩：若 payload 指定了明确状态，则强制写入
    if status_value and status_value != next_status:
        try:
            _ = update_skill_status(skill_id=int(existing_id), status=status_value)
        except Exception:
            pass

    return int(existing_id), "updated", None
