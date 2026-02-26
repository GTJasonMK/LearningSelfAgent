import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from backend.src.common.path_utils import is_path_within_root
from backend.src.common.serializers import skill_content_from_row
from backend.src.common.utils import coerce_str_list, extract_json_object, now_iso, parse_json_list, truncate_text
from backend.src.constants import (
    DEFAULT_SKILL_VERSION,
    ERROR_MESSAGE_LLM_CALL_FAILED,
    SKILL_CATEGORY_CHOICES,
    SKILL_DEFAULT_CATEGORY,
)
from backend.src.prompt.paths import skills_prompt_dir
from backend.src.prompt.file_trash import stage_delete_file
from backend.src.prompt.skill_files import build_skill_markdown, write_skill_file
from backend.src.prompt.system_prompts import load_system_prompt
from backend.src.services.llm.llm_client import call_openai
from backend.src.storage import get_connection

logger = logging.getLogger(__name__)


def classify_skill(
    skill_payload: Dict[str, Any],
    model: str,
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    调用 LLM 对技能做“类目/标签/触发词”分类。

    返回：(result, error_message)
    result: {"category": str, "tags": [...], "triggers": [...], "aliases": [...]}
    """
    categories_text = "\n".join(f"- {c}" for c in SKILL_CATEGORY_CHOICES)
    prompt = load_system_prompt("skill_classify")
    if not prompt:
        return {}, "prompt_not_found:skill_classify"

    # 控制 prompt 体积：避免把 outputs/tool stdout 全量塞给分类器
    compact = dict(skill_payload)
    for k in ("outputs", "inputs"):
        if k in compact:
            # 只保留前几项
            items = compact.get(k)
            if isinstance(items, list) and len(items) > 6:
                compact[k] = items[:6]
    if "description" in compact:
        compact["description"] = truncate_text(str(compact.get("description") or ""), 600)
    if "scope" in compact:
        compact["scope"] = truncate_text(str(compact.get("scope") or ""), 400)

    prompt_text = prompt.format(
        categories=categories_text,
        skill=json.dumps(compact, ensure_ascii=False),
    )

    text, _, err = call_openai(prompt_text, model, {"temperature": 0})
    if err or not text:
        return {}, err or ERROR_MESSAGE_LLM_CALL_FAILED
    obj = extract_json_object(text)
    if not isinstance(obj, dict):
        return {}, "skill_classify: invalid_json"

    category = str(obj.get("category") or "").strip()
    if category not in set(SKILL_CATEGORY_CHOICES):
        return {}, "skill_classify: invalid_category"

    tags = coerce_str_list(obj.get("tags"), max_items=12)
    triggers = coerce_str_list(obj.get("triggers"), max_items=18)
    aliases = coerce_str_list(obj.get("aliases"), max_items=6)

    return (
        {
            "category": category,
            "tags": tags,
            "triggers": triggers,
            "aliases": aliases,
        },
        None,
    )


def _skill_meta_from_db_row(row) -> Dict[str, Any]:
    """
    将 skills_items 的 row 转为写文件需要的 meta。
    """
    meta = skill_content_from_row(row)
    meta.pop("source_path", None)
    meta["category"] = meta.get("category") or SKILL_DEFAULT_CATEGORY
    meta["version"] = meta.get("version") or DEFAULT_SKILL_VERSION
    # Phase 2：Solution/Skill 统一格式（便于 files <-> DB 同步）
    meta["domain_id"] = row["domain_id"] if "domain_id" in row.keys() else None
    meta["skill_type"] = row["skill_type"] if "skill_type" in row.keys() else None
    meta["status"] = row["status"] if "status" in row.keys() else None
    meta["source_task_id"] = row["source_task_id"] if "source_task_id" in row.keys() else None
    meta["source_run_id"] = row["source_run_id"] if "source_run_id" in row.keys() else None
    return meta


def publish_skill_file(skill_id: int) -> Tuple[Optional[str], Optional[str]]:
    """
    将 skills_items 记录写入 backend/prompt/skills，并回写 source_path。
    """
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM skills_items WHERE id = ?", (skill_id,)).fetchone()
        if not row:
            return None, "skill_not_found"
        meta = _skill_meta_from_db_row(row)
        category = str(meta.get("category") or "").strip() or SKILL_DEFAULT_CATEGORY

        # 若已有 source_path，优先原地更新，避免“更新技能导致文件路径漂移”。
        existing_source_path = str(row["source_path"] or "").strip()
        if existing_source_path:
            root = skills_prompt_dir().resolve()
            target = (root / Path(existing_source_path)).resolve()
            try:
                if not is_path_within_root(target, root):
                    return None, "invalid_source_path"
                target.parent.mkdir(parents=True, exist_ok=True)
                markdown = build_skill_markdown(meta=meta)
                target.write_text(markdown, encoding="utf-8")
                return existing_source_path.replace("\\", "/"), None
            except Exception as exc:
                return None, f"publish_skill_file overwrite failed: {exc}"

        try:
            source_path = write_skill_file(
                meta=meta,
                category=category,
                filename_hint=str(meta.get("name") or "skill"),
            )
            conn.execute(
                "UPDATE skills_items SET source_path = ? WHERE id = ?",
                (source_path, skill_id),
            )
        except Exception as exc:
            return None, f"publish_skill_file failed: {exc}"
    return source_path, None


def delete_skill_file_by_source_path(source_path: Optional[str]) -> Tuple[bool, Optional[str]]:
    """
    根据 skills_items.source_path 删除技能文件。

    说明：
    - 只删除 skills 目录下的相对路径，避免误删仓库外文件；
    - 若文件不存在，返回 (False, None)（视为“无需删除”）。
    """
    value = str(source_path or "").strip()
    if not value:
        return False, None

    root = skills_prompt_dir().resolve()
    target = (root / Path(value)).resolve()
    if not is_path_within_root(target, root):
        # 兜底：is_relative_to 仅用于防止路径漂移；失败时直接拒绝删除
        return False, "invalid_source_path"

    if not target.exists():
        return False, None
    if target.is_dir():
        return False, "source_path_is_dir"
    try:
        target.unlink()
        return True, None
    except Exception as exc:
        return False, f"delete_skill_file failed: {exc}"


def stage_delete_skill_file_by_source_path(source_path: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    两阶段删除（技能文件）：将 skills_items.source_path 对应的文件移动到 skills/.trash 下。

    返回：(trash_rel_path, error)
    - trash_rel_path=None, error=None 表示文件不存在或 source_path 为空（无需删除）
    - trash_rel_path!=None 表示已暂存到 .trash，可用于失败回滚或最终彻底删除
    """
    value = str(source_path or "").strip()
    if not value:
        return None, None

    root = skills_prompt_dir().resolve()
    target = (root / Path(value)).resolve()
    # 统一路径安全校验（拒绝越界）
    if not is_path_within_root(target, root):
        return None, "invalid_source_path"

    trash_path, err = stage_delete_file(root_dir=root, target_path=target)
    if err:
        return None, err
    if not trash_path:
        return None, None
    try:
        rel = str(trash_path.relative_to(root)).replace("\\", "/")
    except Exception:
        rel = str(trash_path)
    return rel, None


def classify_and_publish_skill(
    skill_id: int,
    model: str,
) -> dict:
    """
    对指定 skill 进行分类，并落盘到文件系统（backend/prompt/skills），同时更新 DB 字段。
    """
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM skills_items WHERE id = ?", (skill_id,)).fetchone()
        if not row:
            return {"ok": False, "error": "skill_not_found"}

        raw_steps = parse_json_list(row["steps"])
        steps_preview: List[str] = []
        for item in (raw_steps or [])[:12]:
            if isinstance(item, dict):
                title = str(item.get("title") or "").strip()
                if title:
                    steps_preview.append(title)
                    continue
                # 兼容不同字段名
                name = str(item.get("name") or "").strip()
                if name:
                    steps_preview.append(name)
                    continue
                continue
            text = str(item).strip()
            if text:
                steps_preview.append(text)

        payload = {
            "name": row["name"],
            "description": row["description"],
            "scope": row["scope"],
            "steps": steps_preview,
            "inputs": parse_json_list(row["inputs"]),
            "outputs": parse_json_list(row["outputs"]),
        }
        classified, err = classify_skill(payload, model=model)
        if err is not None:
            return {"ok": False, "skill_id": skill_id, "error": err, "updated_at": now_iso()}

        category = classified.get("category") or SKILL_DEFAULT_CATEGORY
        tags = classified.get("tags") or []
        triggers = classified.get("triggers") or []
        aliases = classified.get("aliases") or []
        conn.execute(
            "UPDATE skills_items SET category = ?, tags = ?, triggers = ?, aliases = ? WHERE id = ?",
            (
                str(category),
                json.dumps(tags, ensure_ascii=False),
                json.dumps(triggers, ensure_ascii=False),
                json.dumps(aliases, ensure_ascii=False),
                skill_id,
            ),
        )

    source_path, publish_err = publish_skill_file(skill_id)
    return {
        "ok": publish_err is None,
        "skill_id": skill_id,
        "category": category,
        "tags": tags,
        "triggers": triggers,
        "aliases": aliases,
        "source_path": source_path,
        "error": publish_err,
        "updated_at": now_iso(),
    }
