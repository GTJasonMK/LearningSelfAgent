import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from backend.src.common.utils import now_iso
from backend.src.prompt.paths import skills_prompt_dir
from backend.src.prompt.skill_files import (
    discover_skill_markdown_files,
    normalize_skill_meta,
    parse_skill_markdown,
)
from backend.src.storage import get_connection

logger = logging.getLogger(__name__)


def _category_from_source_path(source_path: str) -> str:
    # skills/tool/web/xxx.md -> tool.web
    parts = [p for p in (source_path or "").split("/") if p]
    if len(parts) <= 1:
        return "misc"
    # 去掉文件名
    dirs = parts[:-1]
    # 仅取前两级即可，避免类目无限增长（可按需调整）
    dirs = dirs[:2]
    return ".".join(dirs) if dirs else "misc"


def _json_list(value: Optional[List[Any]]) -> str:
    return json.dumps(value or [], ensure_ascii=False)


def sync_skills_from_files(base_dir: Optional[Path] = None, *, prune: bool = True) -> dict:
    """
    将 backend/prompt/skills 下的技能文件同步到 SQLite（skills_items）。

    规则：
    - 以 source_path 作为主键（相对 skills 目录的路径）
    - frontmatter.category 缺失时，从目录推断 category
    - prune=True 时强一致删除：若 DB 中存在 source_path，但文件系统已不存在，则删除该 DB 记录
    """
    base_dir = base_dir or skills_prompt_dir()
    errors: List[str] = []
    items = []

    # 重要：强一致删除需要“文件存在集合”，但 parse 失败不应误删 DB。
    # 因此这里单独维护 discovered_source_paths，并且无论 parse 是否成功都纳入集合。
    rel_root = base_dir
    discovered_source_paths = set()
    for path in discover_skill_markdown_files(base_dir=base_dir):
        try:
            rel = str(path.relative_to(rel_root)).replace("\\", "/")
        except ValueError:
            rel = path.name
        discovered_source_paths.add(rel)
        try:
            text = path.read_text(encoding="utf-8")
            items.append(parse_skill_markdown(text=text, source_path=rel))
        except Exception as exc:
            errors.append(f"{path}: {exc}")
            continue

    inserted = 0
    updated = 0
    skipped = 0
    deleted = 0

    with get_connection() as conn:
        for item in items:
            meta = normalize_skill_meta(item.meta or {})
            name = str(meta.get("name") or "").strip()
            if not name:
                skipped += 1
                continue
            category = str(meta.get("category") or "").strip() or _category_from_source_path(item.source_path)

            row = conn.execute(
                "SELECT id FROM skills_items WHERE source_path = ? LIMIT 1",
                (item.source_path,),
            ).fetchone()

            payload = {
                "name": name,
                "description": meta.get("description"),
                "scope": meta.get("scope"),
                "category": category,
                "tags": _json_list(meta.get("tags")),
                "triggers": _json_list(meta.get("triggers")),
                "aliases": _json_list(meta.get("aliases")),
                "prerequisites": _json_list(meta.get("prerequisites")),
                "inputs": _json_list(meta.get("inputs")),
                "outputs": _json_list(meta.get("outputs")),
                "steps": _json_list(meta.get("steps")),
                "failure_modes": _json_list(meta.get("failure_modes")),
                "validation": _json_list(meta.get("validation")),
                "version": meta.get("version"),
                "source_path": item.source_path,
                # Phase 2：用于区分 methodology/solution 以及归属领域
                "domain_id": meta.get("domain_id"),
                "skill_type": meta.get("skill_type"),
                "status": meta.get("status"),
                "source_task_id": meta.get("source_task_id"),
                "source_run_id": meta.get("source_run_id"),
            }

            if row:
                conn.execute(
                    """
                    UPDATE skills_items
                    SET name = ?,
                        description = ?,
                        scope = ?,
                        category = ?,
                        tags = ?,
                        triggers = ?,
                        aliases = ?,
                        source_path = ?,
                        prerequisites = ?,
                        inputs = ?,
                        outputs = ?,
                        steps = ?,
                        failure_modes = ?,
                        validation = ?,
                        version = ?,
                        domain_id = ?,
                        skill_type = ?,
                        status = ?,
                        source_task_id = ?,
                        source_run_id = ?
                    WHERE id = ?
                    """,
                    (
                        payload["name"],
                        payload["description"],
                        payload["scope"],
                        payload["category"],
                        payload["tags"],
                        payload["triggers"],
                        payload["aliases"],
                        payload["source_path"],
                        payload["prerequisites"],
                        payload["inputs"],
                        payload["outputs"],
                        payload["steps"],
                        payload["failure_modes"],
                        payload["validation"],
                        payload["version"],
                        payload["domain_id"],
                        payload["skill_type"],
                        payload["status"],
                        payload["source_task_id"],
                        payload["source_run_id"],
                        row["id"],
                    ),
                )
                updated += 1
            else:
                created_at = now_iso()
                conn.execute(
                    "INSERT INTO skills_items (name, created_at, description, scope, category, tags, triggers, aliases, source_path, prerequisites, inputs, outputs, steps, failure_modes, validation, version, task_id, domain_id, skill_type, status, source_task_id, source_run_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        payload["name"],
                        created_at,
                        payload["description"],
                        payload["scope"],
                        payload["category"],
                        payload["tags"],
                        payload["triggers"],
                        payload["aliases"],
                        payload["source_path"],
                        payload["prerequisites"],
                        payload["inputs"],
                        payload["outputs"],
                        payload["steps"],
                        payload["failure_modes"],
                        payload["validation"],
                        payload["version"],
                        None,
                        payload["domain_id"],
                        payload["skill_type"],
                        payload["status"],
                        payload["source_task_id"],
                        payload["source_run_id"],
                    ),
                )
                inserted += 1

        # prune：删除“文件已不存在”的 DB 记录（仅对有 source_path 的技能生效）
        if prune:
            try:
                rows = conn.execute(
                    "SELECT id, source_path FROM skills_items WHERE source_path IS NOT NULL AND source_path != ''",
                ).fetchall()
                for row in rows:
                    sp = str(row["source_path"] or "").replace("\\", "/").strip()
                    if not sp:
                        continue
                    if sp in discovered_source_paths:
                        continue
                    conn.execute("DELETE FROM skills_items WHERE id = ?", (int(row["id"]),))
                    deleted += 1
            except Exception as exc:
                errors.append(f"prune_failed: {exc}")

    if errors:
        for err in errors[:5]:
            logger.warning("skill file load error: %s", err)

    return {
        "base_dir": str(base_dir),
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "deleted": deleted,
        "prune": bool(prune),
        "errors": errors,
    }
