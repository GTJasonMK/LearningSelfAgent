from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from backend.src.common.utils import atomic_write_text, now_iso
from backend.src.constants import DEFAULT_TOOL_VERSION
from backend.src.prompt.file_trash import stage_delete_file
from backend.src.prompt.paths import tools_prompt_dir
from backend.src.prompt.skill_files import parse_skill_markdown
from backend.src.prompt.skill_files import slugify_filename
from backend.src.storage import get_connection

logger = logging.getLogger(__name__)

_FRONTMATTER_DELIM = "---"


def _build_tool_markdown(meta: Dict[str, Any], body: Optional[str] = None) -> str:
    """
    生成 tool Markdown（JSON frontmatter + 正文）。

    说明：
    - 使用 JSON frontmatter，避免强依赖 PyYAML；
    - 正文主要用于人工补充说明（可为空）。
    """
    fm_text = json.dumps(meta, ensure_ascii=False, indent=2, sort_keys=True).strip()
    body_text = str(body or "").rstrip()
    lines = [_FRONTMATTER_DELIM, fm_text, _FRONTMATTER_DELIM, "", body_text, ""]
    return "\n".join(lines)


def _tools_base_dir(base_dir: Optional[Path] = None) -> Path:
    return base_dir or tools_prompt_dir()


def tool_file_path_from_source_path(source_path: str, base_dir: Optional[Path] = None) -> Path:
    return _tools_base_dir(base_dir) / Path(str(source_path or "").strip())


def _safe_json_obj(value: Any) -> Optional[dict]:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            out = json.loads(value)
            return out if isinstance(out, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def publish_tool_file(
    tool_id: int,
    *,
    conn=None,
    base_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    将 tools_items 的一条记录写入文件系统（backend/prompt/tools）。

    规则：
    - 若 tools_items.source_path 已存在：优先原地覆盖（保持路径稳定）；
    - 否则生成默认文件名，并回写 source_path。
    """
    if conn is None:
        with get_connection() as inner:
            return publish_tool_file(int(tool_id), conn=inner, base_dir=base_dir)

    row = conn.execute("SELECT * FROM tools_items WHERE id = ? LIMIT 1", (int(tool_id),)).fetchone()
    if not row:
        return {"ok": False, "error": "tool_not_found"}

    meta = {
        "id": int(row["id"]),
        "name": row["name"],
        "description": row["description"],
        "version": row["version"],
        "metadata": _safe_json_obj(row["metadata"]) if row["metadata"] else None,
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "last_used_at": row["last_used_at"],
    }

    existing_source_path = str(row["source_path"] or "").strip()
    if existing_source_path:
        target = tool_file_path_from_source_path(existing_source_path, base_dir=base_dir).resolve()
        root = _tools_base_dir(base_dir).resolve()
        try:
            if not target.is_relative_to(root):
                return {"ok": False, "error": "invalid_source_path"}
        except Exception:
            return {"ok": False, "error": "invalid_source_path"}
        markdown = _build_tool_markdown(meta=meta, body="")
        atomic_write_text(target, markdown, encoding="utf-8")
        return {"ok": True, "source_path": existing_source_path.replace("\\", "/"), "path": str(target)}

    # 新建：默认用 name + id 生成，避免同名冲突/重命名导致漂移
    filename = f"{slugify_filename(str(row['name'] or 'tool'))}_{int(row['id'])}.md"
    rel = filename
    target = (_tools_base_dir(base_dir) / filename).resolve()
    markdown = _build_tool_markdown(meta=meta, body="")
    atomic_write_text(target, markdown, encoding="utf-8")
    conn.execute("UPDATE tools_items SET source_path = ? WHERE id = ?", (rel, int(tool_id)))
    return {"ok": True, "source_path": rel.replace("\\", "/"), "path": str(target)}


def stage_delete_tool_file_by_source_path(source_path: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    两阶段删除（工具文件）：将 tools_items.source_path 对应文件移动到 tools/.trash 下。
    """
    value = str(source_path or "").strip()
    if not value:
        return None, None

    root = tools_prompt_dir().resolve()
    target = tool_file_path_from_source_path(value).resolve()
    try:
        if not target.is_relative_to(root):
            return None, "invalid_source_path"
    except Exception:
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


def _discover_tool_files(base_dir: Path) -> List[Path]:
    if not base_dir.exists():
        return []
    files: List[Path] = []
    for path in base_dir.rglob("*.md"):
        if not path.is_file():
            continue
        name = path.name.lower()
        if name in {"readme.md", "_readme.md"}:
            continue
        # 跳过隐藏目录/文件（例如 .trash）
        if any(part.startswith(".") for part in path.parts):
            continue
        files.append(path)
    return sorted(files)


def sync_tools_from_files(base_dir: Optional[Path] = None, *, prune: bool = True) -> dict:
    """
    将 backend/prompt/tools 下的工具文件同步到 SQLite（tools_items）。

    规则：
    - 以 source_path（相对 tools 目录的路径）作为主键
    - 若 meta.id 存在，则优先用 id 定位（用于“保留 tool_id，避免 skills.scope=tool:{id} 失效”）
    - prune=True：DB 中 source_path 存在但文件不存在 -> 删除 DB 记录（强一致删除）
    - 会自动“补齐未跟踪的历史 DB 工具”：source_path 为空的行会落盘文件（便于后续恢复）
    """
    base = base_dir or tools_prompt_dir()
    base.mkdir(parents=True, exist_ok=True)

    errors: List[str] = []
    discovered_source_paths = set()
    parsed_items: List[Tuple[str, Dict[str, Any]]] = []

    for path in _discover_tool_files(base):
        try:
            rel = str(path.relative_to(base)).replace("\\", "/")
        except Exception:
            rel = path.name
        discovered_source_paths.add(rel)
        try:
            text = path.read_text(encoding="utf-8")
            parsed = parse_skill_markdown(text=text, source_path=rel)
            meta = parsed.meta or {}
            parsed_items.append((rel, meta))
        except Exception as exc:
            errors.append(f"{path}: {exc}")
            continue

    inserted = 0
    updated = 0
    deleted = 0
    published = 0

    with get_connection() as conn:
        # 1) 文件 -> DB upsert
        for source_path, meta in parsed_items:
            name = str(meta.get("name") or meta.get("title") or "").strip()
            if not name:
                continue
            description = str(meta.get("description") or "").strip() or None
            version = str(meta.get("version") or "").strip() or None
            if not version:
                version = DEFAULT_TOOL_VERSION
            metadata_obj = meta.get("metadata")
            metadata = metadata_obj if isinstance(metadata_obj, dict) else _safe_json_obj(metadata_obj)

            created_at = str(meta.get("created_at") or "").strip() or now_iso()
            updated_at = str(meta.get("updated_at") or "").strip() or created_at
            last_used_at = str(meta.get("last_used_at") or "").strip() or updated_at

            # 允许保留 tool_id（id）用于稳定关联
            tool_id = None
            try:
                if meta.get("id") is not None:
                    tool_id = int(meta.get("id"))
            except Exception:
                tool_id = None

            if tool_id is not None and tool_id > 0:
                row = conn.execute("SELECT id FROM tools_items WHERE id = ? LIMIT 1", (int(tool_id),)).fetchone()
                if row:
                    conn.execute(
                        "UPDATE tools_items SET name = ?, description = ?, version = ?, created_at = ?, updated_at = ?, last_used_at = ?, metadata = ?, source_path = ? WHERE id = ?",
                        (
                            name,
                            description or "",
                            version,
                            created_at,
                            updated_at,
                            last_used_at,
                            json.dumps(metadata, ensure_ascii=False) if metadata is not None else None,
                            source_path,
                            int(tool_id),
                        ),
                    )
                    updated += 1
                    continue

                # 新插入：显式指定 id（确保恢复后 skill.scope=tool:{id} 仍有效）
                conn.execute(
                    "INSERT INTO tools_items (id, name, description, version, created_at, updated_at, last_used_at, metadata, source_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        int(tool_id),
                        name,
                        description or "",
                        version,
                        created_at,
                        updated_at,
                        last_used_at,
                        json.dumps(metadata, ensure_ascii=False) if metadata is not None else None,
                        source_path,
                    ),
                )
                inserted += 1
                continue

            row = conn.execute(
                "SELECT id FROM tools_items WHERE source_path = ? ORDER BY id ASC LIMIT 1",
                (source_path,),
            ).fetchone()
            if row:
                conn.execute(
                    "UPDATE tools_items SET name = ?, description = ?, version = ?, updated_at = ?, metadata = ? WHERE id = ?",
                    (
                        name,
                        description or "",
                        version,
                        updated_at,
                        json.dumps(metadata, ensure_ascii=False) if metadata is not None else None,
                        int(row["id"]),
                    ),
                )
                updated += 1
            else:
                conn.execute(
                    "INSERT INTO tools_items (name, description, version, created_at, updated_at, last_used_at, metadata, source_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        name,
                        description or "",
                        version,
                        created_at,
                        updated_at,
                        last_used_at,
                        json.dumps(metadata, ensure_ascii=False) if metadata is not None else None,
                        source_path,
                    ),
                )
                inserted += 1

        # 2) DB -> 文件：补齐未跟踪（source_path 为空）的历史工具，便于后续恢复
        rows = conn.execute(
            "SELECT id FROM tools_items WHERE source_path IS NULL OR source_path = '' ORDER BY id ASC"
        ).fetchall()
        for row in rows:
            info = publish_tool_file(int(row["id"]), conn=conn, base_dir=base)
            if info.get("ok") and info.get("source_path"):
                published += 1

        # 3) prune：文件不存在 -> 删除 DB 记录（仅对有 source_path 的行生效）
        if prune:
            rows = conn.execute(
                "SELECT id, source_path FROM tools_items WHERE source_path IS NOT NULL AND source_path != ''"
            ).fetchall()
            for row in rows:
                sp = str(row["source_path"] or "").replace("\\", "/").strip()
                if not sp:
                    continue
                if sp in discovered_source_paths:
                    continue
                conn.execute("DELETE FROM tools_items WHERE id = ?", (int(row["id"]),))
                deleted += 1

    if errors:
        for err in errors[:5]:
            logger.warning("tool file load error: %s", err)

    return {
        "base_dir": str(base),
        "inserted": inserted,
        "updated": updated,
        "deleted": deleted,
        "published": published,
        "prune": bool(prune),
        "errors": errors,
    }
