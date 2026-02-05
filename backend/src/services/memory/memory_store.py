import json
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from backend.src.common.utils import atomic_write_text, now_iso, parse_json_list
from backend.src.prompt.paths import memory_prompt_dir
from backend.src.prompt.skill_files import parse_skill_markdown
from backend.src.storage import get_connection


_FRONTMATTER_DELIM = "---"


def _build_memory_markdown(meta: Dict[str, Any], content: str) -> str:
    """
    生成 memory Markdown（JSON frontmatter + 正文）。

    说明：
    - frontmatter 采用 JSON（而不是 YAML），避免依赖 PyYAML；
    - parse_skill_markdown 已支持 JSON frontmatter 兜底解析。
    """
    fm_text = json.dumps(meta, ensure_ascii=False, indent=2, sort_keys=True).strip()
    body = str(content or "").rstrip()
    lines = [_FRONTMATTER_DELIM, fm_text, _FRONTMATTER_DELIM, "", body, ""]
    return "\n".join(lines)


def _safe_uid(value: Optional[str]) -> Optional[str]:
    uid = str(value or "").strip()
    return uid or None


def _generate_uid() -> str:
    return uuid.uuid4().hex


def _memory_base_dir(base_dir: Optional[Path] = None) -> Path:
    return base_dir or memory_prompt_dir()


def memory_file_path(uid: str, base_dir: Optional[Path] = None) -> Path:
    return _memory_base_dir(base_dir) / f"{str(uid).strip()}.md"


def delete_memory_file_by_uid(uid: Optional[str], base_dir: Optional[Path] = None) -> Tuple[bool, Optional[str]]:
    """
    删除指定 uid 的 memory 文件。
    """
    uid_value = _safe_uid(uid)
    if not uid_value:
        return False, None
    path = memory_file_path(uid_value, base_dir=base_dir).resolve()
    root = _memory_base_dir(base_dir).resolve()
    try:
        if not path.is_relative_to(root):
            return False, "invalid_uid_path"
    except Exception:
        return False, "invalid_uid_path"
    if not path.exists():
        return False, None
    try:
        path.unlink()
        return True, None
    except Exception as exc:
        return False, f"delete_memory_file failed: {exc}"


def publish_memory_item_file(
    *,
    item_id: int,
    conn=None,
    base_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    将 memory_items 的一条记录写入文件系统（backend/prompt/memory）。

    设计目标：
    - 文件系统作为“灵魂存档”可编辑；
    - DB 作为“快速查询大脑”可随时从文件重建；
    - uid 用于 DB <-> 文件的稳定映射（强一致删除依赖 uid）。
    """
    # 说明：允许复用外部事务连接，避免“DB 已写入但文件落盘失败”导致不一致。
    if conn is None:
        with get_connection() as inner:
            return publish_memory_item_file(item_id=item_id, conn=inner, base_dir=base_dir)

    row = conn.execute("SELECT * FROM memory_items WHERE id = ? LIMIT 1", (int(item_id),)).fetchone()
    if not row:
        return {"ok": False, "error": "memory_item_not_found"}

    uid_value = _safe_uid(row["uid"])
    if not uid_value:
        uid_value = _generate_uid()
        conn.execute("UPDATE memory_items SET uid = ? WHERE id = ?", (uid_value, int(item_id)))
        row = conn.execute("SELECT * FROM memory_items WHERE id = ? LIMIT 1", (int(item_id),)).fetchone()

    tags = parse_json_list(row["tags"]) if row["tags"] else []
    meta = {
        "uid": uid_value,
        "created_at": row["created_at"] or now_iso(),
        "memory_type": row["memory_type"] or "",
        "tags": tags,
        "task_id": row["task_id"],
    }
    markdown = _build_memory_markdown(meta=meta, content=row["content"] or "")
    path = memory_file_path(uid_value, base_dir=base_dir)
    atomic_write_text(path, markdown, encoding="utf-8")
    return {"ok": True, "uid": uid_value, "path": str(path)}


def _discover_memory_files(base_dir: Path) -> List[Path]:
    if not base_dir.exists():
        return []
    files: List[Path] = []
    for path in base_dir.rglob("*.md"):
        if not path.is_file():
            continue
        name = path.name.lower()
        if name in {"readme.md", "_readme.md"}:
            continue
        # 跳过隐藏目录/文件（例如 .trash），避免“删除暂存”被同步回数据库
        if any(part.startswith(".") for part in path.parts):
            continue
        files.append(path)
    return sorted(files)


def sync_memory_from_files(base_dir: Optional[Path] = None, *, prune: bool = True) -> dict:
    """
    将 backend/prompt/memory 下的 memory 文件同步到 SQLite（memory_items）。

    规则：
    - 以 uid 作为主键（文件 frontmatter.uid；缺失则用文件名 stem）
    - prune=True 时强一致删除：DB 中 uid 存在但文件系统已不存在 -> 删除 DB 记录
    - 会自动“补齐未跟踪的历史 DB 记忆”：uid 为空的行会被生成 uid 并落盘文件（便于后续恢复）
    """
    base = base_dir or memory_prompt_dir()
    base.mkdir(parents=True, exist_ok=True)

    errors: List[str] = []
    discovered_uids = set()
    upserts = 0
    inserted = 0
    updated = 0
    deleted = 0
    published = 0

    # 重要：parse 失败不应误删 DB，因此先记录“存在的 uid 集合”（即使解析失败也尽量保留）。
    parsed_items: List[Tuple[str, Dict[str, Any], str]] = []
    for path in _discover_memory_files(base):
        uid_value: Optional[str] = None
        try:
            text = path.read_text(encoding="utf-8")
            rel = str(path.relative_to(base)).replace("\\", "/")
            parsed = parse_skill_markdown(text=text, source_path=rel)
            meta = parsed.meta or {}
            uid_value = _safe_uid(meta.get("uid")) or _safe_uid(meta.get("id")) or _safe_uid(path.stem)
            if not uid_value:
                uid_value = _generate_uid()
                # 自动修复：补齐 uid 并回写文件（避免用户手写新文件忘记 uid）
                meta = dict(meta)
                meta["uid"] = uid_value
                fixed = _build_memory_markdown(meta=meta, content=parsed.body or "")
                atomic_write_text(path, fixed, encoding="utf-8")
            discovered_uids.add(uid_value)
            parsed_items.append((uid_value, meta, parsed.body or ""))
        except Exception as exc:
            # 解析失败时：尽量用文件名作为 uid，避免误删 DB
            try:
                uid_value = _safe_uid(path.stem)
                if uid_value:
                    discovered_uids.add(uid_value)
            except Exception:
                pass
            errors.append(f"{path}: {exc}")

    with get_connection() as conn:
        # 1) 文件 -> DB upsert
        for uid_value, meta, body in parsed_items:
            content = str(body or "")
            memory_type = str(meta.get("memory_type") or meta.get("type") or "").strip() or "short_term"
            tags = meta.get("tags")
            if not isinstance(tags, list):
                tags = []
            task_id = meta.get("task_id")
            created_at = str(meta.get("created_at") or "").strip() or None

            row = conn.execute("SELECT id, created_at FROM memory_items WHERE uid = ? LIMIT 1", (uid_value,)).fetchone()
            if row:
                fields = ["content = ?", "memory_type = ?", "tags = ?", "task_id = ?"]
                params: List[Any] = [
                    content,
                    memory_type,
                    json.dumps(list(tags), ensure_ascii=False),
                    int(task_id) if task_id is not None else None,
                ]
                if created_at and created_at != str(row["created_at"] or ""):
                    fields.append("created_at = ?")
                    params.append(created_at)
                params.append(int(row["id"]))
                conn.execute(f"UPDATE memory_items SET {', '.join(fields)} WHERE id = ?", params)
                updated += 1
            else:
                conn.execute(
                    "INSERT INTO memory_items (content, created_at, memory_type, tags, task_id, uid) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        content,
                        created_at or now_iso(),
                        memory_type,
                        json.dumps(list(tags), ensure_ascii=False),
                        int(task_id) if task_id is not None else None,
                        uid_value,
                    ),
                )
                inserted += 1
            upserts += 1

        # 2) DB -> 文件：补齐未跟踪（uid 为空）的历史记录，避免“DB 有、文件无”导致恢复不完整
        rows = conn.execute(
            "SELECT id FROM memory_items WHERE uid IS NULL OR uid = '' ORDER BY id ASC"
        ).fetchall()
        for row in rows:
            info = publish_memory_item_file(item_id=int(row["id"]), conn=conn, base_dir=base)
            if info.get("ok") and info.get("uid"):
                discovered_uids.add(str(info.get("uid")))
                published += 1

        # 3) prune：文件不存在 -> 删除 DB 记录（仅对已跟踪 uid 生效）
        if prune:
            rows = conn.execute(
                "SELECT id, uid FROM memory_items WHERE uid IS NOT NULL AND uid != ''"
            ).fetchall()
            for row in rows:
                uid_value = str(row["uid"] or "").strip()
                if not uid_value:
                    continue
                if uid_value in discovered_uids:
                    continue
                conn.execute("DELETE FROM memory_items WHERE id = ?", (int(row["id"]),))
                deleted += 1

    return {
        "base_dir": str(base),
        "upserts": upserts,
        "inserted": inserted,
        "updated": updated,
        "deleted": deleted,
        "published": published,
        "prune": bool(prune),
        "errors": errors,
    }
