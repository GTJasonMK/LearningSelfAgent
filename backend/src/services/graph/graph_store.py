from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from backend.src.common.utils import (
    atomic_write_text,
    build_json_frontmatter_markdown,
    coerce_int,
    discover_markdown_files,
    now_iso,
    parse_positive_int,
    parse_json_dict,
)
from backend.src.prompt.paths import graph_prompt_dir
from backend.src.prompt.skill_files import parse_skill_markdown
from backend.src.storage import get_connection

logger = logging.getLogger(__name__)

def graph_nodes_dir() -> Path:
    return graph_prompt_dir() / "nodes"


def graph_edges_dir() -> Path:
    return graph_prompt_dir() / "edges"


def _graph_nodes_dir(base_dir: Optional[Path] = None) -> Path:
    base = base_dir or graph_prompt_dir()
    return base / "nodes"


def _graph_edges_dir(base_dir: Optional[Path] = None) -> Path:
    base = base_dir or graph_prompt_dir()
    return base / "edges"


def graph_node_file_path(node_id: int) -> Path:
    return graph_nodes_dir() / f"{int(node_id)}.md"


def graph_edge_file_path(edge_id: int) -> Path:
    return graph_edges_dir() / f"{int(edge_id)}.md"


def publish_graph_node_file(node_id: int, *, conn=None, base_dir: Optional[Path] = None) -> Dict[str, Any]:
    """
    将 graph_nodes 的一条记录写入文件系统（backend/prompt/graph/nodes）。
    """
    if conn is None:
        with get_connection() as inner:
            return publish_graph_node_file(int(node_id), conn=inner, base_dir=base_dir)

    row = conn.execute("SELECT * FROM graph_nodes WHERE id = ? LIMIT 1", (int(node_id),)).fetchone()
    if not row:
        return {"ok": False, "error": "node_not_found"}

    meta = {
        "id": int(row["id"]),
        "label": row["label"],
        "created_at": row["created_at"],
        "node_type": row["node_type"],
        "attributes": parse_json_dict(row["attributes"]) if row["attributes"] else None,
        "task_id": row["task_id"],
        "evidence": row["evidence"],
    }
    path = _graph_nodes_dir(base_dir) / f"{int(node_id)}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(path, build_json_frontmatter_markdown(meta=meta, body=""), encoding="utf-8")
    return {"ok": True, "path": str(path)}


def publish_graph_edge_file(edge_id: int, *, conn=None, base_dir: Optional[Path] = None) -> Dict[str, Any]:
    """
    将 graph_edges 的一条记录写入文件系统（backend/prompt/graph/edges）。
    """
    if conn is None:
        with get_connection() as inner:
            return publish_graph_edge_file(int(edge_id), conn=inner, base_dir=base_dir)

    row = conn.execute("SELECT * FROM graph_edges WHERE id = ? LIMIT 1", (int(edge_id),)).fetchone()
    if not row:
        return {"ok": False, "error": "edge_not_found"}

    meta = {
        "id": int(row["id"]),
        "source": int(row["source"]),
        "target": int(row["target"]),
        "relation": row["relation"],
        "created_at": row["created_at"],
        "confidence": row["confidence"],
        "evidence": row["evidence"],
    }
    path = _graph_edges_dir(base_dir) / f"{int(edge_id)}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(path, build_json_frontmatter_markdown(meta=meta, body=""), encoding="utf-8")
    return {"ok": True, "path": str(path)}


def _parse_graph_file(path: Path, rel_root: Path) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[str]]:
    """
    返回：(meta, rel, error)
    """
    try:
        rel = str(path.relative_to(rel_root)).replace("\\", "/")
    except Exception:
        rel = path.name
    try:
        text = path.read_text(encoding="utf-8")
        parsed = parse_skill_markdown(text=text, source_path=rel)
        meta = parsed.meta or {}
        if not isinstance(meta, dict):
            return None, rel, "invalid_meta"
        return meta, rel, None
    except Exception as exc:
        return None, rel, f"{exc}"


def sync_graph_from_files(base_dir: Optional[Path] = None, *, prune: bool = True) -> dict:
    """
    将 backend/prompt/graph 下的 nodes/edges 文件同步到 SQLite（graph_nodes/graph_edges）。

    设计：
    - 文件系统作为“灵魂存档”（可编辑/可恢复）
    - SQLite 作为“快速查询大脑”

    规则：
    - 节点文件：graph/nodes/{id}.md（frontmatter.id 可选；默认用文件名 stem）
    - 边文件：graph/edges/{id}.md（frontmatter.id 可选；默认用文件名 stem）
    - prune=True：若 DB 存在但文件不存在，则删除 DB 记录（强一致删除）
    - 迁移兜底：若目录内完全没有任何 graph 文件，则自动把 DB 现有图谱导出到文件（避免升级后误删历史数据）
    """
    base = base_dir or graph_prompt_dir()
    nodes_base = base / "nodes"
    edges_base = base / "edges"
    nodes_base.mkdir(parents=True, exist_ok=True)
    edges_base.mkdir(parents=True, exist_ok=True)

    errors: List[str] = []
    discovered_node_ids: Set[int] = set()
    discovered_edge_ids: Set[int] = set()
    node_items: List[Dict[str, Any]] = []
    edge_items: List[Dict[str, Any]] = []

    # --- nodes ---
    for path in discover_markdown_files(nodes_base):
        # parse 失败也要尽量纳入 discovered（避免误删 DB）
        stem_id = parse_positive_int(path.stem, default=None)
        if stem_id:
            discovered_node_ids.add(stem_id)
        meta, _rel, err = _parse_graph_file(path, nodes_base)
        if err:
            errors.append(f"{path}: {err}")
            continue
        if not meta:
            continue
        node_id = parse_positive_int(meta.get("id"), default=None) or stem_id
        if not node_id:
            continue
        discovered_node_ids.add(int(node_id))
        node_items.append({"id": int(node_id), "meta": meta})

    # --- edges ---
    for path in discover_markdown_files(edges_base):
        stem_id = parse_positive_int(path.stem, default=None)
        if stem_id:
            discovered_edge_ids.add(stem_id)
        meta, _rel, err = _parse_graph_file(path, edges_base)
        if err:
            errors.append(f"{path}: {err}")
            continue
        if not meta:
            continue
        edge_id = parse_positive_int(meta.get("id"), default=None) or stem_id
        if not edge_id:
            continue
        discovered_edge_ids.add(int(edge_id))
        edge_items.append({"id": int(edge_id), "meta": meta})

    inserted_nodes = 0
    updated_nodes = 0
    inserted_edges = 0
    updated_edges = 0
    deleted_nodes = 0
    deleted_edges = 0
    exported_nodes = 0
    exported_edges = 0

    with get_connection() as conn:
        # 迁移兜底：如果完全没有任何文件，则导出 DB 现有图谱，并跳过 prune
        if not node_items and not edge_items:
            db_nodes = conn.execute("SELECT id FROM graph_nodes ORDER BY id ASC").fetchall()
            db_edges = conn.execute("SELECT id FROM graph_edges ORDER BY id ASC").fetchall()
            for row in db_nodes:
                info = publish_graph_node_file(int(row["id"]), conn=conn, base_dir=base)
                if info.get("ok"):
                    exported_nodes += 1
            for row in db_edges:
                info = publish_graph_edge_file(int(row["id"]), conn=conn, base_dir=base)
                if info.get("ok"):
                    exported_edges += 1
            return {
                "base_dir": str(base),
                "mode": "export_db_to_files",
                "exported_nodes": exported_nodes,
                "exported_edges": exported_edges,
                "errors": errors,
            }

        # 1) 文件 -> DB upsert（nodes）
        for item in node_items:
            node_id = int(item["id"])
            meta = item["meta"]
            label = str(meta.get("label") or "").strip()
            if not label:
                continue
            created_at = str(meta.get("created_at") or "").strip() or now_iso()
            node_type = str(meta.get("node_type") or meta.get("type") or "").strip() or None
            attributes = parse_json_dict(meta.get("attributes"))
            attributes_value = json.dumps(attributes, ensure_ascii=False) if attributes is not None else None
            task_id = parse_positive_int(meta.get("task_id"), default=None)
            evidence = str(meta.get("evidence") or "").strip() or None

            row = conn.execute("SELECT id, created_at FROM graph_nodes WHERE id = ? LIMIT 1", (node_id,)).fetchone()
            if row:
                fields = ["label = ?", "node_type = ?", "attributes = ?", "task_id = ?", "evidence = ?"]
                params: List[Any] = [label, node_type, attributes_value, task_id, evidence]
                if created_at and created_at != str(row["created_at"] or ""):
                    fields.append("created_at = ?")
                    params.append(created_at)
                params.append(node_id)
                conn.execute(f"UPDATE graph_nodes SET {', '.join(fields)} WHERE id = ?", params)
                updated_nodes += 1
            else:
                conn.execute(
                    "INSERT INTO graph_nodes (id, label, created_at, node_type, attributes, task_id, evidence) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (node_id, label, created_at, node_type, attributes_value, task_id, evidence),
                )
                inserted_nodes += 1

        # 2) 文件 -> DB upsert（edges）
        for item in edge_items:
            edge_id = int(item["id"])
            meta = item["meta"]
            source = parse_positive_int(meta.get("source"), default=None)
            target = parse_positive_int(meta.get("target"), default=None)
            relation = str(meta.get("relation") or "").strip()
            if not source or not target or not relation:
                continue
            source_id = coerce_int(source, default=0)
            target_id = coerce_int(target, default=0)
            if source_id <= 0 or target_id <= 0:
                continue

            # 若节点不存在，跳过（避免产生悬挂边）
            exists = conn.execute(
                "SELECT COUNT(*) AS c FROM graph_nodes WHERE id IN (?, ?)",
                (source_id, target_id),
            ).fetchone()
            if not exists or coerce_int(exists["c"], default=0) < 2:
                continue

            created_at = str(meta.get("created_at") or "").strip() or now_iso()
            confidence = meta.get("confidence")
            try:
                confidence_value = float(confidence) if confidence is not None else None
            except Exception:
                confidence_value = None
            evidence = str(meta.get("evidence") or "").strip() or None

            row = conn.execute("SELECT id, created_at FROM graph_edges WHERE id = ? LIMIT 1", (edge_id,)).fetchone()
            if row:
                fields = ["source = ?", "target = ?", "relation = ?", "confidence = ?", "evidence = ?"]
                params2: List[Any] = [source_id, target_id, relation, confidence_value, evidence]
                if created_at and created_at != str(row["created_at"] or ""):
                    fields.append("created_at = ?")
                    params2.append(created_at)
                params2.append(edge_id)
                conn.execute(f"UPDATE graph_edges SET {', '.join(fields)} WHERE id = ?", params2)
                updated_edges += 1
            else:
                conn.execute(
                    "INSERT INTO graph_edges (id, source, target, relation, created_at, confidence, evidence) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (edge_id, source_id, target_id, relation, created_at, confidence_value, evidence),
                )
                inserted_edges += 1

        # 3) prune：文件不存在 -> 删除 DB 记录
        if prune:
            rows = conn.execute("SELECT id FROM graph_edges ORDER BY id ASC").fetchall()
            for row in rows:
                eid = int(row["id"])
                if eid in discovered_edge_ids:
                    continue
                conn.execute("DELETE FROM graph_edges WHERE id = ?", (eid,))
                deleted_edges += 1

            rows = conn.execute("SELECT id FROM graph_nodes ORDER BY id ASC").fetchall()
            for row in rows:
                nid = int(row["id"])
                if nid in discovered_node_ids:
                    continue
                conn.execute("DELETE FROM graph_nodes WHERE id = ?", (nid,))
                conn.execute("DELETE FROM graph_edges WHERE source = ? OR target = ?", (nid, nid))
                deleted_nodes += 1

    if errors:
        for err in errors[:5]:
            logger.warning("graph file load error: %s", err)

    return {
        "base_dir": str(base),
        "inserted_nodes": inserted_nodes,
        "updated_nodes": updated_nodes,
        "inserted_edges": inserted_edges,
        "updated_edges": updated_edges,
        "deleted_nodes": deleted_nodes,
        "deleted_edges": deleted_edges,
        "prune": bool(prune),
        "errors": errors,
    }
