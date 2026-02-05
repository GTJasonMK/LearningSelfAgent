from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from backend.src.common.utils import now_iso
from backend.src.repositories.repo_conn import provide_connection


@dataclass(frozen=True)
class GraphNodeCreateParams:
    """
    graph_nodes 创建参数。
    """

    label: str
    node_type: Optional[str]
    attributes: Optional[dict]
    task_id: Optional[int]
    evidence: Optional[str]
    created_at: Optional[str] = None


@dataclass(frozen=True)
class GraphEdgeCreateParams:
    """
    graph_edges 创建参数。
    """

    source: int
    target: int
    relation: str
    confidence: Optional[float]
    evidence: Optional[str]
    created_at: Optional[str] = None


def count_graph_nodes(*, conn: Optional[sqlite3.Connection] = None) -> int:
    sql = "SELECT COUNT(*) AS count FROM graph_nodes"
    with provide_connection(conn) as inner:
        row = inner.execute(sql).fetchone()
    return int(row["count"]) if row else 0


def count_graph_edges(*, conn: Optional[sqlite3.Connection] = None) -> int:
    sql = "SELECT COUNT(*) AS count FROM graph_edges"
    with provide_connection(conn) as inner:
        row = inner.execute(sql).fetchone()
    return int(row["count"]) if row else 0


def create_graph_node(params: GraphNodeCreateParams, *, conn: Optional[sqlite3.Connection] = None) -> int:
    created = params.created_at or now_iso()
    attributes_value = json.dumps(params.attributes, ensure_ascii=False) if params.attributes else None
    sql = "INSERT INTO graph_nodes (label, created_at, node_type, attributes, task_id, evidence) VALUES (?, ?, ?, ?, ?, ?)"
    sql_params = (params.label, created, params.node_type, attributes_value, params.task_id, params.evidence)
    with provide_connection(conn) as inner:
        cursor = inner.execute(sql, sql_params)
        return int(cursor.lastrowid)


def get_graph_node(*, node_id: int, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM graph_nodes WHERE id = ?"
    params = (int(node_id),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def list_graph_nodes(*, conn: Optional[sqlite3.Connection] = None) -> List[sqlite3.Row]:
    sql = "SELECT * FROM graph_nodes ORDER BY id ASC"
    with provide_connection(conn) as inner:
        return list(inner.execute(sql).fetchall())


def required_nodes_exist_for_edge(
    *,
    source: int,
    target: int,
    required_count: int,
    conn: Optional[sqlite3.Connection] = None,
) -> bool:
    sql = "SELECT COUNT(*) AS count FROM graph_nodes WHERE id IN (?, ?)"
    params = (int(source), int(target))
    with provide_connection(conn) as inner:
        row = inner.execute(sql, params).fetchone()
    count = int(row["count"]) if row else 0
    return count >= int(required_count)


def create_graph_edge(params: GraphEdgeCreateParams, *, conn: Optional[sqlite3.Connection] = None) -> int:
    created = params.created_at or now_iso()
    sql = "INSERT INTO graph_edges (source, target, relation, created_at, confidence, evidence) VALUES (?, ?, ?, ?, ?, ?)"
    sql_params = (
        int(params.source),
        int(params.target),
        params.relation,
        created,
        params.confidence,
        params.evidence,
    )
    with provide_connection(conn) as inner:
        cursor = inner.execute(sql, sql_params)
        return int(cursor.lastrowid)


def get_graph_edge(*, edge_id: int, conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
    sql = "SELECT * FROM graph_edges WHERE id = ?"
    params = (int(edge_id),)
    with provide_connection(conn) as inner:
        return inner.execute(sql, params).fetchone()


def list_graph_edges(*, conn: Optional[sqlite3.Connection] = None) -> List[sqlite3.Row]:
    sql = "SELECT * FROM graph_edges ORDER BY id ASC"
    with provide_connection(conn) as inner:
        return list(inner.execute(sql).fetchall())


def query_graph(
    *,
    node_id: Optional[int],
    label: Optional[str],
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[Dict[int, sqlite3.Row], List[sqlite3.Row]]:
    """
    返回 (nodes_map, edges_rows)。
    """
    with provide_connection(conn) as inner:
        nodes_map: Dict[int, sqlite3.Row] = {}
        if node_id is not None:
            row = get_graph_node(node_id=int(node_id), conn=inner)
            if row:
                nodes_map[int(row["id"])] = row

        if label:
            pattern = f"%{label}%"
            rows = inner.execute(
                "SELECT * FROM graph_nodes WHERE label LIKE ? OR node_type LIKE ?",
                (pattern, pattern),
            ).fetchall()
            for row in rows:
                nodes_map[int(row["id"])] = row

        node_ids = list(nodes_map.keys())
        if not node_ids:
            return nodes_map, []

        placeholders = ",".join(["?"] * len(node_ids))
        rows = inner.execute(
            f"SELECT * FROM graph_edges WHERE source IN ({placeholders}) OR target IN ({placeholders})",
            node_ids + node_ids,
        ).fetchall()
        return nodes_map, list(rows)


def search_graph_nodes_like(
    *,
    q: str,
    limit: int = 10,
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    """
    图谱节点检索：当前规模通常较小，使用 LIKE 即可。
    """
    pattern = f"%{q}%"
    sql = "SELECT * FROM graph_nodes WHERE label LIKE ? OR node_type LIKE ? ORDER BY id ASC LIMIT ?"
    params = (pattern, pattern, int(limit))
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def list_graph_edges_for_node_ids(
    *,
    node_ids: Sequence[int],
    conn: Optional[sqlite3.Connection] = None,
) -> List[sqlite3.Row]:
    ids = [int(x) for x in (node_ids or []) if int(x) > 0]
    if not ids:
        return []
    placeholders = ",".join(["?"] * len(ids))
    sql = f"SELECT * FROM graph_edges WHERE source IN ({placeholders}) OR target IN ({placeholders})"
    params = ids + ids
    with provide_connection(conn) as inner:
        return list(inner.execute(sql, params).fetchall())


def delete_graph_node(
    *,
    node_id: int,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[sqlite3.Row]:
    """
    删除节点，并级联删除相关边；返回被删除的 node 行。
    """
    with provide_connection(conn) as inner:
        row = get_graph_node(node_id=node_id, conn=inner)
        if not row:
            return None
        inner.execute("DELETE FROM graph_nodes WHERE id = ?", (int(node_id),))
        inner.execute("DELETE FROM graph_edges WHERE source = ? OR target = ?", (int(node_id), int(node_id)))
        return row


def delete_graph_edge(
    *,
    edge_id: int,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[sqlite3.Row]:
    with provide_connection(conn) as inner:
        row = get_graph_edge(edge_id=edge_id, conn=inner)
        if not row:
            return None
        inner.execute("DELETE FROM graph_edges WHERE id = ?", (int(edge_id),))
        return row


def update_graph_node(
    *,
    node_id: int,
    label: Optional[str] = None,
    node_type: Optional[str] = None,
    attributes: Optional[dict] = None,
    task_id: Optional[int] = None,
    evidence: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[sqlite3.Row]:
    fields: List[str] = []
    params: List[Any] = []
    if label is not None:
        fields.append("label = ?")
        params.append(label)
    if node_type is not None:
        fields.append("node_type = ?")
        params.append(node_type)
    if attributes is not None:
        fields.append("attributes = ?")
        params.append(json.dumps(attributes, ensure_ascii=False))
    if task_id is not None:
        fields.append("task_id = ?")
        params.append(int(task_id))
    if evidence is not None:
        fields.append("evidence = ?")
        params.append(evidence)
    with provide_connection(conn) as inner:
        existing = get_graph_node(node_id=node_id, conn=inner)
        if not existing:
            return None
        if fields:
            params.append(int(node_id))
            inner.execute(
                f"UPDATE graph_nodes SET {', '.join(fields)} WHERE id = ?",
                params,
            )
        return get_graph_node(node_id=node_id, conn=inner)


def update_graph_edge(
    *,
    edge_id: int,
    relation: Optional[str] = None,
    confidence: Optional[float] = None,
    evidence: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[sqlite3.Row]:
    fields: List[str] = []
    params: List[Any] = []
    if relation is not None:
        fields.append("relation = ?")
        params.append(relation)
    if confidence is not None:
        fields.append("confidence = ?")
        params.append(confidence)
    if evidence is not None:
        fields.append("evidence = ?")
        params.append(evidence)
    with provide_connection(conn) as inner:
        existing = get_graph_edge(edge_id=edge_id, conn=inner)
        if not existing:
            return None
        if fields:
            params.append(int(edge_id))
            inner.execute(
                f"UPDATE graph_edges SET {', '.join(fields)} WHERE id = ?",
                params,
            )
        return get_graph_edge(edge_id=edge_id, conn=inner)
