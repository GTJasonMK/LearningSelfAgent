from __future__ import annotations

import sqlite3
from typing import Optional, Sequence

from backend.src.repositories import graph_repo
from backend.src.services.common.coerce import (
    to_int,
    to_int_list,
    to_int_or_default,
    to_optional_int,
    to_text,
)

GraphEdgeCreateParams = graph_repo.GraphEdgeCreateParams
GraphNodeCreateParams = graph_repo.GraphNodeCreateParams


def count_graph_nodes(*, conn: Optional[sqlite3.Connection] = None) -> int:
    return to_int_or_default(graph_repo.count_graph_nodes(conn=conn), default=0)


def count_graph_edges(*, conn: Optional[sqlite3.Connection] = None) -> int:
    return to_int_or_default(graph_repo.count_graph_edges(conn=conn), default=0)


def create_graph_node(params: GraphNodeCreateParams, *, conn: Optional[sqlite3.Connection] = None) -> int:
    return to_int(graph_repo.create_graph_node(params, conn=conn))


def get_graph_node(*, node_id: int, conn: Optional[sqlite3.Connection] = None):
    return graph_repo.get_graph_node(node_id=to_int(node_id), conn=conn)


def list_graph_nodes(*, conn: Optional[sqlite3.Connection] = None):
    return graph_repo.list_graph_nodes(conn=conn)


def required_nodes_exist_for_edge(
    *,
    source: int,
    target: int,
    required_count: int,
    conn: Optional[sqlite3.Connection] = None,
) -> bool:
    return bool(
        graph_repo.required_nodes_exist_for_edge(
            source=to_int(source),
            target=to_int(target),
            required_count=to_int(required_count),
            conn=conn,
        )
    )


def create_graph_edge(params: GraphEdgeCreateParams, *, conn: Optional[sqlite3.Connection] = None) -> int:
    return to_int(graph_repo.create_graph_edge(params, conn=conn))


def get_graph_edge(*, edge_id: int, conn: Optional[sqlite3.Connection] = None):
    return graph_repo.get_graph_edge(edge_id=to_int(edge_id), conn=conn)


def list_graph_edges(*, conn: Optional[sqlite3.Connection] = None):
    return graph_repo.list_graph_edges(conn=conn)


def query_graph(
    *,
    node_id: Optional[int],
    label: Optional[str],
    conn: Optional[sqlite3.Connection] = None,
):
    return graph_repo.query_graph(
        node_id=to_optional_int(node_id),
        label=label,
        conn=conn,
    )


def search_graph_nodes_like(
    *,
    q: str,
    limit: int = 10,
    conn: Optional[sqlite3.Connection] = None,
):
    return graph_repo.search_graph_nodes_like(
        q=to_text(q),
        limit=to_int(limit),
        conn=conn,
    )


def list_graph_edges_for_node_ids(
    *,
    node_ids: Sequence[int],
    conn: Optional[sqlite3.Connection] = None,
):
    return graph_repo.list_graph_edges_for_node_ids(
        node_ids=to_int_list(node_ids),
        conn=conn,
    )


def delete_graph_node(*, node_id: int, conn: Optional[sqlite3.Connection] = None):
    return graph_repo.delete_graph_node(node_id=to_int(node_id), conn=conn)


def delete_graph_edge(*, edge_id: int, conn: Optional[sqlite3.Connection] = None):
    return graph_repo.delete_graph_edge(edge_id=to_int(edge_id), conn=conn)


def update_graph_node(
    *,
    node_id: int,
    label: Optional[str] = None,
    node_type: Optional[str] = None,
    attributes: Optional[dict] = None,
    task_id: Optional[int] = None,
    evidence: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
):
    return graph_repo.update_graph_node(
        node_id=to_int(node_id),
        label=label,
        node_type=node_type,
        attributes=attributes,
        task_id=to_optional_int(task_id),
        evidence=evidence,
        conn=conn,
    )


def update_graph_edge(
    *,
    edge_id: int,
    source: Optional[int] = None,
    target: Optional[int] = None,
    relation: Optional[str] = None,
    confidence: Optional[float] = None,
    evidence: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
):
    return graph_repo.update_graph_edge(
        edge_id=to_int(edge_id),
        source=to_optional_int(source),
        target=to_optional_int(target),
        relation=relation,
        confidence=confidence,
        evidence=evidence,
        conn=conn,
    )
