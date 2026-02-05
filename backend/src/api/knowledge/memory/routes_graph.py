import asyncio
from typing import Optional

from fastapi import APIRouter

from backend.src.common.errors import AppError
from backend.src.api.schemas import (
    GraphEdgeCreate,
    GraphEdgeUpdate,
    GraphNodeCreate,
    GraphNodeUpdate,
)
from backend.src.common.serializers import graph_edge_from_row, graph_node_from_row
from backend.src.api.utils import ensure_write_permission, error_response, now_iso
from backend.src.constants import (
    ERROR_CODE_INVALID_GRAPH_EDGE,
    ERROR_CODE_INVALID_REQUEST,
    ERROR_CODE_NOT_FOUND,
    ERROR_MESSAGE_EDGE_NOT_FOUND,
    ERROR_MESSAGE_NODE_NOT_FOUND,
    GRAPH_EDGE_REQUIRED_NODE_COUNT,
    HTTP_STATUS_BAD_REQUEST,
    HTTP_STATUS_NOT_FOUND,
)
from backend.src.repositories.graph_repo import (
    GraphEdgeCreateParams,
    GraphNodeCreateParams,
    count_graph_edges,
    count_graph_nodes,
    create_graph_edge as create_graph_edge_repo,
    create_graph_node as create_graph_node_repo,
    delete_graph_edge as delete_graph_edge_repo,
    delete_graph_node as delete_graph_node_repo,
    get_graph_edge as get_graph_edge_repo,
    get_graph_node as get_graph_node_repo,
    list_graph_edges as list_graph_edges_repo,
    list_graph_nodes as list_graph_nodes_repo,
    query_graph as query_graph_repo,
    required_nodes_exist_for_edge,
    update_graph_edge as update_graph_edge_repo,
    update_graph_node as update_graph_node_repo,
)
from backend.src.services.graph.graph_delete import delete_graph_edge_strong, delete_graph_node_strong
from backend.src.services.graph.graph_store import (
    publish_graph_edge_file,
    publish_graph_node_file,
    sync_graph_from_files,
)
from backend.src.storage import get_connection

router = APIRouter()


@router.get("/memory/graph")
def memory_graph() -> dict:
    return {"nodes": count_graph_nodes(), "edges": count_graph_edges()}


@router.post("/memory/graph/nodes")
def create_graph_node(payload: GraphNodeCreate) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    with get_connection() as conn:
        node_id = create_graph_node_repo(
            GraphNodeCreateParams(
                label=payload.label,
                node_type=payload.node_type,
                attributes=payload.attributes,
                task_id=payload.task_id,
                evidence=payload.evidence,
                created_at=now_iso(),
            ),
            conn=conn,
        )
        publish = publish_graph_node_file(int(node_id), conn=conn)
        if not publish.get("ok"):
            raise AppError(
                code=ERROR_CODE_INVALID_REQUEST,
                message=str(publish.get("error") or "publish_graph_node_failed"),
                status_code=HTTP_STATUS_BAD_REQUEST,
            )
        row = get_graph_node_repo(node_id=node_id, conn=conn)
    return {"node": graph_node_from_row(row), "file": publish}


@router.post("/memory/graph/edges")
def create_graph_edge(payload: GraphEdgeCreate) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    with get_connection() as conn:
        if not required_nodes_exist_for_edge(
            source=payload.source,
            target=payload.target,
            required_count=GRAPH_EDGE_REQUIRED_NODE_COUNT,
            conn=conn,
        ):
            return error_response(
                ERROR_CODE_INVALID_GRAPH_EDGE,
                ERROR_MESSAGE_NODE_NOT_FOUND,
                HTTP_STATUS_BAD_REQUEST,
            )
        edge_id = create_graph_edge_repo(
            GraphEdgeCreateParams(
                source=payload.source,
                target=payload.target,
                relation=payload.relation,
                confidence=payload.confidence,
                evidence=payload.evidence,
                created_at=now_iso(),
            ),
            conn=conn,
        )
        publish = publish_graph_edge_file(int(edge_id), conn=conn)
        if not publish.get("ok"):
            raise AppError(
                code=ERROR_CODE_INVALID_REQUEST,
                message=str(publish.get("error") or "publish_graph_edge_failed"),
                status_code=HTTP_STATUS_BAD_REQUEST,
            )
        row = get_graph_edge_repo(edge_id=edge_id, conn=conn)
    return {"edge": graph_edge_from_row(row), "file": publish}


@router.get("/memory/graph/nodes")
def list_graph_nodes() -> dict:
    return {"items": [graph_node_from_row(row) for row in list_graph_nodes_repo()]}


@router.get("/memory/graph/edges")
def list_graph_edges() -> dict:
    return {"items": [graph_edge_from_row(row) for row in list_graph_edges_repo()]}


@router.get("/memory/graph/query")
def query_graph(node_id: Optional[int] = None, label: Optional[str] = None) -> dict:
    nodes_map, edge_rows = query_graph_repo(node_id=node_id, label=label)
    return {
        "nodes": [graph_node_from_row(row) for row in nodes_map.values()],
        "edges": [graph_edge_from_row(row) for row in edge_rows],
    }


@router.delete("/memory/graph/nodes/{node_id}")
def delete_graph_node(node_id: int):
    permission = ensure_write_permission()
    if permission:
        return permission
    try:
        result = delete_graph_node_strong(int(node_id))
    except AppError as exc:
        return error_response(exc.code, exc.message, exc.status_code)
    row = result.get("row")
    if not row:
        return error_response(ERROR_CODE_NOT_FOUND, ERROR_MESSAGE_NODE_NOT_FOUND, HTTP_STATUS_NOT_FOUND)
    return {"deleted": True, "node": graph_node_from_row(row), "file": result.get("file"), "edges_deleted": result.get("edges_deleted")}


@router.delete("/memory/graph/edges/{edge_id}")
def delete_graph_edge(edge_id: int):
    permission = ensure_write_permission()
    if permission:
        return permission
    try:
        result = delete_graph_edge_strong(int(edge_id))
    except AppError as exc:
        return error_response(exc.code, exc.message, exc.status_code)
    row = result.get("row")
    if not row:
        return error_response(ERROR_CODE_NOT_FOUND, ERROR_MESSAGE_EDGE_NOT_FOUND, HTTP_STATUS_NOT_FOUND)
    return {"deleted": True, "edge": graph_edge_from_row(row), "file": result.get("file")}


@router.patch("/memory/graph/nodes/{node_id}")
def update_graph_node(node_id: int, payload: GraphNodeUpdate):
    permission = ensure_write_permission()
    if permission:
        return permission
    with get_connection() as conn:
        row = update_graph_node_repo(
            node_id=node_id,
            label=payload.label,
            node_type=payload.node_type,
            attributes=payload.attributes,
            task_id=payload.task_id,
            evidence=payload.evidence,
            conn=conn,
        )
        if not row:
            return error_response(
                ERROR_CODE_NOT_FOUND,
                ERROR_MESSAGE_NODE_NOT_FOUND,
                HTTP_STATUS_NOT_FOUND,
            )
        publish = publish_graph_node_file(int(node_id), conn=conn)
        if not publish.get("ok"):
            raise AppError(
                code=ERROR_CODE_INVALID_REQUEST,
                message=str(publish.get("error") or "publish_graph_node_failed"),
                status_code=HTTP_STATUS_BAD_REQUEST,
            )
        latest = get_graph_node_repo(node_id=int(node_id), conn=conn)
    return {"node": graph_node_from_row(latest or row), "file": publish}


@router.patch("/memory/graph/edges/{edge_id}")
def update_graph_edge(edge_id: int, payload: GraphEdgeUpdate):
    permission = ensure_write_permission()
    if permission:
        return permission
    with get_connection() as conn:
        row = update_graph_edge_repo(
            edge_id=edge_id,
            relation=payload.relation,
            confidence=payload.confidence,
            evidence=payload.evidence,
            conn=conn,
        )
        if not row:
            return error_response(
                ERROR_CODE_NOT_FOUND,
                ERROR_MESSAGE_EDGE_NOT_FOUND,
                HTTP_STATUS_NOT_FOUND,
            )
        publish = publish_graph_edge_file(int(edge_id), conn=conn)
        if not publish.get("ok"):
            raise AppError(
                code=ERROR_CODE_INVALID_REQUEST,
                message=str(publish.get("error") or "publish_graph_edge_failed"),
                status_code=HTTP_STATUS_BAD_REQUEST,
            )
        latest = get_graph_edge_repo(edge_id=int(edge_id), conn=conn)
    return {"edge": graph_edge_from_row(latest or row), "file": publish}


@router.post("/memory/graph/sync")
async def sync_graph() -> dict:
    """
    将 backend/prompt/graph 下的图谱文件同步到数据库（graph_nodes/graph_edges）。
    """
    permission = ensure_write_permission()
    if permission:
        return permission
    result = await asyncio.to_thread(sync_graph_from_files, None, prune=True)
    return {"result": result}
