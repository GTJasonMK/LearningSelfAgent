from typing import Optional

from fastapi import APIRouter

from backend.src.common.serializers import (
    graph_edge_from_row,
    graph_node_from_row,
    memory_from_row,
    skill_from_row,
)
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
    INJECTION_WEIGHT_GRAPH,
    INJECTION_WEIGHT_MEMORY,
    INJECTION_WEIGHT_SKILL,
    SOURCE_GRAPH,
    SOURCE_MEMORY,
    SOURCE_SKILLS,
)
from backend.src.repositories.graph_repo import list_graph_edges_for_node_ids, search_graph_nodes_like
from backend.src.repositories.memory_repo import search_memory_fts_or_like
from backend.src.repositories.search_records_repo import create_search_record
from backend.src.repositories.skills_repo import search_skills_fts_or_like

router = APIRouter()


@router.get("/search")
def unified_search(q: str, limit: Optional[int] = None) -> dict:
    effective_limit = int(limit) if limit is not None and int(limit) > 0 else DEFAULT_PAGE_LIMIT
    memory_rows = search_memory_fts_or_like(q=str(q or ""), limit=effective_limit)
    skills_rows = search_skills_fts_or_like(q=str(q or ""), limit=effective_limit)
    graph_rows = search_graph_nodes_like(q=str(q or ""), limit=effective_limit)

    memory_results = [memory_from_row(row) for row in memory_rows]
    skills_results = [skill_from_row(row) for row in skills_rows]
    graph_nodes_results = [graph_node_from_row(row) for row in graph_rows]
    node_ids = [node["id"] for node in graph_nodes_results]
    graph_edges_results = []
    if node_ids:
        edge_rows = list_graph_edges_for_node_ids(node_ids=node_ids)
        graph_edges_results = [graph_edge_from_row(row) for row in edge_rows]
    sources = []
    if memory_results:
        sources.append(SOURCE_MEMORY)
    if skills_results:
        sources.append(SOURCE_SKILLS)
    if graph_nodes_results or graph_edges_results:
        sources.append(SOURCE_GRAPH)
    injection = [
        {
            "type": SOURCE_MEMORY,
            "ref_id": item["id"],
            "weight": INJECTION_WEIGHT_MEMORY,
            "snippet": item["content"],
        }
        for item in memory_results
    ]
    injection += [
        {
            "type": SOURCE_SKILLS,
            "ref_id": item["id"],
            "weight": INJECTION_WEIGHT_SKILL,
            "snippet": item["name"],
        }
        for item in skills_results
    ]
    injection += [
        {
            "type": SOURCE_GRAPH,
            "ref_id": node["id"],
            "weight": INJECTION_WEIGHT_GRAPH,
            "snippet": node["label"],
        }
        for node in graph_nodes_results
    ]
    record_id = create_search_record(
        query=str(q or ""),
        sources=sources,
        result_count=len(memory_results) + len(skills_results) + len(graph_nodes_results),
        task_id=None,
    )
    return {
        "memory": memory_results,
        "skills": skills_results,
        "graph": {"nodes": graph_nodes_results, "edges": graph_edges_results},
        "sources": sources,
        "record_id": record_id,
        "injection": injection,
    }
