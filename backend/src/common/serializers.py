"""
序列化工具（row -> dict）。

说明：
- 这些函数会被 API / services / actions 共同复用；
- 放在 common 层，避免 services/actions 反向依赖 api 层代码。
"""

from __future__ import annotations

from backend.src.common.utils import as_bool, parse_json_list, parse_json_value


def task_from_row(row) -> dict:
    return {
        "id": row["id"],
        "title": row["title"],
        "status": row["status"],
        "created_at": row["created_at"],
        "expectation_id": row["expectation_id"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
    }


def task_step_from_row(row) -> dict:
    return {
        "id": row["id"],
        "task_id": row["task_id"],
        "run_id": row["run_id"],
        "title": row["title"],
        "status": row["status"],
        "detail": row["detail"],
        "result": row["result"],
        "error": row["error"],
        "attempts": row["attempts"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "step_order": row["step_order"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def task_output_from_row(row) -> dict:
    return {
        "id": row["id"],
        "task_id": row["task_id"],
        "run_id": row["run_id"],
        "output_type": row["output_type"],
        "content": row["content"],
        "created_at": row["created_at"],
    }


def chat_message_from_row(row) -> dict:
    return {
        "id": row["id"],
        "role": row["role"],
        "content": row["content"],
        "created_at": row["created_at"],
        "task_id": row["task_id"],
        "run_id": row["run_id"],
        "metadata": parse_json_value(row["metadata"]),
    }


def task_run_from_row(row) -> dict:
    return {
        "id": row["id"],
        "task_id": row["task_id"],
        "status": row["status"],
        "summary": row["summary"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def llm_record_from_row(row) -> dict:
    return {
        "id": row["id"],
        "prompt": row["prompt"],
        "response": row["response"],
        "task_id": row["task_id"],
        "run_id": row["run_id"],
        "provider": row["provider"],
        "model": row["model"],
        "prompt_template_id": row["prompt_template_id"],
        "variables": parse_json_value(row["variables"]),
        "parameters": parse_json_value(row["parameters"]),
        "status": row["status"],
        "error": row["error"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "tokens_prompt": row["tokens_prompt"],
        "tokens_completion": row["tokens_completion"],
        "tokens_total": row["tokens_total"],
    }


def prompt_template_from_row(row) -> dict:
    return {
        "id": row["id"],
        "name": row["name"],
        "template": row["template"],
        "description": row["description"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def expectation_from_row(row) -> dict:
    return {
        "id": row["id"],
        "goal": row["goal"],
        "criteria": parse_json_list(row["criteria"]),
        "created_at": row["created_at"],
    }


def eval_criterion_from_row(row) -> dict:
    return {
        "id": row["id"],
        "eval_id": row["eval_id"],
        "criterion": row["criterion"],
        "status": row["status"],
        "notes": row["notes"],
        "created_at": row["created_at"],
    }


def eval_from_row(row) -> dict:
    return {
        "id": row["id"],
        "status": row["status"],
        "score": row["score"],
        "notes": row["notes"],
        "task_id": row["task_id"],
        "expectation_id": row["expectation_id"],
        "created_at": row["created_at"],
    }


def memory_from_row(row) -> dict:
    return {
        "id": row["id"],
        "uid": row["uid"],
        "content": row["content"],
        "created_at": row["created_at"],
        "memory_type": row["memory_type"],
        "tags": parse_json_list(row["tags"]),
        "task_id": row["task_id"],
    }


def skill_from_row(row) -> dict:
    return {
        "id": row["id"],
        "name": row["name"],
        "created_at": row["created_at"],
        "description": row["description"],
        "scope": row["scope"],
        "category": row["category"],
        "tags": parse_json_list(row["tags"]),
        "triggers": parse_json_list(row["triggers"]),
        "aliases": parse_json_list(row["aliases"]),
        "source_path": row["source_path"],
        "prerequisites": parse_json_list(row["prerequisites"]),
        "inputs": parse_json_list(row["inputs"]),
        "outputs": parse_json_list(row["outputs"]),
        "steps": parse_json_list(row["steps"]),
        "failure_modes": parse_json_list(row["failure_modes"]),
        "validation": parse_json_list(row["validation"]),
        "version": row["version"],
        "task_id": row["task_id"],
        # Phase 2：Solution/Skill 统一字段（docs/agent 依赖）
        "domain_id": row["domain_id"],
        "skill_type": row["skill_type"],
        "status": row["status"],
        "source_task_id": row["source_task_id"],
        "source_run_id": row["source_run_id"],
    }


def skill_validation_from_row(row) -> dict:
    return {
        "id": row["id"],
        "skill_id": row["skill_id"],
        "task_id": row["task_id"],
        "run_id": row["run_id"],
        "status": row["status"],
        "notes": row["notes"],
        "created_at": row["created_at"],
    }


def graph_node_from_row(row) -> dict:
    return {
        "id": row["id"],
        "label": row["label"],
        "created_at": row["created_at"],
        "node_type": row["node_type"],
        "attributes": parse_json_value(row["attributes"]),
        "task_id": row["task_id"],
        "evidence": row["evidence"],
    }


def graph_edge_from_row(row) -> dict:
    return {
        "id": row["id"],
        "source": row["source"],
        "target": row["target"],
        "relation": row["relation"],
        "created_at": row["created_at"],
        "confidence": row["confidence"],
        "evidence": row["evidence"],
    }


def tool_from_row(row) -> dict:
    return {
        "id": row["id"],
        "name": row["name"],
        "description": row["description"],
        "version": row["version"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "last_used_at": row["last_used_at"],
        "metadata": parse_json_value(row["metadata"]),
        "source_path": row["source_path"],
    }


def tool_call_from_row(row) -> dict:
    return {
        "id": row["id"],
        "tool_id": row["tool_id"],
        "task_id": row["task_id"],
        "skill_id": row["skill_id"],
        "run_id": row["run_id"],
        "reuse": as_bool(row["reuse"]),
        "reuse_status": row["reuse_status"],
        "reuse_notes": row["reuse_notes"],
        "input": row["input"],
        "output": row["output"],
        "created_at": row["created_at"],
    }


def search_record_from_row(row) -> dict:
    return {
        "id": row["id"],
        "query": row["query"],
        "sources": parse_json_list(row["sources"]),
        "result_count": row["result_count"],
        "task_id": row["task_id"],
        "created_at": row["created_at"],
    }
