# -*- coding: utf-8 -*-
"""
数据库列迁移定义。

定义各表需要确保存在的列，用于增量迁移。
"""

import sqlite3
from typing import Dict, List

from backend.src.constants import DEFAULT_MEMORY_TYPE


# 各表需要确保存在的列定义
# 格式：{表名: [列定义列表]}
COLUMN_MIGRATIONS: Dict[str, List[str]] = {
    "tasks": [
        "started_at TEXT",
        "finished_at TEXT",
    ],
    "task_steps": [
        "run_id INTEGER",
        "detail TEXT",
        "result TEXT",
        "error TEXT",
        "attempts INTEGER",
        "started_at TEXT",
        "finished_at TEXT",
        "step_order INTEGER",
        "executor TEXT",
        "updated_at TEXT",
    ],
    "task_outputs": [
        "run_id INTEGER",
        "output_type TEXT",
        "content TEXT",
        "created_at TEXT",
    ],
    "task_runs": [
        "status TEXT",
        "mode TEXT",
        "summary TEXT",
        "started_at TEXT",
        "finished_at TEXT",
        "created_at TEXT",
        "updated_at TEXT",
        "agent_plan TEXT",
        "agent_state TEXT",
    ],
    "memory_items": [
        f"memory_type TEXT NOT NULL DEFAULT '{DEFAULT_MEMORY_TYPE}'",
        "tags TEXT",
        "task_id INTEGER",
        "uid TEXT",
    ],
    "skills_items": [
        "description TEXT",
        "scope TEXT",
        "category TEXT",
        "tags TEXT",
        "triggers TEXT",
        "aliases TEXT",
        "source_path TEXT",
        "prerequisites TEXT",
        "inputs TEXT",
        "outputs TEXT",
        "steps TEXT",
        "failure_modes TEXT",
        "validation TEXT",
        "version TEXT",
        "task_id INTEGER",
        "domain_id TEXT",
        "skill_type TEXT DEFAULT 'methodology'",
        "status TEXT DEFAULT 'approved'",
        "source_task_id INTEGER",
        "source_run_id INTEGER",
    ],
    "skill_validation_records": [
        "task_id INTEGER",
        "run_id INTEGER",
        "status TEXT",
        "notes TEXT",
        "created_at TEXT",
    ],
    "graph_nodes": [
        "node_type TEXT",
        "attributes TEXT",
        "task_id INTEGER",
        "evidence TEXT",
    ],
    "graph_edges": [
        "confidence REAL",
        "evidence TEXT",
    ],
    "tool_call_records": [
        "run_id INTEGER",
        "reuse_status TEXT",
        "reuse_notes TEXT",
    ],
    "tools_items": [
        "created_at TEXT",
        "updated_at TEXT",
        "last_used_at TEXT",
        "metadata TEXT",
        "source_path TEXT",
    ],
    "tool_version_records": [
        "tool_id INTEGER NOT NULL",
        "previous_version TEXT",
        "next_version TEXT NOT NULL",
        "previous_snapshot TEXT",
        "change_notes TEXT",
        "created_at TEXT NOT NULL",
    ],
    "llm_records": [
        "run_id INTEGER",
        "provider TEXT",
        "model TEXT",
        "prompt_template_id INTEGER",
        "variables TEXT",
        "parameters TEXT",
        "status TEXT",
        "error TEXT",
        "started_at TEXT",
        "finished_at TEXT",
        "created_at TEXT",
        "updated_at TEXT",
        "tokens_prompt INTEGER",
        "tokens_completion INTEGER",
        "tokens_total INTEGER",
    ],
    "eval_criteria_records": [
        "criterion TEXT",
        "status TEXT",
        "notes TEXT",
        "created_at TEXT",
    ],
    "agent_review_records": [
        "pass_score REAL",
        "pass_threshold REAL",
        "distill_status TEXT",
        "distill_score REAL",
        "distill_threshold REAL",
        "distill_notes TEXT",
    ],
    "prompt_templates": [
        "description TEXT",
        "created_at TEXT",
        "updated_at TEXT",
    ],
    "cleanup_jobs": [
        "status TEXT",
        "mode TEXT",
        "tables TEXT",
        "retention_days INTEGER",
        "before TEXT",
        "limit_value INTEGER",
        "last_run_at TEXT",
        "next_run_at TEXT",
        "interval_minutes INTEGER",
        "created_at TEXT",
        "updated_at TEXT",
    ],
    "cleanup_job_runs": [
        "job_id INTEGER NOT NULL",
        "status TEXT NOT NULL",
        "run_at TEXT NOT NULL",
        "finished_at TEXT",
        "summary TEXT",
        "detail TEXT",
    ],
    "graph_extract_tasks": [
        "task_id INTEGER NOT NULL",
        "run_id INTEGER NOT NULL",
        "content TEXT NOT NULL",
        "status TEXT NOT NULL",
        "attempts INTEGER NOT NULL",
        "error TEXT",
        "created_at TEXT NOT NULL",
        "updated_at TEXT NOT NULL",
        "finished_at TEXT",
    ],
    "config_store": [
        "llm_provider TEXT",
        "llm_api_key TEXT",
        "llm_base_url TEXT",
        "llm_model TEXT",
    ],
    "permissions_store": [
        "disabled_actions TEXT",
        "disabled_tools TEXT",
    ],
    "domains": [
        "domain_id TEXT NOT NULL UNIQUE",
        "name TEXT NOT NULL",
        "parent_id TEXT",
        "description TEXT",
        "keywords TEXT",
        "skill_count INTEGER DEFAULT 0",
        "status TEXT DEFAULT 'active'",
        "created_at TEXT NOT NULL",
        "updated_at TEXT NOT NULL",
    ],
}


def ensure_column(conn: sqlite3.Connection, table: str, column_def: str) -> None:
    """
    确保表中存在指定列，不存在则添加。

    Args:
        conn: 数据库连接
        table: 表名
        column_def: 列定义（如 "column_name TEXT"）
    """
    column_name = column_def.split()[0]
    columns = conn.execute(f"PRAGMA table_info({table})").fetchall()
    if any(col["name"] == column_name for col in columns):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column_def}")


def run_column_migrations(conn: sqlite3.Connection) -> None:
    """
    执行所有列迁移。

    Args:
        conn: 数据库连接
    """
    for table, column_defs in COLUMN_MIGRATIONS.items():
        for column_def in column_defs:
            ensure_column(conn, table, column_def)
