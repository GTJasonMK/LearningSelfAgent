# -*- coding: utf-8 -*-
"""
数据库表结构定义。

包含所有表的 CREATE TABLE 语句。
"""

from backend.src.constants import (
    DEFAULT_MEMORY_TYPE,
    SINGLETON_ROW_ID,
)


def get_schema_sql() -> str:
    """
    获取完整的表结构 SQL。

    Returns:
        CREATE TABLE 语句集合
    """
    return f"""
    CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        status TEXT NOT NULL,
        created_at TEXT NOT NULL,
        expectation_id INTEGER,
        started_at TEXT,
        finished_at TEXT
    );

    CREATE TABLE IF NOT EXISTS task_steps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL,
        run_id INTEGER,
        title TEXT NOT NULL,
        status TEXT NOT NULL,
        executor TEXT,
        detail TEXT,
        result TEXT,
        error TEXT,
        attempts INTEGER,
        started_at TEXT,
        finished_at TEXT,
        step_order INTEGER,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS task_outputs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL,
        run_id INTEGER,
        output_type TEXT NOT NULL,
        content TEXT NOT NULL,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS chat_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        role TEXT NOT NULL,
        content TEXT NOT NULL,
        created_at TEXT NOT NULL,
        task_id INTEGER,
        run_id INTEGER,
        metadata TEXT
    );

    CREATE TABLE IF NOT EXISTS task_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL,
        status TEXT NOT NULL,
        mode TEXT,
        summary TEXT,
        started_at TEXT,
        finished_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS expectations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        goal TEXT NOT NULL,
        criteria TEXT NOT NULL,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS eval_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        status TEXT NOT NULL,
        score REAL,
        notes TEXT,
        task_id INTEGER,
        expectation_id INTEGER,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS eval_criteria_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        eval_id INTEGER NOT NULL,
        criterion TEXT NOT NULL,
        status TEXT NOT NULL,
        notes TEXT,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS agent_review_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER,
        run_id INTEGER NOT NULL,
        status TEXT NOT NULL,
        pass_score REAL,
        pass_threshold REAL,
        distill_status TEXT,
        distill_score REAL,
        distill_threshold REAL,
        distill_notes TEXT,
        summary TEXT,
        issues TEXT,
        next_actions TEXT,
        skills TEXT,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS memory_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        content TEXT NOT NULL,
        created_at TEXT NOT NULL,
        memory_type TEXT NOT NULL DEFAULT '{DEFAULT_MEMORY_TYPE}',
        tags TEXT,
        task_id INTEGER,
        uid TEXT
    );

    CREATE TABLE IF NOT EXISTS skills_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        created_at TEXT NOT NULL,
        description TEXT,
        scope TEXT,
        category TEXT,
        tags TEXT,
        triggers TEXT,
        aliases TEXT,
        source_path TEXT,
        prerequisites TEXT,
        inputs TEXT,
        outputs TEXT,
        steps TEXT,
        failure_modes TEXT,
        validation TEXT,
        version TEXT,
        task_id INTEGER
    );

    CREATE TABLE IF NOT EXISTS skill_validation_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        skill_id INTEGER NOT NULL,
        task_id INTEGER,
        run_id INTEGER,
        status TEXT NOT NULL,
        notes TEXT,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS graph_nodes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        label TEXT NOT NULL,
        created_at TEXT NOT NULL,
        node_type TEXT,
        attributes TEXT,
        task_id INTEGER,
        evidence TEXT
    );

    CREATE TABLE IF NOT EXISTS graph_edges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source INTEGER NOT NULL,
        target INTEGER NOT NULL,
        relation TEXT NOT NULL,
        created_at TEXT NOT NULL,
        confidence REAL,
        evidence TEXT
    );

    CREATE TABLE IF NOT EXISTS llm_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        prompt TEXT NOT NULL,
        response TEXT NOT NULL,
        task_id INTEGER,
        run_id INTEGER,
        provider TEXT,
        model TEXT,
        prompt_template_id INTEGER,
        variables TEXT,
        parameters TEXT,
        status TEXT,
        error TEXT,
        started_at TEXT,
        finished_at TEXT,
        created_at TEXT,
        updated_at TEXT,
        tokens_prompt INTEGER,
        tokens_completion INTEGER,
        tokens_total INTEGER
    );

    CREATE TABLE IF NOT EXISTS prompt_templates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        template TEXT NOT NULL,
        description TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS tools_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT NOT NULL,
        version TEXT NOT NULL,
        created_at TEXT,
        updated_at TEXT,
        last_used_at TEXT,
        metadata TEXT,
        source_path TEXT
    );

    CREATE TABLE IF NOT EXISTS tool_call_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tool_id INTEGER NOT NULL,
        task_id INTEGER,
        skill_id INTEGER,
        run_id INTEGER,
        reuse INTEGER NOT NULL,
        input TEXT NOT NULL,
        output TEXT NOT NULL,
        reuse_status TEXT,
        reuse_notes TEXT,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS search_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        query TEXT NOT NULL,
        sources TEXT NOT NULL,
        result_count INTEGER NOT NULL,
        task_id INTEGER,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS config_store (
        id INTEGER PRIMARY KEY CHECK (id = {SINGLETON_ROW_ID}),
        tray_enabled INTEGER NOT NULL,
        pet_enabled INTEGER NOT NULL,
        panel_enabled INTEGER NOT NULL,
        llm_provider TEXT,
        llm_api_key TEXT,
        llm_base_url TEXT,
        llm_model TEXT
    );

    CREATE TABLE IF NOT EXISTS permissions_store (
        id INTEGER PRIMARY KEY CHECK (id = {SINGLETON_ROW_ID}),
        allowed_paths TEXT NOT NULL,
        allowed_ops TEXT NOT NULL,
        disabled_actions TEXT NOT NULL,
        disabled_tools TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS update_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        status TEXT NOT NULL,
        notes TEXT,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS tool_version_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tool_id INTEGER NOT NULL,
        previous_version TEXT,
        next_version TEXT NOT NULL,
        previous_snapshot TEXT,
        change_notes TEXT,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS skill_version_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        skill_id INTEGER NOT NULL,
        previous_version TEXT,
        next_version TEXT NOT NULL,
        previous_snapshot TEXT,
        change_notes TEXT,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS cleanup_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        status TEXT NOT NULL,
        mode TEXT NOT NULL,
        tables TEXT,
        retention_days INTEGER,
        before TEXT,
        limit_value INTEGER,
        last_run_at TEXT,
        next_run_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS cleanup_job_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER NOT NULL,
        status TEXT NOT NULL,
        run_at TEXT NOT NULL,
        finished_at TEXT,
        summary TEXT,
        detail TEXT
    );

    CREATE TABLE IF NOT EXISTS graph_extract_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL,
        run_id INTEGER NOT NULL,
        content TEXT NOT NULL,
        status TEXT NOT NULL,
        attempts INTEGER NOT NULL,
        error TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        finished_at TEXT
    );

    CREATE TABLE IF NOT EXISTS domains (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        domain_id TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL,
        parent_id TEXT,
        description TEXT,
        keywords TEXT,
        skill_count INTEGER DEFAULT 0,
        status TEXT DEFAULT 'active',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    """
