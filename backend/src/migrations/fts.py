# -*- coding: utf-8 -*-
"""
FTS5 全文检索设置。

初始化 SQLite FTS5 虚拟表与同步触发器。
"""

import sqlite3


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    """检查表是否存在。"""
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (name,),
    ).fetchone()
    return bool(row and row["name"])


def _count_rows(conn: sqlite3.Connection, table: str) -> int:
    """统计表行数。"""
    try:
        row = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()
        return int(row["c"]) if row else 0
    except Exception:
        return 0


def setup_memory_fts(conn: sqlite3.Connection) -> None:
    """
    设置 memory_items 的 FTS5 索引。

    如果 SQLite 未编译 FTS5 支持，则静默跳过。
    """
    try:
        fts_existed = _table_exists(conn, "memory_items_fts")

        conn.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS memory_items_fts USING fts5(
                content,
                tags,
                content='memory_items',
                content_rowid='id',
                tokenize='unicode61',
                prefix='2 3 4'
            );
            """
        )

        conn.executescript(
            """
            CREATE TRIGGER IF NOT EXISTS memory_items_ai AFTER INSERT ON memory_items BEGIN
                INSERT INTO memory_items_fts(rowid, content, tags) VALUES (new.id, new.content, new.tags);
            END;
            CREATE TRIGGER IF NOT EXISTS memory_items_ad AFTER DELETE ON memory_items BEGIN
                INSERT INTO memory_items_fts(memory_items_fts, rowid, content, tags) VALUES('delete', old.id, old.content, old.tags);
            END;
            CREATE TRIGGER IF NOT EXISTS memory_items_au AFTER UPDATE ON memory_items BEGIN
                INSERT INTO memory_items_fts(memory_items_fts, rowid, content, tags) VALUES('delete', old.id, old.content, old.tags);
                INSERT INTO memory_items_fts(rowid, content, tags) VALUES (new.id, new.content, new.tags);
            END;
            """
        )

        # 首次创建或 FTS 为空但主表不为空时，rebuild 以补齐历史数据
        if (not fts_existed) or (
            _count_rows(conn, "memory_items") > 0 and _count_rows(conn, "memory_items_fts") == 0
        ):
            conn.execute("INSERT INTO memory_items_fts(memory_items_fts) VALUES('rebuild');")

    except sqlite3.OperationalError:
        # FTS5 不可用，静默降级
        pass


def setup_skills_fts(conn: sqlite3.Connection) -> None:
    """
    设置 skills_items 的 FTS5 索引。

    如果 SQLite 未编译 FTS5 支持，则静默跳过。
    """
    try:
        fts_existed = _table_exists(conn, "skills_items_fts")

        conn.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS skills_items_fts USING fts5(
                name,
                description,
                scope,
                category,
                tags,
                triggers,
                content='skills_items',
                content_rowid='id',
                tokenize='unicode61',
                prefix='2 3 4'
            );
            """
        )

        conn.executescript(
            """
            CREATE TRIGGER IF NOT EXISTS skills_items_ai AFTER INSERT ON skills_items BEGIN
                INSERT INTO skills_items_fts(rowid, name, description, scope, category, tags, triggers)
                VALUES (new.id, new.name, new.description, new.scope, new.category, new.tags, new.triggers);
            END;
            CREATE TRIGGER IF NOT EXISTS skills_items_ad AFTER DELETE ON skills_items BEGIN
                INSERT INTO skills_items_fts(skills_items_fts, rowid, name, description, scope, category, tags, triggers)
                VALUES('delete', old.id, old.name, old.description, old.scope, old.category, old.tags, old.triggers);
            END;
            CREATE TRIGGER IF NOT EXISTS skills_items_au AFTER UPDATE ON skills_items BEGIN
                INSERT INTO skills_items_fts(skills_items_fts, rowid, name, description, scope, category, tags, triggers)
                VALUES('delete', old.id, old.name, old.description, old.scope, old.category, old.tags, old.triggers);
                INSERT INTO skills_items_fts(rowid, name, description, scope, category, tags, triggers)
                VALUES (new.id, new.name, new.description, new.scope, new.category, new.tags, new.triggers);
            END;
            """
        )

        if (not fts_existed) or (
            _count_rows(conn, "skills_items") > 0 and _count_rows(conn, "skills_items_fts") == 0
        ):
            conn.execute("INSERT INTO skills_items_fts(skills_items_fts) VALUES('rebuild');")

    except sqlite3.OperationalError:
        # FTS5 不可用，静默降级
        pass


def run_fts_setup(conn: sqlite3.Connection) -> None:
    """
    执行所有 FTS 设置。

    Args:
        conn: 数据库连接
    """
    setup_memory_fts(conn)
    setup_skills_fts(conn)
