# -*- coding: utf-8 -*-
"""
FTS5 全文检索设置。

初始化 SQLite FTS5 虚拟表与同步触发器。
"""

import logging
import sqlite3

logger = logging.getLogger(__name__)


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


def _drop_triggers(conn: sqlite3.Connection, trigger_names: list[str]) -> None:
    """
    删除触发器（FTS shadow tables 损坏时，用于解除写入阻塞）。

    说明：
    - FTS5 虚拟表损坏时，插入/更新主表会因为触发器写入 FTS 而失败；
    - 这里优先保证“主表可写”，并在查询侧回退 LIKE（fts_table_exists 会判定不可用）。
    """
    existing: set[str] = set()
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND name IN ({})".format(
                ",".join(["?"] * len(trigger_names))
            ),
            tuple(trigger_names),
        ).fetchall()
        existing = {str(r["name"]) for r in (rows or []) if r and r["name"]}
    except Exception:
        existing = set()

    if not existing:
        return

    for name in trigger_names:
        if name not in existing:
            continue
        try:
            conn.execute(f"DROP TRIGGER IF EXISTS {name}")
        except Exception:
            # 删除失败不应阻塞启动（保持可用性）
            pass


def _disable_memory_fts_triggers(conn: sqlite3.Connection, reason: str) -> None:
    # 避免重复日志刷屏：只有当触发器确实存在时才 drop+log
    before = None
    try:
        before = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name IN ('memory_items_ai','memory_items_ad','memory_items_au') LIMIT 1"
        ).fetchone()
    except Exception:
        before = None
    if not before:
        return
    _drop_triggers(conn, ["memory_items_ai", "memory_items_ad", "memory_items_au"])
    try:
        logger.warning("memory_items_fts disabled (triggers dropped): %s", reason)
    except Exception:
        pass


def _disable_skills_fts_triggers(conn: sqlite3.Connection, reason: str) -> None:
    before = None
    try:
        before = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name IN ('skills_items_ai','skills_items_ad','skills_items_au') LIMIT 1"
        ).fetchone()
    except Exception:
        before = None
    if not before:
        return
    _drop_triggers(conn, ["skills_items_ai", "skills_items_ad", "skills_items_au"])
    try:
        logger.warning("skills_items_fts disabled (triggers dropped): %s", reason)
    except Exception:
        pass


def _probe_fts_table(conn: sqlite3.Connection, table: str) -> bool:
    try:
        conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def setup_memory_fts(conn: sqlite3.Connection) -> None:
    """
    设置 memory_items 的 FTS5 索引。

    如果 SQLite 未编译 FTS5 支持，则静默跳过。
    """
    try:
        fts_existed = _table_exists(conn, "memory_items_fts")

        try:
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
        except sqlite3.OperationalError as exc:
            # 常见失败模式：reset/手工操作误删 shadow tables（*_config/_data/_idx/_docsize）
            # 导致“vtable constructor failed”。此时保留主表可写更重要：删除触发器并回退 LIKE。
            if fts_existed:
                _disable_memory_fts_triggers(conn, f"create_or_open_failed: {exc}")
            return

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

        # FTS vtable 可能存在但不可用（shadow tables 被误删）：此时立即禁用触发器避免写入失败。
        if not _probe_fts_table(conn, "memory_items_fts"):
            _disable_memory_fts_triggers(conn, "probe_failed")
            return

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

        try:
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
        except sqlite3.OperationalError as exc:
            if fts_existed:
                _disable_skills_fts_triggers(conn, f"create_or_open_failed: {exc}")
            return

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

        if not _probe_fts_table(conn, "skills_items_fts"):
            _disable_skills_fts_triggers(conn, "probe_failed")
            return

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
