# -*- coding: utf-8 -*-
"""
数据库连接管理。

提供 SQLite 连接获取、路径解析、自动初始化。
迁移逻辑已提取到 migrations 模块。
"""

import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

from backend.src.constants import (
    DB_ENV_VAR,
    DB_RELATIVE_PATH,
)

_DB_INITIALIZED_PATH: Optional[str] = None
_DB_INIT_LOCK = threading.Lock()
logger = logging.getLogger(__name__)


def _default_db_path() -> str:
    """获取默认数据库路径。"""
    base_dir = Path(__file__).resolve().parent
    return str((base_dir / Path(*DB_RELATIVE_PATH)).resolve())


def _project_root() -> Path:
    """
    获取项目根目录。

    说明：
    - storage.py 位于 backend/src/ 下：parents[2] 即仓库根目录；
    - 相对路径统一解析到仓库根目录，避免进程中途 chdir 导致 SQLite 相对路径漂移。
    """
    return Path(__file__).resolve().parents[2]


def _normalize_db_path(raw_path: Optional[str]) -> str:
    """
    规范化数据库路径。

    目标：
    - 允许通过环境变量切换 DB；
    - 将相对路径固定解析为绝对路径（以项目根目录为基准），避免运行时 cwd 漂移；
    - 支持 ':memory:'（每次连接都是独立内存库，需要在连接内自愈初始化）。
    """
    raw = str(raw_path or "").strip()
    if not raw:
        return _default_db_path()

    # 兼容用户在 .env / bat 里写 `"path"` 或 `'path'`
    raw = raw.strip().strip("\"").strip("'")

    # 展开环境变量与 ~
    raw = os.path.expandvars(os.path.expanduser(raw))

    if raw == ":memory:":
        return raw

    # SQLite URI（高级用法）：由调用方自行确保路径正确
    if raw.startswith("file:"):
        return raw

    path = Path(raw)
    if not path.is_absolute():
        try:
            return str((_project_root() / path).resolve())
        except Exception:
            return str((_project_root() / path).absolute())

    try:
        return str(path.resolve())
    except Exception:
        return str(path.absolute())


def resolve_db_path() -> str:
    """
    解析数据库路径（始终以环境变量为准，避免测试/多进程下路径漂移）。

    说明：
    - 不把 DB_PATH 固化成模块级常量，避免 unittest 里切换临时库时需要 reload 模块；
    - init_db 以"路径粒度"做初始化缓存：同一路径只初始化一次，路径变化则重新初始化。
    """
    return _normalize_db_path(os.getenv(DB_ENV_VAR))


@contextmanager
def _open_connection(db_path: str) -> Iterator[sqlite3.Connection]:
    """
    获取 SQLite 连接（上下文管理器），并确保自动 close。

    注意：
    - sqlite3.Connection 的内置 context manager 只负责 commit/rollback，不会 close；
      这会导致 Windows 下测试无法删除临时 DB 文件、以及长期运行的进程句柄泄漏。
    """
    is_memory = db_path == ":memory:"
    is_uri = db_path.startswith("file:")

    if (not is_memory) and (not is_uri):
        try:
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            logger.warning("mkdir db parent failed: %s", exc, exc_info=True)

    # 并行执行/后台线程写库场景下，SQLite 可能出现短暂锁争用。
    # - timeout：连接级 busy timeout（秒）
    # - PRAGMA busy_timeout：语义更直观，毫秒；两者叠加以提升稳定性
    #
    # 说明：并行调度执行器会产生“多线程多连接写入”；这里适度提高等待窗口，
    # 避免短暂锁争用直接变成步骤失败（由上层触发反思/重试会更慢且更不稳定）。
    conn = sqlite3.connect(db_path, timeout=15.0, uri=is_uri)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout = 15000")
    except Exception as exc:
        logger.warning("set PRAGMA busy_timeout failed: %s", exc, exc_info=True)
    try:
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception as exc:
            logger.warning("db rollback failed: %s", exc, exc_info=True)
        raise
    finally:
        try:
            conn.close()
        except Exception as exc:
            logger.warning("db close failed: %s", exc, exc_info=True)


def _core_tables_ready(conn: sqlite3.Connection) -> bool:
    """
    轻量 schema 探测：判断核心表是否存在。

    说明：
    - “no such table” 往往来自：DB 路径漂移 / :memory: 新连接 / DB 被外部脚本重置；
    - 这里用 sqlite_master 做 O(1) 探测，缺失则触发 migrations 自愈。
    """
    core = ("tasks", "task_runs", "task_steps", "task_run_events", "config_store", "permissions_store")
    marks = ",".join(["?"] * len(core))
    try:
        rows = conn.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name IN ({marks})",
            core,
        ).fetchall()
    except Exception:
        return False
    existing = {row["name"] for row in rows or []}
    return all(name in existing for name in core)


def _apply_db_pragmas(conn: sqlite3.Connection) -> None:
    """
    应用连接级/库级 pragmas（尽量幂等）。
    """
    try:
        # 并行读取/写入更友好（仍保持单写者语义），提升多线程 Agent 执行稳定性。
        conn.execute("PRAGMA journal_mode = WAL")
    except Exception as exc:
        logger.warning("set PRAGMA journal_mode=WAL failed: %s", exc, exc_info=True)


def _ensure_db_initialized(conn: sqlite3.Connection, db_path: str) -> None:
    """
    确保当前连接已完成 schema/migrations 初始化（自愈）。

    Args:
        conn: 当前连接
        db_path: 当前连接使用的 db_path（用于缓存判断）
    """
    global _DB_INITIALIZED_PATH

    # :memory: 每次都是新库，必须在当前连接内初始化（不能复用“另一个连接”做 init）
    if db_path == ":memory:":
        from backend.src.migrations import run_all_migrations

        run_all_migrations(conn)
        _apply_db_pragmas(conn)
        return

    # 快路径：已初始化且核心表存在
    if _DB_INITIALIZED_PATH == db_path and _core_tables_ready(conn):
        # 轻量自愈：
        # - migrations 只在“首次初始化/缺表”时运行，但运行中仍可能发生“seed 行被误删 / FTS vtable 损坏”等情况；
        # - 这里做一次低成本兜底，避免后续链路因为缺少内置工具/配置、或 FTS 触发器写入失败而出现不可恢复中断。
        try:
            from backend.src.migrations.fts import run_fts_setup
            from backend.src.migrations.seeds import seed_builtin_tools, seed_config_store, seed_permissions_store

            seed_config_store(conn)
            seed_permissions_store(conn)
            seed_builtin_tools(conn)
            # FTS 兜底：若 shadow tables 被误删会导致 "vtable constructor failed"，
            # 进而触发 memory/skills 写入失败。run_fts_setup 会在探测到 vtable 不可用时主动 drop triggers 并降级。
            run_fts_setup(conn)
        except Exception:
            pass
        return

    with _DB_INIT_LOCK:
        # 二次检查：避免并发下重复 migrations
        if _DB_INITIALIZED_PATH == db_path and _core_tables_ready(conn):
            return

        from backend.src.migrations import run_all_migrations

        run_all_migrations(conn)
        _apply_db_pragmas(conn)
        _DB_INITIALIZED_PATH = db_path


@contextmanager
def get_connection(db_path: Optional[str] = None) -> Iterator[sqlite3.Connection]:
    """
    获取 SQLite 连接（上下文管理器），并确保自动 close。

    额外保证：
    - 当 DB 路径发生变化（例如 unittest 切换临时库）或调用方未显式调用 init_db 时，
      这里会自动完成一次初始化，避免 "no such table" 类错误。

    Args:
        db_path: 可选的数据库路径，默认使用 resolve_db_path()

    Yields:
        SQLite 连接对象
    """
    db_path = _normalize_db_path(db_path or resolve_db_path())
    with _open_connection(db_path) as conn:
        _ensure_db_initialized(conn, db_path)
        yield conn


def init_db() -> None:
    """
    初始化当前 resolve_db_path() 对应的数据库。
    """
    with get_connection() as _:
        return


def reset_db_cache() -> None:
    """
    重置数据库初始化缓存（用于测试）。

    注意：这不会删除数据库文件，只是清除内存中的初始化标记，
    使得下次 get_connection 时会重新执行初始化逻辑。
    """
    global _DB_INITIALIZED_PATH
    with _DB_INIT_LOCK:
        _DB_INITIALIZED_PATH = None
