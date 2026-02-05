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


def resolve_db_path() -> str:
    """
    解析数据库路径（始终以环境变量为准，避免测试/多进程下路径漂移）。

    说明：
    - 不把 DB_PATH 固化成模块级常量，避免 unittest 里切换临时库时需要 reload 模块；
    - init_db 以"路径粒度"做初始化缓存：同一路径只初始化一次，路径变化则重新初始化。
    """
    return str(os.getenv(DB_ENV_VAR, _default_db_path()))


@contextmanager
def _open_connection(db_path: str) -> Iterator[sqlite3.Connection]:
    """
    获取 SQLite 连接（上下文管理器），并确保自动 close。

    注意：
    - sqlite3.Connection 的内置 context manager 只负责 commit/rollback，不会 close；
      这会导致 Windows 下测试无法删除临时 DB 文件、以及长期运行的进程句柄泄漏。
    """
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
    conn = sqlite3.connect(db_path, timeout=15.0)
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


def _init_db_for_path(db_path: str) -> None:
    """
    初始化指定路径的数据库。

    Args:
        db_path: 数据库文件路径
    """
    global _DB_INITIALIZED_PATH

    with _DB_INIT_LOCK:
        if _DB_INITIALIZED_PATH == db_path:
            return

        try:
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            logger.warning("mkdir db parent failed: %s", exc, exc_info=True)

        # 延迟导入避免循环依赖
        from backend.src.migrations import run_all_migrations

        with _open_connection(db_path) as conn:
            run_all_migrations(conn)
            try:
                # 并行读取/写入更友好（仍保持单写者语义），提升多线程 Agent 执行稳定性。
                conn.execute("PRAGMA journal_mode = WAL")
            except Exception as exc:
                logger.warning("set PRAGMA journal_mode=WAL failed: %s", exc, exc_info=True)

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
    db_path = str(db_path or resolve_db_path())
    if _DB_INITIALIZED_PATH != db_path:
        _init_db_for_path(db_path)
    with _open_connection(db_path) as conn:
        yield conn


def init_db() -> None:
    """
    初始化当前 resolve_db_path() 对应的数据库。
    """
    _init_db_for_path(resolve_db_path())


def reset_db_cache() -> None:
    """
    重置数据库初始化缓存（用于测试）。

    注意：这不会删除数据库文件，只是清除内存中的初始化标记，
    使得下次 get_connection 时会重新执行初始化逻辑。
    """
    global _DB_INITIALIZED_PATH
    with _DB_INIT_LOCK:
        _DB_INITIALIZED_PATH = None
