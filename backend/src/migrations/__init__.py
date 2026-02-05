# -*- coding: utf-8 -*-
"""
数据库迁移模块。

统一管理表结构、列迁移、FTS 索引、初始数据。
"""

import sqlite3

from backend.src.migrations.schema import get_schema_sql
from backend.src.migrations.columns import run_column_migrations
from backend.src.migrations.fts import run_fts_setup
from backend.src.migrations.seeds import run_all_seeds


def run_all_migrations(conn: sqlite3.Connection) -> None:
    """
    执行所有数据库迁移。

    执行顺序：
    1. 创建表结构
    2. 添加缺失的列
    3. 设置 FTS 索引
    4. 填充初始数据

    Args:
        conn: 数据库连接
    """
    # 1. 创建表结构
    conn.executescript(get_schema_sql())

    # 2. 添加缺失的列
    run_column_migrations(conn)

    # 3. 设置 FTS 索引
    run_fts_setup(conn)

    # 4. 填充初始数据
    run_all_seeds(conn)


__all__ = [
    "run_all_migrations",
    "get_schema_sql",
    "run_column_migrations",
    "run_fts_setup",
    "run_all_seeds",
]
