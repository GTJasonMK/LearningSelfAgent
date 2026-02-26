# -*- coding: utf-8 -*-
"""
初始数据填充。

包含配置、权限、内置工具、预定义领域的初始化。
"""

import json
import sqlite3
from typing import List, Tuple

from backend.src.common.utils import now_iso, parse_json_list, tool_is_draft
from backend.src.constants import (
    DEFAULT_ALLOWED_OPS,
    DEFAULT_ALLOWED_PATHS,
    DEFAULT_DISABLED_ACTIONS,
    DEFAULT_DISABLED_TOOLS,
    RIGHT_CODES_DEFAULT_BASE_URL,
    DEFAULT_PANEL_ENABLED,
    DEFAULT_PET_ENABLED,
    DEFAULT_TRAY_ENABLED,
    OP_EXEC,
    SINGLETON_ROW_ID,
    TOOL_DESCRIPTION_WEB_FETCH,
    TOOL_NAME_WEB_FETCH,
    TOOL_VERSION_WEB_FETCH,
    TOOL_WEB_FETCH_ARGS_TEMPLATE,
    TOOL_WEB_FETCH_TIMEOUT_MS,
)

# LLM 默认配置（用于首次初始化与“空值回填”）
DEFAULT_LLM_PROVIDER = "rightcode"
DEFAULT_LLM_BASE_URL = RIGHT_CODES_DEFAULT_BASE_URL
DEFAULT_LLM_MODEL = "gpt-5.2"


def seed_config_store(conn: sqlite3.Connection) -> None:
    """
    初始化配置表。

    Args:
        conn: 数据库连接
    """
    config_row = conn.execute(
        "SELECT id, llm_provider, llm_base_url, llm_model FROM config_store WHERE id = ?",
        (SINGLETON_ROW_ID,),
    ).fetchone()

    if not config_row:
        conn.execute(
            "INSERT INTO config_store (id, tray_enabled, pet_enabled, panel_enabled, llm_provider, llm_base_url, llm_model) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                SINGLETON_ROW_ID,
                int(DEFAULT_TRAY_ENABLED),
                int(DEFAULT_PET_ENABLED),
                int(DEFAULT_PANEL_ENABLED),
                DEFAULT_LLM_PROVIDER,
                DEFAULT_LLM_BASE_URL,
                DEFAULT_LLM_MODEL,
            ),
        )
        return

    # 对已有行做“空值回填”：
    # - 仅当字段为空时补默认值；
    # - 若用户已配置 provider/base_url/model，则保持不变。
    provider = str(config_row["llm_provider"] or "").strip()
    base_url = str(config_row["llm_base_url"] or "").strip()
    model = str(config_row["llm_model"] or "").strip()

    next_provider = provider or DEFAULT_LLM_PROVIDER
    next_base_url = base_url or DEFAULT_LLM_BASE_URL
    next_model = model or DEFAULT_LLM_MODEL

    if (
        next_provider != provider
        or next_base_url != base_url
        or next_model != model
    ):
        conn.execute(
            "UPDATE config_store SET llm_provider = ?, llm_base_url = ?, llm_model = ? WHERE id = ?",
            (next_provider, next_base_url, next_model, SINGLETON_ROW_ID),
        )


def seed_permissions_store(conn: sqlite3.Connection) -> None:
    """
    初始化权限表。

    Args:
        conn: 数据库连接
    """
    permissions_row = conn.execute(
        "SELECT id FROM permissions_store WHERE id = ?",
        (SINGLETON_ROW_ID,),
    ).fetchone()

    if not permissions_row:
        conn.execute(
            "INSERT INTO permissions_store (id, allowed_paths, allowed_ops, disabled_actions, disabled_tools) VALUES (?, ?, ?, ?, ?)",
            (
                SINGLETON_ROW_ID,
                json.dumps(list(DEFAULT_ALLOWED_PATHS)),
                json.dumps(list(DEFAULT_ALLOWED_OPS)),
                json.dumps(list(DEFAULT_DISABLED_ACTIONS)),
                json.dumps(list(DEFAULT_DISABLED_TOOLS)),
            ),
        )
    else:
        # 确保 OP_EXEC 在 allowed_ops 中
        row = conn.execute(
            "SELECT allowed_ops FROM permissions_store WHERE id = ?",
            (SINGLETON_ROW_ID,),
        ).fetchone()

        allowed_ops = parse_json_list(row["allowed_ops"] if row else None)

        if OP_EXEC not in allowed_ops:
            allowed_ops.append(OP_EXEC)
            conn.execute(
                "UPDATE permissions_store SET allowed_ops = ? WHERE id = ?",
                (json.dumps(allowed_ops), SINGLETON_ROW_ID),
            )


def seed_builtin_tools(conn: sqlite3.Connection) -> None:
    """
    初始化内置工具（如 web_fetch）。

    Args:
        conn: 数据库连接
    """
    try:
        rows = conn.execute(
            "SELECT id, metadata FROM tools_items WHERE name = ? ORDER BY id ASC",
            (TOOL_NAME_WEB_FETCH,),
        ).fetchall()

        # 仅当不存在“非 draft 的可用 web_fetch”时才插入内置版本：
        # - 避免被 Agent 运行时创建的 draft 版本占坑，导致后续任务只能选到不可复用的工具；
        # - 与 tools_repo.get_tool_by_name 的优先级策略配合：优先非 draft。
        has_non_draft = False
        for row in rows or []:
            if not row:
                continue
            if tool_is_draft(row["metadata"]):
                continue
            has_non_draft = True
            break

        if not has_non_draft:
            now = now_iso()
            metadata = json.dumps(
                {
                    "exec": {
                        "type": "shell",
                        "args": list(TOOL_WEB_FETCH_ARGS_TEMPLATE),
                        "timeout_ms": TOOL_WEB_FETCH_TIMEOUT_MS,
                        "retry": {"max_attempts": 3, "backoff_ms": 200},
                    }
                },
                ensure_ascii=False,
            )
            conn.execute(
                "INSERT INTO tools_items (name, description, version, created_at, updated_at, last_used_at, metadata) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    TOOL_NAME_WEB_FETCH,
                    TOOL_DESCRIPTION_WEB_FETCH,
                    TOOL_VERSION_WEB_FETCH,
                    now,
                    now,
                    now,
                    metadata,
                ),
            )
    except Exception:
        # 初始化失败不应阻塞应用启动
        pass


# 预定义领域列表
# 格式：(domain_id, name, parent_id, description, keywords_json)
PREDEFINED_DOMAINS: List[Tuple[str, str, str | None, str, str]] = [
    # 一级领域
    ("data", "数据处理", None, "数据采集、清洗、转换、分析等操作", '["数据", "处理", "分析", "清洗", "转换"]'),
    ("finance", "金融分析", None, "股票、基金、风控等金融领域分析", '["金融", "股票", "基金", "投资", "风控"]'),
    ("file", "文件操作", None, "文件读写、格式转换、压缩解压等操作", '["文件", "读写", "转换", "压缩", "解压"]'),
    ("web", "网络爬虫", None, "网页抓取、数据解析、内容存储等操作", '["爬虫", "抓取", "网页", "解析", "HTTP"]'),
    ("dev", "开发工具", None, "代码生成、测试、部署等开发相关操作", '["开发", "代码", "测试", "部署", "编程"]'),
    ("misc", "未分类", None, "暂未归类的技能和方案", '["其他", "未分类", "杂项"]'),
    # 二级领域
    ("data.collect", "数据采集", "data", "从各种来源采集原始数据", '["采集", "获取", "抓取", "下载"]'),
    ("data.clean", "数据清洗", "data", "数据去噪、补缺、标准化处理", '["清洗", "去噪", "补缺", "标准化"]'),
    ("data.analyze", "数据分析", "data", "数据统计、可视化、建模分析", '["分析", "统计", "可视化", "建模"]'),
    ("finance.stock", "股票分析", "finance", "股票行情分析、选股、推荐", '["股票", "行情", "选股", "K线"]'),
    ("finance.fund", "基金分析", "finance", "基金评估、配置、推荐", '["基金", "净值", "配置", "定投"]'),
    ("finance.risk", "风险控制", "finance", "风险评估、预警、控制", '["风险", "风控", "预警", "评估"]'),
    ("file.read", "文件读取", "file", "读取各种格式文件内容", '["读取", "解析", "加载"]'),
    ("file.write", "文件写入", "file", "创建或写入文件内容", '["写入", "创建", "保存", "导出"]'),
    ("file.convert", "格式转换", "file", "文件格式之间的转换", '["转换", "格式", "编码"]'),
    ("web.crawl", "网页抓取", "web", "抓取网页内容", '["抓取", "爬取", "下载"]'),
    ("web.parse", "内容解析", "web", "解析网页结构和内容", '["解析", "提取", "DOM", "XPath"]'),
    ("dev.codegen", "代码生成", "dev", "自动生成代码", '["生成", "代码", "模板"]'),
    ("dev.test", "测试验证", "dev", "代码测试和验证", '["测试", "验证", "断言", "覆盖"]'),
    ("dev.deploy", "部署发布", "dev", "应用部署和发布", '["部署", "发布", "上线", "CI/CD"]'),
]


def seed_predefined_domains(conn: sqlite3.Connection) -> None:
    """
    初始化预定义领域。

    Args:
        conn: 数据库连接
    """
    now = now_iso()

    for domain_id, name, parent_id, description, keywords in PREDEFINED_DOMAINS:
        try:
            existing = conn.execute(
                "SELECT id FROM domains WHERE domain_id = ?",
                (domain_id,),
            ).fetchone()

            if not existing:
                conn.execute(
                    """INSERT INTO domains (domain_id, name, parent_id, description, keywords, skill_count, status, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, 0, 'active', ?, ?)""",
                    (domain_id, name, parent_id, description, keywords, now, now),
                )
        except Exception:
            # 初始化失败不应阻塞应用启动
            pass


def run_all_seeds(conn: sqlite3.Connection) -> None:
    """
    执行所有初始数据填充。

    Args:
        conn: 数据库连接
    """
    seed_config_store(conn)
    seed_permissions_store(conn)
    seed_builtin_tools(conn)
    seed_predefined_domains(conn)
