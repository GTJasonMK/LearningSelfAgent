from __future__ import annotations

import sqlite3
from typing import Dict, Optional, Tuple

from backend.src.common.utils import coerce_int
from backend.src.constants import SINGLETON_ROW_ID
from backend.src.migrations import run_all_migrations
from backend.src.repositories.repo_conn import provide_connection


def _is_missing_table_error(exc: Exception, table_name: str) -> bool:
    msg = str(exc or "").lower()
    return ("no such table" in msg) and (str(table_name or "").lower() in msg)


def _ensure_config_tables(conn: sqlite3.Connection) -> None:
    """
    仓储层兜底：确保 config_store / permissions_store 存在。

    说明：
    - 正常情况下 storage.get_connection 已会自动 migrations；
    - 但部分脚本/测试可能直接 sqlite3.connect 后把 conn 传入仓储层；
    - 若缺表直接抛异常会导致 API 500/链路中断，因此这里做一次自愈。
    """
    try:
        config_row = conn.execute(
            "SELECT id FROM config_store WHERE id = ?",
            (SINGLETON_ROW_ID,),
        ).fetchone()
        permissions_row = conn.execute(
            "SELECT id FROM permissions_store WHERE id = ?",
            (SINGLETON_ROW_ID,),
        ).fetchone()
    except sqlite3.OperationalError as exc:
        if _is_missing_table_error(exc, "config_store") or _is_missing_table_error(exc, "permissions_store"):
            try:
                # migrations 依赖 sqlite3.Row（例如 PRAGMA table_info 的返回值要支持 col["name"]）
                conn.row_factory = sqlite3.Row
            except Exception:
                pass
            run_all_migrations(conn)
            return
        raise

    # 种子数据兜底：避免用户/脚本误删 singleton 行导致 UPDATE 影响 0 行，从而配置写入“表面成功但实际无效”。
    if not config_row or not permissions_row:
        from backend.src.migrations.seeds import seed_config_store, seed_permissions_store

        seed_config_store(conn)
        seed_permissions_store(conn)


def _row_value(row: object, index: int, key: str):
    """
    读取 sqlite3.Row / tuple 结果中的字段值。

    说明：
    - 正常连接会设置 row_factory=sqlite3.Row；
    - 但为了脚本/测试鲁棒性，这里也兼容 tuple 结果（按 SELECT 顺序取值）。
    """
    try:
        if hasattr(row, "keys"):
            return row[key]
    except Exception:
        pass
    try:
        return row[index]
    except Exception:
        return None


def _fetch_singleton_row(conn: sqlite3.Connection, *, select_sql: str):
    _ensure_config_tables(conn)
    return conn.execute(select_sql, (SINGLETON_ROW_ID,)).fetchone()


def fetch_llm_store_config(
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> Dict[str, Optional[str]]:
    """
    读取 config_store 中的 LLM 配置（provider/api_key/base_url/model）。

    说明：
    - 返回 api_key 明文仅用于后端内部调用（LLMClient）；API 层应自行决定是否回显。
    """
    with provide_connection(conn) as inner:
        row = _fetch_singleton_row(
            inner,
            select_sql="SELECT llm_provider, llm_api_key, llm_base_url, llm_model FROM config_store WHERE id = ?",
        )

    if not row:
        return {"provider": None, "api_key": None, "base_url": None, "model": None}

    provider = (str(_row_value(row, 0, "llm_provider") or "")).strip() or None
    return {
        "provider": provider,
        "api_key": _row_value(row, 1, "llm_api_key"),
        "base_url": _row_value(row, 2, "llm_base_url"),
        "model": _row_value(row, 3, "llm_model"),
    }


def fetch_app_config(
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> Dict[str, Optional[int]]:
    """
    读取 UI 开关配置（tray/pet/panel）。
    """
    with provide_connection(conn) as inner:
        row = _fetch_singleton_row(
            inner,
            select_sql="SELECT tray_enabled, pet_enabled, panel_enabled FROM config_store WHERE id = ?",
        )
    if not row:
        return {"tray_enabled": None, "pet_enabled": None, "panel_enabled": None}
    return {
        "tray_enabled": _row_value(row, 0, "tray_enabled"),
        "pet_enabled": _row_value(row, 1, "pet_enabled"),
        "panel_enabled": _row_value(row, 2, "panel_enabled"),
    }


def update_app_config(
    *,
    tray_enabled: Optional[int],
    pet_enabled: Optional[int],
    panel_enabled: Optional[int],
    conn: Optional[sqlite3.Connection] = None,
) -> Dict[str, int]:
    """
    更新 UI 开关配置（None 表示不修改）。返回更新后的值（0/1）。
    """
    with provide_connection(conn) as inner:
        _ensure_config_tables(inner)
        current = fetch_app_config(conn=inner)
        next_tray = (
            coerce_int(tray_enabled, default=0)
            if tray_enabled is not None
            else coerce_int(current.get("tray_enabled") or 0, default=0)
        )
        next_pet = (
            coerce_int(pet_enabled, default=0)
            if pet_enabled is not None
            else coerce_int(current.get("pet_enabled") or 0, default=0)
        )
        next_panel = (
            coerce_int(panel_enabled, default=0)
            if panel_enabled is not None
            else coerce_int(current.get("panel_enabled") or 0, default=0)
        )

        inner.execute(
            "UPDATE config_store SET tray_enabled = ?, pet_enabled = ?, panel_enabled = ? WHERE id = ?",
            (next_tray, next_pet, next_panel, SINGLETON_ROW_ID),
        )
        return {"tray_enabled": next_tray, "pet_enabled": next_pet, "panel_enabled": next_panel}


def set_llm_store_config(
    *,
    provider: Optional[str],
    api_key: Optional[str],
    base_url: Optional[str],
    model: Optional[str],
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """
    写入 LLM 配置（参数为“最终值”）：允许写入 None 表示清空。
    """
    with provide_connection(conn) as inner:
        _ensure_config_tables(inner)
        inner.execute(
            "UPDATE config_store SET llm_provider = ?, llm_api_key = ?, llm_base_url = ?, llm_model = ? WHERE id = ?",
            (provider, api_key, base_url, model, SINGLETON_ROW_ID),
        )


def fetch_permissions_store(
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[str, str, str, str]:
    """
    返回 (allowed_paths_json, allowed_ops_json, disabled_actions_json, disabled_tools_json)；
    若未初始化则返回 ("[]", "[]", "[]", "[]")。
    """
    with provide_connection(conn) as inner:
        row = _fetch_singleton_row(
            inner,
            select_sql="SELECT allowed_paths, allowed_ops, disabled_actions, disabled_tools FROM permissions_store WHERE id = ?",
        )
    if not row:
        return "[]", "[]", "[]", "[]"
    return (
        _row_value(row, 0, "allowed_paths") or "[]",
        _row_value(row, 1, "allowed_ops") or "[]",
        _row_value(row, 2, "disabled_actions") or "[]",
        _row_value(row, 3, "disabled_tools") or "[]",
    )


def set_permissions_store(
    *,
    allowed_paths_json: str,
    allowed_ops_json: str,
    disabled_actions_json: str,
    disabled_tools_json: str,
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    with provide_connection(conn) as inner:
        _ensure_config_tables(inner)
        inner.execute(
            "UPDATE permissions_store SET allowed_paths = ?, allowed_ops = ?, disabled_actions = ?, disabled_tools = ? WHERE id = ?",
            (allowed_paths_json, allowed_ops_json, disabled_actions_json, disabled_tools_json, SINGLETON_ROW_ID),
        )
