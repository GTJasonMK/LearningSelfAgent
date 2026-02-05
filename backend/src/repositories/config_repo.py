from __future__ import annotations

import sqlite3
from typing import Dict, Optional, Tuple

from backend.src.constants import SINGLETON_ROW_ID
from backend.src.repositories.repo_conn import provide_connection


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
        row = inner.execute(
            "SELECT llm_provider, llm_api_key, llm_base_url, llm_model FROM config_store WHERE id = ?",
            (SINGLETON_ROW_ID,),
        ).fetchone()

    if not row:
        return {"provider": None, "api_key": None, "base_url": None, "model": None}

    provider = (row["llm_provider"] or "").strip() or None
    return {
        "provider": provider,
        "api_key": row["llm_api_key"],
        "base_url": row["llm_base_url"],
        "model": row["llm_model"],
    }


def fetch_app_config(
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> Dict[str, Optional[int]]:
    """
    读取 UI 开关配置（tray/pet/panel）。
    """
    with provide_connection(conn) as inner:
        row = inner.execute(
            "SELECT tray_enabled, pet_enabled, panel_enabled FROM config_store WHERE id = ?",
            (SINGLETON_ROW_ID,),
        ).fetchone()
    if not row:
        return {"tray_enabled": None, "pet_enabled": None, "panel_enabled": None}
    return {
        "tray_enabled": row["tray_enabled"],
        "pet_enabled": row["pet_enabled"],
        "panel_enabled": row["panel_enabled"],
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
        current = fetch_app_config(conn=inner)
        next_tray = int(tray_enabled) if tray_enabled is not None else int(current.get("tray_enabled") or 0)
        next_pet = int(pet_enabled) if pet_enabled is not None else int(current.get("pet_enabled") or 0)
        next_panel = int(panel_enabled) if panel_enabled is not None else int(current.get("panel_enabled") or 0)

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
        row = inner.execute(
            "SELECT allowed_paths, allowed_ops, disabled_actions, disabled_tools FROM permissions_store WHERE id = ?",
            (SINGLETON_ROW_ID,),
        ).fetchone()
    if not row:
        return "[]", "[]", "[]", "[]"
    return (
        row["allowed_paths"] or "[]",
        row["allowed_ops"] or "[]",
        row["disabled_actions"] or "[]",
        row["disabled_tools"] or "[]",
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
        inner.execute(
            "UPDATE permissions_store SET allowed_paths = ?, allowed_ops = ?, disabled_actions = ?, disabled_tools = ? WHERE id = ?",
            (allowed_paths_json, allowed_ops_json, disabled_actions_json, disabled_tools_json, SINGLETON_ROW_ID),
        )
