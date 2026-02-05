import json

from fastapi import APIRouter

from backend.src.api.schemas import ConfigUpdate, LLMConfigUpdate, PermissionsUpdate
from backend.src.api.utils import as_bool, ensure_write_permission, parse_json_list
from backend.src.actions.registry import list_action_types
from backend.src.constants import LLM_PROVIDER_OPENAI
from backend.src.repositories.config_repo import (
    fetch_app_config,
    fetch_llm_store_config,
    fetch_permissions_store,
    set_llm_store_config,
    set_permissions_store,
    update_app_config,
)

router = APIRouter()


@router.get("/config")
def get_config() -> dict:
    row = fetch_app_config()
    return {
        "tray_enabled": as_bool(row["tray_enabled"]),
        "pet_enabled": as_bool(row["pet_enabled"]),
        "panel_enabled": as_bool(row["panel_enabled"]),
    }


@router.patch("/config")
def update_config(payload: ConfigUpdate) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    current = fetch_app_config()
    tray_enabled = int(payload.tray_enabled) if payload.tray_enabled is not None else current["tray_enabled"]
    pet_enabled = int(payload.pet_enabled) if payload.pet_enabled is not None else current["pet_enabled"]
    panel_enabled = (
        int(payload.panel_enabled) if payload.panel_enabled is not None else current["panel_enabled"]
    )
    updated = update_app_config(
        tray_enabled=tray_enabled,
        pet_enabled=pet_enabled,
        panel_enabled=panel_enabled,
    )
    return {
        "config": {
            "tray_enabled": as_bool(updated["tray_enabled"]),
            "pet_enabled": as_bool(updated["pet_enabled"]),
            "panel_enabled": as_bool(updated["panel_enabled"]),
        }
    }


@router.get("/config/llm")
def get_llm_config() -> dict:
    config = fetch_llm_store_config()
    provider = config.get("provider")
    api_key = config.get("api_key")
    base_url = config.get("base_url")
    model = config.get("model")
    return {
        # 不回显明文 key，避免误展示；前端用 api_key_set 判断是否已配置
        "provider": (str(provider or "").strip() or None) or LLM_PROVIDER_OPENAI,
        "api_key_set": bool(api_key),
        "base_url": base_url,
        "model": model,
    }


@router.patch("/config/llm")
def update_llm_config(payload: LLMConfigUpdate) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission

    def normalize_optional_text(value: str):
        if value is None:
            return None
        text = str(value).strip()
        return text if text else None

    current = fetch_llm_store_config()
    current_provider = current.get("provider")
    current_key = current.get("api_key")
    current_base_url = current.get("base_url")
    current_model = current.get("model")

    # None 表示不修改；空字符串表示清空
    next_provider = current_provider
    if payload.provider is not None:
        next_provider = normalize_optional_text(payload.provider)
    next_key = current_key
    if payload.api_key is not None:
        next_key = normalize_optional_text(payload.api_key)
    next_base_url = current_base_url
    if payload.base_url is not None:
        next_base_url = normalize_optional_text(payload.base_url)
    next_model = current_model
    if payload.model is not None:
        next_model = normalize_optional_text(payload.model)

    set_llm_store_config(
        provider=next_provider,
        api_key=next_key,
        base_url=next_base_url,
        model=next_model,
    )
    return {
        "llm_config": {
            "provider": (str(next_provider or "").strip() or None) or LLM_PROVIDER_OPENAI,
            "api_key_set": bool(next_key),
            "base_url": next_base_url,
            "model": next_model,
        }
    }


@router.get("/permissions")
def get_permissions() -> dict:
    allowed_paths, allowed_ops, disabled_actions, disabled_tools = fetch_permissions_store()
    return {
        "allowed_paths": parse_json_list(allowed_paths),
        "allowed_ops": parse_json_list(allowed_ops),
        "disabled_actions": parse_json_list(disabled_actions),
        "disabled_tools": parse_json_list(disabled_tools),
    }


@router.patch("/permissions")
def update_permissions(payload: PermissionsUpdate) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    current_paths, current_ops, current_disabled_actions, current_disabled_tools = fetch_permissions_store()
    allowed_paths = json.dumps(payload.allowed_paths) if payload.allowed_paths is not None else current_paths
    allowed_ops = json.dumps(payload.allowed_ops) if payload.allowed_ops is not None else current_ops
    disabled_actions = (
        json.dumps(payload.disabled_actions) if payload.disabled_actions is not None else current_disabled_actions
    )
    disabled_tools = (
        json.dumps(payload.disabled_tools) if payload.disabled_tools is not None else current_disabled_tools
    )
    set_permissions_store(
        allowed_paths_json=allowed_paths,
        allowed_ops_json=allowed_ops,
        disabled_actions_json=disabled_actions,
        disabled_tools_json=disabled_tools,
    )
    return {
        "permissions": {
            "allowed_paths": parse_json_list(allowed_paths),
            "allowed_ops": parse_json_list(allowed_ops),
            "disabled_actions": parse_json_list(disabled_actions),
            "disabled_tools": parse_json_list(disabled_tools),
        }
    }


@router.get("/actions")
def list_actions() -> dict:
    return {"items": list_action_types()}
