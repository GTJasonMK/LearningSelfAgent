import asyncio
import json
from typing import Optional

from fastapi import APIRouter

from backend.src.common.app_error_utils import app_error_response
from backend.src.common.errors import AppError
from backend.src.api.schemas import ConfigUpdate, LLMConfigUpdate, PermissionsUpdate
from backend.src.api.utils import as_bool, parse_json_list, require_write_permission
from backend.src.actions.registry import export_action_contract_schema, list_action_types
from backend.src.constants import (
    ERROR_CODE_INVALID_REQUEST,
    HTTP_STATUS_BAD_REQUEST,
    LLM_PROVIDER_OPENAI,
)
from backend.src.common.utils import error_response
from backend.src.services.llm.llm_client import LLMClient
from backend.src.services.permissions.permissions_store import get_permission_policy_matrix
from backend.src.services.system.system_config import (
    fetch_app_config,
    fetch_llm_store_config,
    fetch_permissions_store,
    set_llm_store_config,
    set_permissions_store,
    update_app_config,
)

router = APIRouter()
LLM_CONFIG_TEST_PROMPT = "请仅回复：OK"


def _coalesce_flag_int(value: Optional[bool], *, current_value) -> int:
    if value is None:
        return int(current_value)
    return int(bool(value))


def _normalize_optional_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _resolve_next_llm_config(payload: LLMConfigUpdate, current: dict) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    current_provider = current.get("provider")
    current_key = current.get("api_key")
    current_base_url = current.get("base_url")
    current_model = current.get("model")

    # None 表示不修改；空字符串表示清空
    next_provider = current_provider
    if payload.provider is not None:
        next_provider = _normalize_optional_text(payload.provider)

    next_key = current_key
    if payload.api_key is not None:
        next_key = _normalize_optional_text(payload.api_key)

    next_base_url = current_base_url
    if payload.base_url is not None:
        next_base_url = _normalize_optional_text(payload.base_url)

    next_model = current_model
    if payload.model is not None:
        next_model = _normalize_optional_text(payload.model)

    return next_provider, next_key, next_base_url, next_model


def _coalesce_json_list_field(value: Optional[list], *, current_json: Optional[str]) -> Optional[str]:
    if value is None:
        return current_json
    return json.dumps(value)


def _permissions_payload_from_json_fields(
    *,
    allowed_paths: Optional[str],
    allowed_ops: Optional[str],
    disabled_actions: Optional[str],
    disabled_tools: Optional[str],
) -> dict:
    return {
        "allowed_paths": parse_json_list(allowed_paths),
        "allowed_ops": parse_json_list(allowed_ops),
        "disabled_actions": parse_json_list(disabled_actions),
        "disabled_tools": parse_json_list(disabled_tools),
    }


@router.get("/config")
def get_config() -> dict:
    row = fetch_app_config()
    return {
        "tray_enabled": as_bool(row["tray_enabled"]),
        "pet_enabled": as_bool(row["pet_enabled"]),
        "panel_enabled": as_bool(row["panel_enabled"]),
    }


@router.patch("/config")
@require_write_permission
def update_config(payload: ConfigUpdate) -> dict:
    current = fetch_app_config()
    tray_enabled = _coalesce_flag_int(payload.tray_enabled, current_value=current["tray_enabled"])
    pet_enabled = _coalesce_flag_int(payload.pet_enabled, current_value=current["pet_enabled"])
    panel_enabled = _coalesce_flag_int(payload.panel_enabled, current_value=current["panel_enabled"])
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
@require_write_permission
def update_llm_config(payload: LLMConfigUpdate) -> dict:
    current = fetch_llm_store_config()
    next_provider, next_key, next_base_url, next_model = _resolve_next_llm_config(payload, current)

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


@router.post("/config/llm/test")
async def test_llm_config(payload: LLMConfigUpdate) -> dict:
    """
    测试 LLM 配置可用性（不落库）。

    用途：
    - 前端设置页“测试可用性”按钮；
    - 支持传入临时 provider/api_key/base_url/model 进行探测。
    """
    current = fetch_llm_store_config()
    next_provider, next_key, next_base_url, next_model = _resolve_next_llm_config(payload, current)

    client = None
    try:
        client = await asyncio.to_thread(
            LLMClient,
            provider=next_provider,
            api_key=next_key,
            base_url=next_base_url,
            default_model=next_model,
            strict_mode=True,
        )
        model_to_use = str(next_model or getattr(client, "_default_model", "") or "").strip() or None
        text, tokens = await client.complete_prompt(
            prompt=LLM_CONFIG_TEST_PROMPT,
            model=model_to_use,
            parameters={"temperature": 0, "max_tokens": 24},
            timeout=20,
        )
        preview = str(text or "").strip()
        if not preview:
            return error_response(
                ERROR_CODE_INVALID_REQUEST,
                "LLM返回为空，请检查模型可用性",
                HTTP_STATUS_BAD_REQUEST,
            )
        return {
            "ok": True,
            "provider": (str(next_provider or "").strip() or None) or LLM_PROVIDER_OPENAI,
            "base_url": next_base_url,
            "model": model_to_use,
            "response_preview": preview[:160],
            "tokens": tokens,
        }
    except AppError as exc:
        return app_error_response(exc)
    except Exception as exc:
        return error_response(
            ERROR_CODE_INVALID_REQUEST,
            str(exc) or "LLM连通性测试失败",
            HTTP_STATUS_BAD_REQUEST,
        )
    finally:
        if client is not None:
            try:
                await client.aclose()
            except Exception:
                pass


@router.get("/permissions")
def get_permissions() -> dict:
    allowed_paths, allowed_ops, disabled_actions, disabled_tools = fetch_permissions_store()
    return _permissions_payload_from_json_fields(
        allowed_paths=allowed_paths,
        allowed_ops=allowed_ops,
        disabled_actions=disabled_actions,
        disabled_tools=disabled_tools,
    )


@router.get("/permissions/matrix")
def get_permissions_matrix() -> dict:
    return {"matrix": get_permission_policy_matrix()}


@router.patch("/permissions")
@require_write_permission
def update_permissions(payload: PermissionsUpdate) -> dict:
    current_paths, current_ops, current_disabled_actions, current_disabled_tools = fetch_permissions_store()
    allowed_paths = _coalesce_json_list_field(payload.allowed_paths, current_json=current_paths)
    allowed_ops = _coalesce_json_list_field(payload.allowed_ops, current_json=current_ops)
    disabled_actions = _coalesce_json_list_field(
        payload.disabled_actions,
        current_json=current_disabled_actions,
    )
    disabled_tools = _coalesce_json_list_field(
        payload.disabled_tools,
        current_json=current_disabled_tools,
    )
    set_permissions_store(
        allowed_paths_json=allowed_paths,
        allowed_ops_json=allowed_ops,
        disabled_actions_json=disabled_actions,
        disabled_tools_json=disabled_tools,
    )
    return {
        "permissions": _permissions_payload_from_json_fields(
            allowed_paths=allowed_paths,
            allowed_ops=allowed_ops,
            disabled_actions=disabled_actions,
            disabled_tools=disabled_tools,
        )
    }


@router.get("/actions")
def list_actions() -> dict:
    return {"items": list_action_types()}


@router.get("/actions/schema")
def get_actions_schema() -> dict:
    return {"schema": export_action_contract_schema()}
