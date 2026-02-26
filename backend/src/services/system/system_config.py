from __future__ import annotations

from typing import Optional

from backend.src.repositories.config_repo import (
    fetch_app_config as fetch_app_config_repo,
)
from backend.src.repositories.config_repo import (
    fetch_llm_store_config as fetch_llm_store_config_repo,
)
from backend.src.repositories.config_repo import (
    fetch_permissions_store as fetch_permissions_store_repo,
)
from backend.src.repositories.config_repo import (
    set_llm_store_config as set_llm_store_config_repo,
)
from backend.src.repositories.config_repo import (
    set_permissions_store as set_permissions_store_repo,
)
from backend.src.repositories.config_repo import (
    update_app_config as update_app_config_repo,
)
from backend.src.services.common.coerce import to_text


def fetch_app_config() -> dict:
    return fetch_app_config_repo()


def update_app_config(*, tray_enabled: Optional[int], pet_enabled: Optional[int], panel_enabled: Optional[int]) -> dict:
    return update_app_config_repo(
        tray_enabled=tray_enabled,
        pet_enabled=pet_enabled,
        panel_enabled=panel_enabled,
    )


def fetch_llm_store_config() -> dict:
    return fetch_llm_store_config_repo()


def set_llm_store_config(
    *,
    provider: Optional[str],
    api_key: Optional[str],
    base_url: Optional[str],
    model: Optional[str],
) -> None:
    set_llm_store_config_repo(
        provider=provider,
        api_key=api_key,
        base_url=base_url,
        model=model,
    )


def fetch_permissions_store():
    return fetch_permissions_store_repo()


def set_permissions_store(
    *,
    allowed_paths_json: str,
    allowed_ops_json: str,
    disabled_actions_json: str,
    disabled_tools_json: str,
) -> None:
    set_permissions_store_repo(
        allowed_paths_json=to_text(allowed_paths_json or "[]"),
        allowed_ops_json=to_text(allowed_ops_json or "[]"),
        disabled_actions_json=to_text(disabled_actions_json or "[]"),
        disabled_tools_json=to_text(disabled_tools_json or "[]"),
    )
