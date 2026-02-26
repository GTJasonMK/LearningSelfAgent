from __future__ import annotations

from typing import Optional

from backend.src.repositories.update_records_repo import (
    create_update_record as create_update_record_repo,
)
from backend.src.repositories.update_records_repo import (
    list_update_records as list_update_records_repo,
)
from backend.src.repositories.update_records_repo import (
    update_update_record as update_update_record_repo,
)
from backend.src.services.common.coerce import to_int, to_optional_text, to_text


def create_update_record(*, status: str, notes: Optional[str]):
    return create_update_record_repo(
        status=to_text(status),
        notes=to_optional_text(notes),
    )


def update_update_record(*, record_id: int, status: str, notes: Optional[str] = None) -> None:
    update_update_record_repo(
        record_id=to_int(record_id),
        status=to_text(status),
        notes=to_optional_text(notes),
    )


def list_update_records(*, limit: int):
    return list_update_records_repo(limit=to_int(limit))
