import os
import subprocess
import sys
import threading
from typing import Optional

from fastapi import APIRouter

from backend.src.api.schemas import UpdateRequest
from backend.src.api.utils import ensure_write_permission
from backend.src.constants import (
    DEFAULT_PAGE_LIMIT,
    UPDATE_STATUS_FAILED,
    UPDATE_STATUS_QUEUED,
    UPDATE_STATUS_RESTARTING,
)
from backend.src.repositories.update_records_repo import (
    create_update_record,
    list_update_records as list_update_records_repo,
    update_update_record,
)
from backend.src.services.tasks.task_recovery import stop_running_task_records

router = APIRouter()


@router.post("/update/restart")
def restart_update(payload: Optional[UpdateRequest] = None) -> dict:
    permission = ensure_write_permission()
    if permission:
        return permission
    notes = payload.notes if payload else None
    record_id, created_at = create_update_record(status=UPDATE_STATUS_QUEUED, notes=notes)
    response = {
        "update": {
            "id": record_id,
            "status": UPDATE_STATUS_QUEUED,
            "notes": notes,
            "created_at": created_at,
        }
    }

    def _restart():
        update_update_record(record_id=int(record_id), status=UPDATE_STATUS_RESTARTING)
        try:
            # 重启前先把 running/waiting 收敛为 stopped，避免重启后 UI 卡在“执行中/等待输入”。
            # 注意：即使收敛失败也不应阻塞重启流程。
            try:
                stop_running_task_records(reason="update_restart")
            except Exception:
                pass

            # 兼容不同启动方式：
            # - scripts/start.py 使用 python -m uvicorn：sys.argv[0] 通常是 uvicorn/__main__.py（可直接执行）
            # - 直接 uvicorn 启动时 sys.argv[0] 可能是 "uvicorn"（不是文件），需要转为 python -m uvicorn
            argv0 = str(sys.argv[0] or "") if sys.argv else ""
            base0 = os.path.basename(argv0).lower()
            is_file = False
            try:
                is_file = bool(argv0) and os.path.exists(argv0)
            except Exception:
                is_file = False
            if (not is_file) and base0 in {"uvicorn", "uvicorn.exe"}:
                command = [sys.executable, "-m", "uvicorn", *sys.argv[1:]]
            else:
                command = [sys.executable, *sys.argv]
            subprocess.Popen(
                command,
                cwd=os.getcwd(),
                env=os.environ.copy(),
            )
        except Exception as exc:
            update_update_record(
                record_id=int(record_id),
                status=UPDATE_STATUS_FAILED,
                notes=f"{notes or ''} restart_error:{exc}",
            )
            return
        os._exit(0)

    threading.Timer(0.5, _restart).start()
    return response


@router.get("/update/records")
def list_update_records(limit: int = DEFAULT_PAGE_LIMIT) -> dict:
    rows = list_update_records_repo(limit=int(limit))
    return {
        "items": [
            {
                "id": row["id"],
                "status": row["status"],
                "notes": row["notes"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]
    }
