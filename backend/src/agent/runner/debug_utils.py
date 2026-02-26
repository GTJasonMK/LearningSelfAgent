from __future__ import annotations

from typing import Optional

from backend.src.services.debug.safe_debug import safe_write_debug as _safe_write_debug_impl


def safe_write_debug(
    task_id: Optional[int],
    run_id: Optional[int],
    *,
    message: str,
    data: Optional[dict] = None,
    level: str = "debug",
) -> None:
    """
    调试输出公共入口。

    约束：
    - 调试写入失败不影响主链路；
    - 所有入口统一通过该函数写 task_outputs(debug)。
    """
    _safe_write_debug_impl(
        task_id=task_id,
        run_id=run_id,
        message=message,
        data=data,
        level=level,
    )
