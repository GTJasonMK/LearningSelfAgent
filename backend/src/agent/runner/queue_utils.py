from __future__ import annotations

import asyncio
from typing import Callable


def make_queue_emit(out_q: "asyncio.Queue[str]") -> Callable[[str], None]:
    """
    构造统一的 SSE 消息入队函数。
    """

    def _emit(msg: str) -> None:
        try:
            if msg:
                out_q.put_nowait(str(msg))
        except Exception:
            return

    return _emit
