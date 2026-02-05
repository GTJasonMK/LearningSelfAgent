from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class AppError(Exception):
    """
    统一的业务异常（服务层抛出，API 层捕获并转换为 HTTP 错误响应）。

    设计目标：
    - 服务层不直接依赖 FastAPI（不返回 JSONResponse），只表达“错误是什么”；
    - API 层统一处理错误协议（error.code/error.message/status_code）。
    """

    code: str
    message: str
    status_code: int
    details: Optional[dict] = None

