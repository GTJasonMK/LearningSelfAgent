# -*- coding: utf-8 -*-
"""
后端 API 客户端封装。

管理 httpx 客户端实例和基础 URL，封装 GET/POST/PATCH/DELETE 方法，
统一错误处理和响应解析，提供 SSE 流式请求方法。
"""

from __future__ import annotations

import os
from typing import Any, Dict, Iterator, Optional

try:
    import httpx  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - 依赖缺失时给出可读错误
    class _MissingHttpxModule:
        class ConnectError(Exception):
            pass

        class TimeoutException(Exception):
            pass

        class RequestError(Exception):
            pass

        class Timeout:
            def __init__(self, *args, **kwargs):
                pass

        class Client:  # noqa: D401 - 占位类型，实际不会使用
            def __init__(self, *args, **kwargs):
                raise ModuleNotFoundError("No module named 'httpx'")

    httpx = _MissingHttpxModule()  # type: ignore[assignment]

from backend.src.cli.sse import SseEvent, iter_sse_stream


class CliError(Exception):
    """CLI 层统一异常，携带退出码。"""

    def __init__(self, message: str, exit_code: int = 1):
        super().__init__(message)
        self.exit_code = exit_code


def _resolve_port(port_arg: Optional[int] = None) -> int:
    """
    解析后端端口（与 scripts/start.py 的逻辑对齐）。

    优先级：命令行参数 > 环境变量 LSA_BACKEND_PORT > 默认 8123
    """
    if port_arg is not None:
        return port_arg
    env_port = os.environ.get("LSA_BACKEND_PORT")
    if env_port:
        try:
            val = int(str(env_port).strip())
            if 1 <= val <= 65535:
                return val
        except ValueError:
            pass
    return 8123


def _extract_error_message(response: Any) -> str:
    """从后端错误响应中提取可读错误信息。"""
    try:
        data = response.json()
        # 后端 AppError 格式：{"error": {"code": "...", "message": "..."}}
        if isinstance(data, dict):
            error = data.get("error")
            if isinstance(error, dict):
                code = error.get("code", "")
                msg = error.get("message", "")
                if code and msg:
                    return f"{code}: {msg}"
                return msg or code
            # 也可能直接是 {"detail": "..."}（FastAPI 默认格式）
            detail = data.get("detail")
            if detail:
                return str(detail)
            message = data.get("message")
            if message:
                return str(message)
    except Exception:
        pass
    return response.text[:200] if response.text else f"HTTP {response.status_code}"


class ApiClient:
    """后端 API HTTP 客户端。"""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: Optional[int] = None,
        timeout: int = 30,
    ):
        resolved_port = _resolve_port(port)
        self._base_url = f"http://{host}:{resolved_port}/api"
        self._timeout = timeout

    def _make_client(self, **kwargs) -> httpx.Client:
        """创建 httpx 客户端，绕过系统代理（CLI 只访问本地后端）。"""
        return httpx.Client(trust_env=False, **kwargs)

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """发送 HTTP 请求并返回 JSON 响应。"""
        url = f"{self._base_url}{path}"
        try:
            with self._make_client(timeout=self._timeout) as client:
                response = client.request(
                    method, url, params=params, json=json_data
                )
        except httpx.ConnectError:
            raise CliError(
                f"无法连接到后端服务 ({self._base_url})。\n"
                "请确认后端已启动，可执行: python scripts/start.py",
                exit_code=2,
            )
        except httpx.TimeoutException:
            raise CliError(
                f"请求超时 ({self._timeout}s)。可通过 --timeout 增大超时时间。",
                exit_code=1,
            )
        except httpx.RequestError as exc:
            raise CliError(f"网络请求失败: {exc}", exit_code=1)
        except ModuleNotFoundError:
            raise CliError(
                "缺少依赖 httpx，请先运行: python scripts/install.py",
                exit_code=2,
            )

        if response.status_code >= 400:
            msg = _extract_error_message(response)
            raise CliError(f"HTTP {response.status_code}: {msg}", exit_code=1)

        try:
            return response.json()
        except Exception:
            return {"raw": response.text}

    def get(
        self, path: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        return self._request("GET", path, params=params)

    def post(
        self,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self._request("POST", path, json_data=json_data)

    def patch(
        self,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self._request("PATCH", path, json_data=json_data)

    def delete(self, path: str) -> Dict[str, Any]:
        return self._request("DELETE", path)

    def stream_post(
        self,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> Iterator[SseEvent]:
        """
        发送 POST 请求并返回 SSE 事件流迭代器。

        SSE 连接不设读超时（后端处理可能耗时较长）。
        """
        url = f"{self._base_url}{path}"
        try:
            # 使用较长的连接超时但不限制读超时
            timeout = httpx.Timeout(connect=self._timeout, read=None, write=30.0, pool=None)
            with self._make_client(timeout=timeout) as client:
                with client.stream(
                    "POST",
                    url,
                    json=json_data,
                    headers={"Accept": "text/event-stream"},
                ) as response:
                    if response.status_code >= 400:
                        # 非流式错误响应：读取全部内容
                        response.read()
                        msg = _extract_error_message(response)
                        raise CliError(
                            f"HTTP {response.status_code}: {msg}", exit_code=1
                        )
                    yield from iter_sse_stream(response.iter_text())
        except CliError:
            raise
        except httpx.ConnectError:
            raise CliError(
                f"无法连接到后端服务 ({self._base_url})。\n"
                "请确认后端已启动，可执行: python scripts/start.py",
                exit_code=2,
            )
        except httpx.TimeoutException:
            raise CliError("连接超时。", exit_code=1)
        except httpx.RequestError as exc:
            raise CliError(f"流式请求失败: {exc}", exit_code=1)
        except ModuleNotFoundError:
            raise CliError(
                "缺少依赖 httpx，请先运行: python scripts/install.py",
                exit_code=2,
            )
