from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional, Dict


def now_iso() -> str:
    """
    统一生成 UTC 的 ISO8601 时间戳（以 Z 结尾）。

    说明：
    - 统一使用 datetime.now(timezone.utc)，避免 datetime.utcnow() 在新版本 Python 中的弃用告警；
    - 前端排序/展示时更稳定（字符串可直接比较）。
    """
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def is_test_env() -> bool:
    """
    判断当前是否处于测试环境。

    用途：
    - 部分后台线程（postprocess/stop-running 等）在测试用例中容易造成 TemporaryDirectory 清理竞态；
    - 在测试环境下可选择同步执行这些后台任务，避免跨用例的线程干扰。
    """
    try:
        flag = str(os.getenv("LSA_TEST_MODE") or "").strip().lower()
        if flag in {"1", "true", "yes"}:
            return True
        if os.getenv("PYTEST_CURRENT_TEST"):
            return True
        # unittest：`python -m unittest ...`（许多用例用 TemporaryDirectory 隔离 DB/Prompt Root）
        return any("unittest" in str(arg).lower() for arg in sys.argv)
    except Exception:
        return False


def as_bool(value: Optional[int]) -> Optional[bool]:
    """
    SQLite 常用的 0/1 -> bool 转换。

    说明：
    - value=None 表示字段为空（保持 None）；
    - 该函数位于 common 层，避免 services/actions 反向依赖 api.utils。
    """
    if value is None:
        return None
    return bool(value)


def error_response(code: str, message: str, status_code: int) -> "JSONResponse":
    """
    统一错误响应结构：{"error": {"code": "...", "message": "..."}}。
    """
    # 懒导入：避免通用工具模块在被离线单测/脚本使用时强依赖 fastapi。
    from fastapi.responses import JSONResponse

    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "message": message}},
    )


def parse_json_list(value: Optional[str]) -> List[Any]:
    """
    解析 JSON 列表；失败/为空返回 []。
    """
    if not value:
        return []
    try:
        out = json.loads(value)
    except json.JSONDecodeError:
        return []
    return out if isinstance(out, list) else []


def parse_json_value(value: Optional[str]) -> Any:
    """
    解析任意 JSON 值；失败/为空返回 None。
    """
    if not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def coerce_str_list(value: Any, max_items: int = 64) -> List[str]:
    """
    将任意值尽量转换为“字符串列表”。

    说明：
    - 支持 list/tuple 等序列：逐项转为 str，去掉空白项，最多保留 max_items；
    - 非序列：转为单个字符串（非空才返回）。
    - 用于 skills 的 tags/triggers/aliases 等字段归一化，避免各处重复实现。
    """
    if value is None:
        return []

    items: List[str] = []
    if isinstance(value, list):
        for item in value:
            text = str(item).strip()
            if text:
                items.append(text)
            if len(items) >= max_items:
                break
        return items

    text = str(value).strip()
    return [text] if text else []


def dump_model(obj: Any) -> Dict[str, Any]:
    """
    将 Pydantic/Dataclass/字典 等对象尽量转换为 dict。

    说明：
    - services/actions 层避免强依赖 pydantic：通过 duck-typing 读取 payload；
    - 兼容 pydantic v1 `.dict()` 与 v2 `.model_dump()`。
    """
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    try:
        if hasattr(obj, "model_dump"):
            out = obj.model_dump()  # type: ignore[attr-defined]
            return out if isinstance(out, dict) else {}
        if hasattr(obj, "dict"):
            out = obj.dict()  # type: ignore[attr-defined]
            return out if isinstance(out, dict) else {}
    except Exception:
        return {}
    try:
        out = dict(obj)
        return out if isinstance(out, dict) else {}
    except Exception:
        return {}


def extract_json_object(text: str) -> Optional[dict]:
    """
    从文本中尽量提取 JSON 对象（dict）。

    设计目标：
    - 允许模型输出前后夹带少量文字（尽量容错）；
    - 允许 ```json ...``` 代码块；
    - 避免“多个 JSON 对象/包含大段代码”时用 first{..}last} 误切片；
    - 仅返回 dict；失败返回 None。
    """
    if not text:
        return None

    raw = str(text)

    def _extract_first_balanced_object(candidate: str) -> Optional[str]:
        start = candidate.find("{")
        if start == -1:
            return None

        in_str = False
        escape = False
        depth = 0
        for i in range(start, len(candidate)):
            ch = candidate[i]
            if in_str:
                if escape:
                    escape = False
                    continue
                if ch == "\\":
                    escape = True
                    continue
                if ch == "\"":
                    in_str = False
                continue

            if ch == "\"":
                in_str = True
                continue
            if ch == "{":
                depth += 1
                continue
            if ch == "}":
                depth -= 1
                if depth == 0:
                    return candidate[start : i + 1]
                continue
        return None

    # 1) 直接解析（最快路径）
    try:
        out = json.loads(raw)
        return out if isinstance(out, dict) else None
    except json.JSONDecodeError:
        pass

    # 2) 生成候选文本（含 code fence）
    candidates: List[str] = []
    stripped = raw.strip()
    if stripped:
        candidates.append(stripped)

    if "```" in raw:
        parts = raw.split("```")
        for i in range(1, len(parts), 2):
            block = str(parts[i] or "")
            lines = block.splitlines()
            if lines:
                first = str(lines[0] or "").strip().lower()
                if first in {"json", "json5", "javascript", "js"}:
                    block = "\n".join(lines[1:])
            block = block.strip()
            if block:
                candidates.append(block)

    # 3) 尝试从候选中解析，必要时抽取第一个“括号平衡”的对象片段
    for cand in candidates:
        try:
            out = json.loads(cand)
            if isinstance(out, dict):
                return out
        except json.JSONDecodeError:
            pass

        sliced = _extract_first_balanced_object(cand)
        if not sliced:
            continue
        try:
            out = json.loads(sliced)
            if isinstance(out, dict):
                return out
        except json.JSONDecodeError:
            continue

    # 4) 最后兜底：旧逻辑（first{..}last}）
    start = raw.find("{")
    end = raw.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            out = json.loads(raw[start : end + 1])
            return out if isinstance(out, dict) else None
        except json.JSONDecodeError:
            return None

    return None


def extract_json_value(text: str) -> Optional[Any]:
    """
    从文本中尽量提取 JSON 值（dict/list/primitive）。

    设计目标：
    - 允许模型输出前后夹带少量文字（尽量容错）；
    - 允许 ```json ...``` 代码块；
    - 失败返回 None。
    """
    if not text:
        return None

    raw = str(text)
    candidates: List[str] = []

    stripped = raw.strip()
    if stripped:
        candidates.append(stripped)

    # 代码块：```json ...``` 或 ``` ... ```
    if "```" in raw:
        parts = raw.split("```")
        for i in range(1, len(parts), 2):
            block = str(parts[i] or "")
            lines = block.splitlines()
            if lines:
                first = str(lines[0] or "").strip().lower()
                if first in {"json", "json5", "javascript", "js"}:
                    block = "\n".join(lines[1:])
            block = block.strip()
            if block:
                candidates.append(block)

    def _append_bracket_slice(open_ch: str, close_ch: str) -> None:
        start = raw.find(open_ch)
        end = raw.rfind(close_ch)
        if start == -1 or end == -1 or end <= start:
            return
        sliced = raw[start : end + 1].strip()
        if sliced and sliced not in candidates:
            candidates.append(sliced)

    _append_bracket_slice("{", "}")
    _append_bracket_slice("[", "]")

    for cand in candidates:
        try:
            return json.loads(cand)
        except Exception:
            continue
    return None


def truncate_text(
    text: str,
    max_chars: int,
    *,
    suffix: str = "...",
    strip: bool = True,
) -> str:
    """
    截断文本到 max_chars（包含 suffix），用于 UI/日志体积控制。
    """
    if max_chars <= 0:
        return ""
    value = str(text or "")
    value = value.strip() if strip else value
    if not value:
        return ""
    if len(value) <= max_chars:
        return value
    if not suffix:
        return value[:max_chars]
    if max_chars <= len(suffix):
        return suffix[:max_chars]
    keep = max_chars - len(suffix)
    return value[:keep].rstrip() + suffix


def render_prompt(template: str, variables: Optional[dict]) -> Optional[str]:
    """
    渲染提示词模板：template.format_map(variables)；缺变量返回 None。
    """
    if not variables:
        return template
    try:
        return template.format_map(variables)
    except KeyError:
        return None


def atomic_write_text(path: "str | Path", text: str, *, encoding: str = "utf-8") -> None:
    """
    原子写文件：先写临时文件，再 os.replace 覆盖目标文件，避免半写入导致文件损坏。
    """
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding=encoding,
            delete=False,
            dir=str(target.parent),
            prefix=f".{target.name}.",
            suffix=".tmp",
        ) as f:
            tmp_fd = f
            tmp_path = f.name
            f.write(text)
        os.replace(tmp_path, str(target))
    finally:
        # 若 replace 前出错，尽量清理临时文件
        try:
            if tmp_path and Path(tmp_path).exists():
                Path(tmp_path).unlink()
        except Exception:
            pass


def atomic_write_json(
    path: "str | Path",
    obj: Any,
    *,
    encoding: str = "utf-8",
    ensure_ascii: bool = False,
    indent: int = 2,
    sort_keys: bool = True,
) -> None:
    """
    原子写 JSON 文件（UTF-8）。
    """
    text = json.dumps(obj, ensure_ascii=ensure_ascii, indent=indent, sort_keys=sort_keys) + "\n"
    atomic_write_text(path, text, encoding=encoding)
