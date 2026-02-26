from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


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


def dump_json_list(value: Optional[Sequence[Any]], *, ensure_ascii: bool = False) -> str:
    """
    将序列统一序列化为 JSON 列表字符串。
    """
    return json.dumps(list(value or []), ensure_ascii=ensure_ascii)


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


def parse_json_dict(value: Any) -> Optional[Dict[str, Any]]:
    """
    解析 JSON 对象（dict）；失败或非对象返回 None。

    支持：
    - 直接传入 dict（原样返回）；
    - 传入 JSON 字符串（调用 parse_json_value 解析）。
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = parse_json_value(value)
        return parsed if isinstance(parsed, dict) else None
    return None


def tool_approval_status(
    metadata: Any,
    *,
    default: str = "",
    approval_key: str = "approval",
) -> str:
    """
    从工具 metadata 中读取审批状态（approval.status）。

    说明：
    - metadata 可为 dict 或 JSON 字符串；
    - 缺失/无效时返回 default；
    - 返回值统一为小写字符串。
    """
    meta = parse_json_dict(metadata)
    if not meta:
        return str(default or "").strip().lower()
    approval = meta.get(str(approval_key or "approval"))
    if not isinstance(approval, dict):
        return str(default or "").strip().lower()
    status = str(approval.get("status") or "").strip().lower()
    return status or str(default or "").strip().lower()


def tool_is_draft(metadata: Any, *, approval_key: str = "approval") -> bool:
    """
    判断工具 metadata 是否为 draft 审批状态。
    """
    return tool_approval_status(metadata, approval_key=approval_key) == "draft"


def bump_semver_patch(version: Optional[str], *, default_version: str) -> str:
    """
    语义化版本号 x.y.z 的 patch + 1；不符合格式时返回 default_version。
    """
    value = str(version or "").strip()
    parts = value.split(".")
    if len(parts) != 3 or any((not p.isdigit()) for p in parts):
        return str(default_version or "0.1.0")
    major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2])
    return f"{major}.{minor}.{patch + 1}"


def dedupe_keep_order(items: List[Any]) -> List[Any]:
    """
    对任意列表去重并保留原顺序（支持 str/dict/list 等混合元素）。
    """
    seen = set()
    out: List[Any] = []
    for item in list(items or []):
        try:
            key = json.dumps(item, ensure_ascii=False, sort_keys=True)
        except TypeError:
            key = str(item)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def action_type_from_step_detail(detail: Any) -> Optional[str]:
    """
    从 step.detail 中提取 action.type（兼容 {"type": "..."} 与 {"action": {"type": "..."}}）。
    """
    obj = parse_json_dict(detail)
    if not obj:
        return None
    nested = obj.get("action")
    nested_type = nested.get("type") if isinstance(nested, dict) else None
    raw = obj.get("type") or nested_type
    text = str(raw or "").strip()
    return text or None


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


def parse_positive_int(value: Any, *, default: Optional[int] = None) -> Optional[int]:
    """
    尝试将 value 解析为正整数；失败或非正数时返回 default。
    """
    try:
        if value is None:
            return default
        parsed = int(value)
    except Exception:
        return default
    if parsed <= 0:
        return default
    return parsed


def parse_optional_int(value: Any, *, default: Optional[int] = None) -> Optional[int]:
    """
    尝试将 value 解析为整数；失败时返回 default。
    """
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default


def coerce_int(value: Any, *, default: int = 0) -> int:
    """
    尝试将 value 转为 int；失败时回退 default。
    """
    try:
        return int(value)
    except Exception:
        return int(default)


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


def json_preview(value: Any, max_chars: int) -> str:
    """
    将任意值压缩为短文本预览（优先 JSON 序列化），用于日志与提示词上下文控长。
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return truncate_text(value, max_chars)
    try:
        return truncate_text(json.dumps(value, ensure_ascii=False), max_chars)
    except Exception:
        return truncate_text(str(value), max_chars)


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


def build_json_frontmatter_markdown(
    meta: Dict[str, Any],
    body: Optional[str] = None,
    *,
    delimiter: str = "---",
) -> str:
    """
    统一构建 JSON frontmatter Markdown 文本。
    """
    fm_text = json.dumps(meta, ensure_ascii=False, indent=2, sort_keys=True).strip()
    body_text = str(body or "").rstrip()
    return "\n".join([delimiter, fm_text, delimiter, "", body_text, ""])


def discover_markdown_files(
    base_dir: "str | Path",
    *,
    skip_readme: bool = True,
    skip_hidden: bool = True,
) -> List[Path]:
    """
    统一发现目录下可同步的 Markdown 文件（默认跳过 readme 与隐藏路径）。
    """
    root = Path(base_dir)
    if not root.exists():
        return []

    files: List[Path] = []
    for path in root.rglob("*.md"):
        if not path.is_file():
            continue
        name = path.name.lower()
        if skip_readme and name in {"readme.md", "_readme.md"}:
            continue
        if skip_hidden and any(part.startswith(".") for part in path.parts):
            continue
        files.append(path)
    return sorted(files)


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
