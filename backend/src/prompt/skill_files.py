import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from backend.src.common.utils import parse_positive_int
from backend.src.prompt.paths import skills_prompt_dir


_FRONTMATTER_DELIM = "---"


@dataclass
class SkillFile:
    """
    skills Markdown 文件的解析结果（frontmatter + body）。

    约定：frontmatter 使用 YAML（推荐），正文为 Markdown。
    """

    meta: Dict[str, Any]
    body: str
    source_path: str


def _try_load_yaml(text: str) -> Optional[dict]:
    """
    优先用 PyYAML 解析 frontmatter；若依赖缺失则返回 None，让调用方走降级解析。
    """
    try:
        import yaml  # type: ignore
    except Exception:
        return None
    try:
        data = yaml.safe_load(text)
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _parse_simple_frontmatter(text: str) -> dict:
    """
    YAML 不可用时的降级解析器：
    - 支持 key: value
    - 支持 key: [a, b]（简单数组）
    - 支持 key:\n  - a\n  - b（列表）

    目的：避免因为缺少 PyYAML 导致整个系统无法启动/同步。
    """
    result: Dict[str, Any] = {}
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        raw = lines[i].rstrip("\n")
        if not raw.strip():
            i += 1
            continue
        if ":" not in raw:
            i += 1
            continue
        key, rest = raw.split(":", 1)
        key = key.strip()
        rest = rest.strip()
        if not key:
            i += 1
            continue
        if rest.startswith("[") and rest.endswith("]"):
            # 非严格 JSON：尽量按逗号拆分
            inner = rest[1:-1].strip()
            items = []
            if inner:
                for part in inner.split(","):
                    val = part.strip().strip("\"'").strip()
                    if val:
                        items.append(val)
            result[key] = items
            i += 1
            continue
        if rest:
            result[key] = rest.strip("\"'").strip()
            i += 1
            continue
        # 可能是 block list
        j = i + 1
        items: List[str] = []
        while j < len(lines):
            nxt = lines[j].rstrip("\n")
            if not nxt.strip():
                j += 1
                continue
            m = re.match(r"^\s*-\s+(.*)$", nxt)
            if not m:
                break
            val = m.group(1).strip().strip("\"'").strip()
            if val:
                items.append(val)
            j += 1
        if items:
            result[key] = items
            i = j
            continue
        i += 1
    return result


def parse_skill_markdown(text: str, source_path: str) -> SkillFile:
    """
    解析 skills Markdown：
    - 以 --- frontmatter --- 开头（可选）
    - meta 解析为 dict
    """
    normalized = text.replace("\r\n", "\n")
    meta: Dict[str, Any] = {}
    body = normalized
    if normalized.startswith(_FRONTMATTER_DELIM + "\n"):
        # 只识别最开头的 frontmatter：避免正文中出现 '---' 被误判
        # 格式：
        # ---
        # key: value
        # ---
        # body...
        end = normalized.find("\n" + _FRONTMATTER_DELIM + "\n", len(_FRONTMATTER_DELIM) + 1)
        if end != -1:
            fm_start = len(_FRONTMATTER_DELIM) + 1  # 跳过首行 '---\n'
            fm_text = normalized[fm_start:end]
            body = normalized[end + len("\n" + _FRONTMATTER_DELIM + "\n") :]
            yaml_data = _try_load_yaml(fm_text)
            if yaml_data is not None:
                meta = yaml_data
            else:
                meta = _parse_simple_frontmatter(fm_text)
                # 兜底：若 frontmatter 实际是 JSON（例如缺少 PyYAML 时被写成 JSON），则尝试解析
                if not meta and fm_text.strip().startswith("{"):
                    try:
                        obj = json.loads(fm_text)
                        meta = obj if isinstance(obj, dict) else {}
                    except json.JSONDecodeError:
                        meta = {}
    return SkillFile(meta=meta, body=body.strip(), source_path=source_path)


def _as_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        out = []
        for item in value:
            text = str(item).strip()
            if text:
                out.append(text)
        return out
    # 兼容：允许用逗号分隔
    text = str(value).strip()
    if not text:
        return []
    if "," in text:
        return [s.strip() for s in text.split(",") if s.strip()]
    return [text]


def _as_json_list(value: Any) -> List[Any]:
    """
    允许 list[str|dict|...]；用于 inputs/outputs/steps 等可能包含结构化对象的字段。
    """
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _normalize_skill_type(value: Any) -> Optional[str]:
    t = str(value or "").strip().lower()
    if not t:
        return None
    if t in {"methodology", "solution"}:
        return t
    return None


def _normalize_skill_status(value: Any) -> Optional[str]:
    s = str(value or "").strip().lower()
    if not s:
        return None
    if s in {"draft", "approved", "deprecated", "abandoned"}:
        return s
    return None


def normalize_skill_meta(meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    将 frontmatter 元数据归一化成后端可落库字段。
    """
    name = str(meta.get("name") or "").strip()
    if not name:
        # 允许用 title 作为兜底（避免新手写错字段）
        name = str(meta.get("title") or "").strip()
    description = str(meta.get("description") or "").strip() or None
    scope = str(meta.get("scope") or "").strip() or None
    category = str(meta.get("category") or "").strip() or None

    tags = _as_list(meta.get("tags"))
    triggers = _as_list(meta.get("triggers"))
    aliases = _as_list(meta.get("aliases"))

    prerequisites = _as_list(meta.get("prerequisites"))
    inputs = _as_json_list(meta.get("inputs"))
    outputs = _as_json_list(meta.get("outputs"))
    steps = _as_json_list(meta.get("steps"))
    failure_modes = _as_list(meta.get("failure_modes"))
    validation = _as_list(meta.get("validation"))

    version = str(meta.get("version") or "").strip() or None

    domain_id = str(meta.get("domain_id") or meta.get("domain") or "").strip() or None
    skill_type = _normalize_skill_type(meta.get("skill_type") or meta.get("type")) or "methodology"
    status = _normalize_skill_status(meta.get("status")) or "approved"
    source_task_id = parse_positive_int(meta.get("source_task_id"), default=None)
    source_run_id = parse_positive_int(meta.get("source_run_id"), default=None)

    return {
        "name": name,
        "description": description,
        "scope": scope,
        "category": category,
        "tags": tags,
        "triggers": triggers,
        "aliases": aliases,
        "prerequisites": prerequisites,
        "inputs": inputs,
        "outputs": outputs,
        "steps": steps,
        "failure_modes": failure_modes,
        "validation": validation,
        "version": version,
        # Phase 2：领域/类型/状态/来源信息（Solution/Skill 复用同一套文件格式）
        "domain_id": domain_id,
        "skill_type": skill_type,
        "status": status,
        "source_task_id": source_task_id,
        "source_run_id": source_run_id,
    }


def discover_skill_markdown_files(base_dir: Optional[Path] = None) -> List[Path]:
    """
    扫描 skills 目录下所有 *.md（递归）。
    """
    root = base_dir or skills_prompt_dir()
    if not root.exists():
        return []
    files: List[Path] = []
    for path in root.rglob("*.md"):
        # 跳过 README / 隐藏文件
        name = path.name.lower()
        if name in {"readme.md", "_readme.md"}:
            continue
        if any(part.startswith(".") for part in path.parts):
            continue
        files.append(path)
    return sorted(files)


def load_skill_files(base_dir: Optional[Path] = None) -> Tuple[List[SkillFile], List[str]]:
    """
    读取并解析所有技能文件。
    返回：(skills, errors)
    """
    errors: List[str] = []
    items: List[SkillFile] = []
    rel_root = base_dir or skills_prompt_dir()
    for path in discover_skill_markdown_files(base_dir=base_dir):
        try:
            text = path.read_text(encoding="utf-8")
            try:
                rel = str(path.relative_to(rel_root)).replace("\\", "/")
            except ValueError:
                rel = path.name
            items.append(parse_skill_markdown(text=text, source_path=rel))
        except Exception as exc:
            errors.append(f"{path}: {exc}")
    return items, errors


def build_skill_markdown(meta: Dict[str, Any], body_sections: Optional[dict] = None) -> str:
    """
    生成技能 Markdown（YAML frontmatter + Markdown 正文）。

    注意：尽量输出 ASCII（避免工具链编码不一致）。如果 meta 包含中文，依旧会写入 UTF-8，
    但 YAML 的格式化会尽量稳定。
    """
    # frontmatter: 仅保留可序列化字段
    clean_meta = dict(meta)

    # YAML 优先；缺失时退回 JSON（保证至少可用）
    fm_text = None
    try:
        import yaml  # type: ignore

        fm_text = yaml.safe_dump(
            clean_meta,
            # 技能库文件主要给人读，允许写入中文等 Unicode 字符，避免被转义成 \\uXXXX 难以维护。
            allow_unicode=True,
            sort_keys=True,
            default_flow_style=False,
        ).strip()
    except Exception:
        fm_text = json.dumps(clean_meta, ensure_ascii=False, indent=2)

    name = str(meta.get("name") or "").strip()
    title = name or "未命名技能"

    sections = body_sections or {}
    desc = str(meta.get("description") or "").strip()
    scope = str(meta.get("scope") or "").strip()
    steps = meta.get("steps") if isinstance(meta.get("steps"), list) else []
    failure_modes = meta.get("failure_modes") if isinstance(meta.get("failure_modes"), list) else []
    validation = meta.get("validation") if isinstance(meta.get("validation"), list) else []

    def _list_block(items: List[Any]) -> str:
        out = []
        for item in items or []:
            text = str(item).strip()
            if text:
                out.append(f"- {text}")
        return "\n".join(out).strip()

    lines: List[str] = []
    lines.append(_FRONTMATTER_DELIM)
    lines.append(fm_text)
    lines.append(_FRONTMATTER_DELIM)
    lines.append("")
    lines.append(f"# {title}")
    lines.append("")
    if desc:
        lines.append(desc)
        lines.append("")
    if scope:
        lines.append("## 适用范围")
        lines.append(scope)
        lines.append("")
    if steps:
        lines.append("## 步骤")
        lines.append(_list_block(steps))
        lines.append("")
    if validation:
        lines.append("## 验证")
        lines.append(_list_block(validation))
        lines.append("")
    if failure_modes:
        lines.append("## 失败模式")
        lines.append(_list_block(failure_modes))
        lines.append("")
    extra = sections.get("extra_markdown")
    if extra:
        lines.append(str(extra).rstrip())
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def slugify_filename(name: str) -> str:
    """
    将名称转为较安全的文件名（不追求完美，只要可读且跨平台）。
    """
    value = (name or "").strip().lower()
    if not value:
        return "skill"
    value = re.sub(r"[^\w\u4e00-\u9fff\- ]+", "", value)
    value = value.replace(" ", "_")
    value = re.sub(r"_+", "_", value).strip("_")
    if not value:
        return "skill"
    return value[:80]


def ensure_category_dir(category: str) -> Path:
    """
    category 形如 tool.web / data.extract；目录采用同名层级。
    """
    root = skills_prompt_dir()
    parts = [p for p in (category or "").split(".") if p]
    if not parts:
        parts = ["misc"]
    target = root.joinpath(*parts)
    target.mkdir(parents=True, exist_ok=True)
    return target


def write_skill_file(meta: Dict[str, Any], category: str, filename_hint: str) -> str:
    """
    根据 meta 写入技能文件，并返回相对路径（用于落库 source_path）。
    """
    category_dir = ensure_category_dir(category)
    filename = slugify_filename(filename_hint) + ".md"
    path = category_dir / filename

    # 若文件已存在且 name 不同，避免覆盖：追加短 hash
    if path.exists():
        try:
            existing = parse_skill_markdown(
                path.read_text(encoding="utf-8"),
                source_path="",
            )
            existing_name = str(existing.meta.get("name") or "").strip()
        except Exception:
            existing_name = ""
        if existing_name and existing_name != str(meta.get("name") or "").strip():
            suffix = datetime.now(timezone.utc).strftime("%H%M%S")
            filename = slugify_filename(filename_hint) + f"_{suffix}.md"
            path = category_dir / filename

    markdown = build_skill_markdown(meta=meta)
    path.write_text(markdown, encoding="utf-8")
    rel = str(path.relative_to(skills_prompt_dir())).replace("\\", "/")
    return rel
