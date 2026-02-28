#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
脚本分析与优化工具（供 tool_call:script_analyze_optimize 调用）。

输入（stdin JSON）示例：
{
  "target_paths": ["scripts/demo.py"],
  "mode": "analyze",
  "languages": ["python", "shell", "javascript", "typescript"],
  "constraints": {"preserve_behavior": true},
  "model": "gpt-5.2"
}

输出（stdout JSON）：
{
  "status": "ok|partial|failed",
  "summary": "...",
  "files": [...],
  "applied": [...],
  "errors": [...]
}
"""

from __future__ import annotations

import argparse
import difflib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# 确保脚本直接执行时可导入 backend 包。
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from backend.src.common.utils import extract_json_object
from backend.src.services.llm.llm_client import call_openai, resolve_default_model

SUPPORTED_LANGUAGES = frozenset({"python", "shell", "javascript", "typescript"})
LANGUAGE_BY_SUFFIX = {
    ".py": "python",
    ".sh": "shell",
    ".bash": "shell",
    ".zsh": "shell",
    ".js": "javascript",
    ".mjs": "javascript",
    ".cjs": "javascript",
    ".ts": "typescript",
    ".mts": "typescript",
    ".cts": "typescript",
}
MODES = frozenset({"analyze", "propose_patch", "apply_patch"})
MAX_SOURCE_CHARS = 12000


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def _truncate_text(value: object, limit: int) -> str:
    text = str(value or "")
    if limit <= 0:
        return ""
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)] + "…"


def _normalize_mode(value: object) -> str:
    mode = str(value or "").strip().lower()
    return mode if mode in MODES else "analyze"


def _normalize_language_list(value: object) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        out: List[str] = []
        for item in value:
            text = str(item or "").strip().lower()
            if text in SUPPORTED_LANGUAGES and text not in out:
                out.append(text)
        return out
    text = str(value or "").strip().lower()
    if text in SUPPORTED_LANGUAGES:
        return [text]
    return []


def _detect_language(path: Path) -> str:
    return LANGUAGE_BY_SUFFIX.get(path.suffix.lower(), "")


def _load_json_input(
    *,
    use_stdin: bool,
    input_json: Optional[str],
    input_file: Optional[str],
) -> Tuple[Optional[dict], Optional[str]]:
    raw = ""
    if input_json:
        raw = str(input_json)
    elif input_file:
        path = Path(str(input_file)).expanduser().resolve()
        if not path.exists():
            return None, f"input_file_not_found: {path}"
        raw = path.read_text(encoding="utf-8")
    elif use_stdin:
        raw = sys.stdin.read()

    raw = str(raw or "").strip()
    if not raw:
        return None, "input_json_empty"
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"input_json_invalid: {exc}"
    if not isinstance(obj, dict):
        return None, "input_json_must_be_object"
    return obj, None


def _ensure_target_paths(value: object) -> List[str]:
    if not isinstance(value, list):
        return []
    out: List[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            out.append(text)
    return out


def _normalize_issues(value: object) -> List[dict]:
    if not isinstance(value, list):
        return []
    out: List[dict] = []
    for item in value:
        if isinstance(item, dict):
            out.append(
                {
                    "type": str(item.get("type") or "").strip() or "issue",
                    "severity": str(item.get("severity") or "").strip() or "medium",
                    "line": item.get("line"),
                    "message": str(item.get("message") or "").strip(),
                }
            )
        else:
            text = str(item or "").strip()
            if text:
                out.append({"type": "issue", "severity": "medium", "line": None, "message": text})
    return [it for it in out if str(it.get("message") or "").strip()]


def _normalize_suggestions(value: object) -> List[dict]:
    if not isinstance(value, list):
        return []
    out: List[dict] = []
    for item in value:
        if isinstance(item, dict):
            out.append(
                {
                    "title": str(item.get("title") or "").strip() or "建议",
                    "detail": str(item.get("detail") or item.get("description") or "").strip(),
                }
            )
        else:
            text = str(item or "").strip()
            if text:
                out.append({"title": "建议", "detail": text})
    return [it for it in out if str(it.get("detail") or "").strip()]


def _count_changed_lines(before: str, after: str) -> int:
    before_lines = before.splitlines()
    after_lines = after.splitlines()
    changed = 0
    matcher = difflib.SequenceMatcher(a=before_lines, b=after_lines)
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            continue
        changed += max(i2 - i1, j2 - j1)
    return int(changed)


def _build_patch(path: Path, before: str, after: str) -> str:
    diff_lines = difflib.unified_diff(
        before.splitlines(keepends=True),
        after.splitlines(keepends=True),
        fromfile=f"a/{path.as_posix()}",
        tofile=f"b/{path.as_posix()}",
        lineterm="",
    )
    return "\n".join(diff_lines).strip()


def _backup_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.bak-{_now_tag()}")


def _build_prompt(
    *,
    file_path: Path,
    language: str,
    mode: str,
    source_code: str,
    constraints: dict,
) -> str:
    preserve_behavior = bool(constraints.get("preserve_behavior", True))
    max_patch_hunks = constraints.get("max_patch_hunks")
    hint_max_hunks = int(max_patch_hunks) if isinstance(max_patch_hunks, int) and max_patch_hunks > 0 else 6

    source = source_code
    truncated = False
    if len(source) > MAX_SOURCE_CHARS:
        source = source[:MAX_SOURCE_CHARS]
        truncated = True

    requirements = [
        "请只输出 JSON 对象，不要输出任何解释性前后缀。",
        "必须包含字段：status, summary, issues, suggestions。",
        "issues 为数组，每项包含 type,severity,line,message。",
        "suggestions 为数组，每项包含 title,detail。",
    ]
    if mode in {"propose_patch", "apply_patch"}:
        requirements.append("必须包含 optimized_code（完整优化后代码字符串）。")
        requirements.append(f"优化改动应尽量集中，不超过 {hint_max_hunks} 个逻辑变更块。")
    else:
        requirements.append("analyze 模式下可不返回 optimized_code。")
    if preserve_behavior:
        requirements.append("在无明确 bug 证据时，保持原有行为不变。")

    meta_lines = [
        f"- file_path: {file_path.as_posix()}",
        f"- language: {language}",
        f"- mode: {mode}",
        f"- source_truncated: {str(truncated).lower()}",
    ]
    if preserve_behavior:
        meta_lines.append("- preserve_behavior: true")
    if isinstance(max_patch_hunks, int) and max_patch_hunks > 0:
        meta_lines.append(f"- max_patch_hunks: {int(max_patch_hunks)}")

    prompt = (
        "你是资深代码审查与重构工程师。\n"
        "任务：分析并优化给定脚本，输出严格 JSON。\n\n"
        "约束：\n"
        + "\n".join(f"- {line}" for line in requirements)
        + "\n\n输入：\n"
        + "\n".join(meta_lines)
        + "\n\n源码：\n```text\n"
        + source
        + "\n```"
    )
    return prompt


def _call_optimizer_llm(
    *,
    prompt: str,
    model: Optional[str],
) -> Tuple[Optional[dict], Optional[str]]:
    text, _, err = call_openai(
        prompt=prompt,
        model=model,
        parameters={"temperature": 0.1},
    )
    if err:
        return None, f"llm_failed:{err}"
    if not text:
        return None, "llm_failed:empty_response"
    obj = extract_json_object(str(text))
    if not isinstance(obj, dict):
        return None, "llm_failed:invalid_json"
    return obj, None


def _process_single_file(
    *,
    raw_path: str,
    mode: str,
    allowed_languages: List[str],
    constraints: dict,
    model: Optional[str],
) -> Tuple[dict, Optional[dict], Optional[dict]]:
    """
    返回：(file_result, applied_item, error_item)
    """
    path = Path(str(raw_path)).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()

    display_path = path.as_posix()
    language = _detect_language(path)
    base_result = {
        "path": display_path,
        "language": language or "unknown",
        "issues": [],
        "suggestions": [],
    }

    if not path.exists() or not path.is_file():
        return (
            {**base_result, "status": "failed"},
            None,
            {"path": display_path, "code": "file_not_found", "message": f"文件不存在: {display_path}"},
        )

    if not language:
        return (
            {**base_result, "status": "failed"},
            None,
            {
                "path": display_path,
                "code": "language_not_supported",
                "message": f"不支持的脚本类型: {path.suffix or '(none)'}",
            },
        )

    if allowed_languages and language not in allowed_languages:
        return (
            {**base_result, "status": "failed"},
            None,
            {
                "path": display_path,
                "code": "language_filtered_out",
                "message": f"脚本语言 {language} 不在请求 languages 白名单中",
            },
        )

    before = path.read_text(encoding="utf-8")
    prompt = _build_prompt(
        file_path=path,
        language=language,
        mode=mode,
        source_code=before,
        constraints=constraints,
    )
    llm_obj, llm_err = _call_optimizer_llm(prompt=prompt, model=model)
    if llm_err:
        return (
            {**base_result, "status": "failed"},
            None,
            {"path": display_path, "code": "llm_failed", "message": llm_err},
        )
    assert isinstance(llm_obj, dict)

    issues = _normalize_issues(llm_obj.get("issues"))
    suggestions = _normalize_suggestions(llm_obj.get("suggestions"))
    summary = str(llm_obj.get("summary") or "").strip()
    optimized_code = llm_obj.get("optimized_code")

    file_result = {
        **base_result,
        "status": "ok",
        "summary": _truncate_text(summary, 260),
        "issues": issues,
        "suggestions": suggestions,
    }

    if mode not in {"propose_patch", "apply_patch"}:
        return file_result, None, None

    if not isinstance(optimized_code, str) or not optimized_code:
        file_result["status"] = "failed"
        return (
            file_result,
            None,
            {
                "path": display_path,
                "code": "missing_optimized_code",
                "message": "LLM 未返回 optimized_code，无法生成补丁",
            },
        )

    patch_text = _build_patch(path, before, optimized_code)
    file_result["patch"] = patch_text

    if mode == "propose_patch":
        return file_result, None, None

    if before == optimized_code:
        return file_result, None, None

    backup = _backup_path(path)
    backup.write_text(before, encoding="utf-8")
    path.write_text(optimized_code, encoding="utf-8")
    changed_lines = _count_changed_lines(before, optimized_code)

    applied_item = {
        "path": display_path,
        "backup_path": backup.as_posix(),
        "changed_lines": int(changed_lines),
    }
    return file_result, applied_item, None


def run_optimizer(payload: dict) -> dict:
    mode = _normalize_mode(payload.get("mode"))
    target_paths = _ensure_target_paths(payload.get("target_paths"))
    allowed_languages = _normalize_language_list(payload.get("languages"))
    constraints = payload.get("constraints") if isinstance(payload.get("constraints"), dict) else {}
    model = str(payload.get("model") or "").strip() or resolve_default_model()

    if not target_paths:
        return {
            "status": "failed",
            "summary": "缺少 target_paths",
            "files": [],
            "applied": [],
            "errors": [{"code": "invalid_input", "message": "target_paths 不能为空"}],
        }

    files: List[dict] = []
    applied: List[dict] = []
    errors: List[dict] = []

    for raw_path in target_paths:
        file_result, applied_item, error_item = _process_single_file(
            raw_path=str(raw_path),
            mode=mode,
            allowed_languages=allowed_languages,
            constraints=constraints,
            model=model,
        )
        files.append(file_result)
        if isinstance(applied_item, dict):
            applied.append(applied_item)
        if isinstance(error_item, dict):
            errors.append(error_item)

    success_count = sum(1 for item in files if str(item.get("status") or "") == "ok")
    error_count = len(errors)
    applied_count = len(applied)
    total = len(files)

    if error_count <= 0:
        status = "ok"
    elif success_count <= 0 and applied_count <= 0:
        status = "failed"
    elif applied_count <= 0 and mode == "apply_patch":
        status = "failed"
    else:
        status = "partial"

    summary = (
        f"mode={mode} 处理 {total} 个文件，成功 {success_count}，"
        f"失败 {error_count}，应用变更 {applied_count}"
    )
    return {
        "status": status,
        "summary": summary,
        "files": files,
        "applied": applied,
        "errors": errors,
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Analyze/optimize existing scripts")
    parser.add_argument("--input-json", dest="input_json", default=None, help="JSON string input")
    parser.add_argument("--input-file", dest="input_file", default=None, help="path to JSON input file")
    parser.add_argument(
        "--input-stdin",
        dest="input_stdin",
        action="store_true",
        help="read input JSON from stdin",
    )
    parser.add_argument("--pretty", dest="pretty", action="store_true", help="pretty print JSON output")
    args = parser.parse_args(argv)

    use_stdin = bool(args.input_stdin) or (not args.input_json and not args.input_file)
    payload, parse_error = _load_json_input(
        use_stdin=use_stdin,
        input_json=args.input_json,
        input_file=args.input_file,
    )
    if parse_error:
        output = {
            "status": "failed",
            "summary": "输入解析失败",
            "files": [],
            "applied": [],
            "errors": [{"code": "invalid_input_json", "message": parse_error}],
        }
        text = json.dumps(output, ensure_ascii=False, indent=2 if args.pretty else None)
        print(text)
        return 0

    try:
        result = run_optimizer(payload or {})
    except Exception as exc:
        result = {
            "status": "failed",
            "summary": "脚本优化执行异常",
            "files": [],
            "applied": [],
            "errors": [{"code": "internal_error", "message": str(exc)}],
        }

    text = json.dumps(result, ensure_ascii=False, indent=2 if args.pretty else None)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
