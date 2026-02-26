from __future__ import annotations

import re


def can_compile_python_source(code: str, *, filename: str = "<python_source>") -> bool:
    source = str(code or "").strip()
    if not source:
        return False
    try:
        compile(source, filename, "exec")
        return True
    except Exception:
        return False


def normalize_python_c_source(code: str, *, compile_name: str = "<python_c>") -> str:
    """
    规范化 python -c 的单行复杂语句，降低落盘脚本 SyntaxError 概率。
    """
    source = str(code or "").strip()
    if not source:
        return source
    if can_compile_python_source(source, filename=compile_name):
        return source

    rewritten = re.sub(
        r";\s*(?=(with|for|if|try|while|def|class|async\s+def|elif|else|except|finally)\b)",
        "\n",
        source,
    )
    rewritten = re.sub(
        r":\s*(?=(with|for|if|try|while|def|class|async\s+def)\b)",
        ":\n    ",
        rewritten,
    )
    rewritten = re.sub(r"\n[ \t]+(?=(elif|else|except|finally)\b)", "\n", rewritten)
    return rewritten


def has_risky_inline_control_flow(code: str) -> bool:
    """
    检测高风险“单行复合控制流”。
    """
    text = str(code or "").strip()
    if not text:
        return False
    if ";" not in text:
        return False
    if "\n" in text:
        return False

    block_headers = re.findall(r"\b(if|for|while|with|try|except|finally|elif|else)\b[^:]*:", text)
    if len(block_headers) >= 2:
        return True

    if re.search(
        r"\b(for|while)\b[^:]*:\s*[^;]+;\s*(if|for|while|with|try|except|finally|elif|else)\b",
        text,
        re.IGNORECASE,
    ):
        return True

    return False

