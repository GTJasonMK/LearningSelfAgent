from pathlib import Path
from typing import Optional

from backend.src.prompt.paths import repo_root, system_prompt_dir


def load_system_prompt(name: str) -> Optional[str]:
    """
    从 backend/prompt/system/ 读取系统提示词。

    约定：
    - name 对应文件名（不含扩展名）
    - 优先读取 .txt，其次 .md
    """
    # 兼容：AGENT_PROMPT_ROOT 可能只覆盖 skills/memory 等目录，system 目录缺失时回退到仓库内置 prompts，
    # 避免单测/新手配置导致“提示词不存在”直接中断主链路。
    bases = [system_prompt_dir()]
    try:
        fallback = (repo_root() / "backend" / "prompt" / "system").resolve()
        if fallback not in [b.resolve() for b in bases]:
            bases.append(fallback)
    except Exception:
        pass

    for base in bases:
        candidates = [
            Path(base) / f"{name}.txt",
            Path(base) / f"{name}.md",
        ]
        for path in candidates:
            if path.exists() and path.is_file():
                try:
                    return path.read_text(encoding="utf-8")
                except Exception:
                    return None
    return None
