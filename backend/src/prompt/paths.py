from pathlib import Path
import os

from backend.src.constants import PROMPT_ENV_VAR


def repo_root() -> Path:
    """
    获取仓库根目录（LearningSelfAgent/）。

    注意：不要依赖当前工作目录，避免从不同启动方式（uvicorn/脚本）运行时路径漂移。
    """
    # backend/src/prompt/paths.py -> backend/src/prompt -> backend/src -> backend -> repo_root
    return Path(__file__).resolve().parents[3]


def prompt_root() -> Path:
    env = str(os.getenv(PROMPT_ENV_VAR, "")).strip()
    if env:
        return Path(env).expanduser().resolve()
    return repo_root() / "backend" / "prompt"


def system_prompt_dir() -> Path:
    return prompt_root() / "system"


def skills_prompt_dir() -> Path:
    return prompt_root() / "skills"


def memory_prompt_dir() -> Path:
    return prompt_root() / "memory"


def tools_prompt_dir() -> Path:
    return prompt_root() / "tools"


def graph_prompt_dir() -> Path:
    return prompt_root() / "graph"
