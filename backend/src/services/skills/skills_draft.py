from __future__ import annotations

from typing import Optional, Tuple

from backend.src.repositories.skills_repo import SkillCreateParams as SkillCreateParamsRepo
from backend.src.repositories.skills_repo import create_skill as create_skill_repo
from backend.src.services.skills.skills_publish import publish_skill_file

SkillCreateParams = SkillCreateParamsRepo


def create_skill(params: SkillCreateParamsRepo) -> int:
    """
    技能草稿创建服务封装。

    说明：
    - 对外隐藏 repository 入口，便于 runner 层去耦；
    - 保持返回值语义与 repository 一致（skill_id）。
    """
    return int(create_skill_repo(params))


def create_and_publish_skill(params: SkillCreateParamsRepo) -> Tuple[int, Optional[str], Optional[str]]:
    """
    创建技能并尝试落盘，返回 (skill_id, source_path, publish_err)。
    """
    skill_id = int(create_skill_repo(params))
    source_path, publish_err = publish_skill_file(int(skill_id))
    return skill_id, source_path, publish_err
