from __future__ import annotations

import json
from typing import List

from backend.src.common.utils import now_iso, parse_json_list
from backend.src.services.knowledge.governance.helpers import _skills_status_where
from backend.src.services.knowledge.skill_tag_policy import normalize_skill_tags
from backend.src.services.skills.skills_publish import publish_skill_file
from backend.src.storage import get_connection


def validate_and_fix_skill_tags(
    *,
    dry_run: bool = True,
    fix: bool = False,
    strict_keys: bool = False,
    include_draft: bool = True,
    limit: int = 5000,
) -> dict:
    """
    标签规范校验（skills_items.tags）。

    参数：
    - dry_run=True：只预览影响面
    - fix=True：写库修复 tags（不 bump version；治理操作不等同于技能迭代）
    - strict_keys=True：未知 key:value tag 直接丢弃（默认仅提示）
    - include_draft：是否包含 draft 技能
    """
    try:
        limit_value = int(limit)
    except Exception:
        limit_value = 5000
    if limit_value <= 0:
        limit_value = 5000

    changed = 0
    matched = 0
    issues_total = 0
    samples: List[dict] = []
    publish_ids: List[int] = []
    publish_errors: List[str] = []

    with get_connection() as conn:
        status_where = _skills_status_where(bool(include_draft))

        rows = conn.execute(
            f"SELECT id, name, tags FROM skills_items WHERE {status_where} ORDER BY id ASC LIMIT ?",
            (int(limit_value),),
        ).fetchall()

        for row in rows or []:
            matched += 1
            sid = int(row["id"])
            original = parse_json_list(row["tags"])
            normalized, issues = normalize_skill_tags(original, strict_keys=bool(strict_keys))
            if issues:
                issues_total += len(issues)
            if normalized != original:
                changed += 1
                if len(samples) < 200:
                    samples.append(
                        {
                            "skill_id": sid,
                            "name": str(row["name"] or ""),
                            "from": original[:20],
                            "to": normalized[:20],
                            "issues": issues[:12],
                        }
                    )
                if fix and not dry_run:
                    conn.execute(
                        "UPDATE skills_items SET tags = ? WHERE id = ?",
                        (json.dumps(normalized, ensure_ascii=False), int(sid)),
                    )
                    publish_ids.append(int(sid))

    if fix and not dry_run and publish_ids:
        for sid in publish_ids[:2000]:
            try:
                _source_path, err = publish_skill_file(int(sid))
                if err:
                    publish_errors.append(f"skill:{sid}: {err}")
            except Exception as exc:
                publish_errors.append(f"skill:{sid}: {exc}")

    return {
        "ok": True,
        "dry_run": bool(dry_run),
        "fix": bool(fix),
        "strict_keys": bool(strict_keys),
        "include_draft": bool(include_draft),
        "limit": int(limit_value),
        "matched": int(matched),
        "changed": int(changed),
        "issues_total": int(issues_total),
        "samples": samples,
        "published": len(publish_ids) if fix and not dry_run else 0,
        "publish_errors": publish_errors[:50],
        "at": now_iso(),
    }
