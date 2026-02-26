from __future__ import annotations

from typing import Any, Dict, List, Optional

from backend.src.common.utils import bump_semver_patch, dedupe_keep_order, now_iso, parse_json_list
from backend.src.constants import DEFAULT_SKILL_VERSION
from backend.src.repositories.skills_repo import VALID_SKILL_STATUSES, update_skill, update_skill_status
from backend.src.services.knowledge.governance.helpers import _skills_status_where
from backend.src.services.skills.skills_publish import publish_skill_file
from backend.src.storage import get_connection


def dedupe_and_merge_skills(
    *,
    dry_run: bool = True,
    include_draft: bool = True,
    merge_across_domains: bool = False,
    reason: Optional[str] = None,
) -> dict:
    """
    去重 + 版本合并（同 scope/name）。

    策略（尽量不破坏引用）：
    - scope 非空：按 (skill_type, scope) 分组
    - scope 为空：按 (skill_type, domain_id, name) 分组（merge_across_domains=True 时忽略 domain_id）
    - 选择 canonical：优先 approved，其次 draft；同等级取 id 最大（认为“最新”）
    - canonical：合并 list 字段并 bump patch 版本 +1
    - duplicates：
      - draft -> abandoned
      - approved -> deprecated
    """
    note = str(reason or "").strip() or "maintenance_dedupe_merge"

    publish_ids: List[int] = []
    publish_errors: List[str] = []

    with get_connection() as conn:
        status_where = _skills_status_where(bool(include_draft))

        rows = conn.execute(
            "SELECT * FROM skills_items WHERE " + status_where + " ORDER BY id ASC",
        ).fetchall()

        groups: Dict[str, List[Any]] = {}
        for row in rows or []:
            name = str(row["name"] or "").strip()
            if not name:
                continue
            skill_type = str(row["skill_type"] or "methodology").strip().lower()
            scope = str(row["scope"] or "").strip()
            domain_id = str(row["domain_id"] or "misc").strip() or "misc"
            if scope:
                key = f"scope:{skill_type}:{scope}"
            else:
                did = "*" if merge_across_domains else domain_id
                key = f"name:{skill_type}:{did}:{name.lower()}"
            groups.setdefault(key, []).append(row)

        merged_count = 0
        marked_duplicates = 0
        actions: List[dict] = []

        def _status_rank(v: str) -> int:
            vv = str(v or "").strip().lower()
            if vv == "approved" or not vv:
                return 2
            if vv == "draft":
                return 1
            return 0

        for key, items in groups.items():
            if len(items) <= 1:
                continue

            # 选 canonical
            ordered = sorted(
                items,
                key=lambda r: (_status_rank(r["status"]), int(r["id"])),
                reverse=True,
            )
            canonical = ordered[0]
            dup_rows = ordered[1:]

            canonical_id = int(canonical["id"])
            canonical_version = canonical["version"] or DEFAULT_SKILL_VERSION
            next_version = bump_semver_patch(
                str(canonical_version),
                default_version=str(DEFAULT_SKILL_VERSION or "0.1.0"),
            )

            # 合并：canonical 优先，duplicates 追加
            def _merge_json_field(field: str) -> List[Any]:
                merged_list: List[Any] = []
                for r in [canonical, *dup_rows]:
                    merged_list.extend(parse_json_list(r[field]))
                return dedupe_keep_order(merged_list)

            merged_tags = _merge_json_field("tags")
            merged_triggers = _merge_json_field("triggers")
            merged_aliases = _merge_json_field("aliases")
            merged_prereq = _merge_json_field("prerequisites")
            merged_inputs = _merge_json_field("inputs")
            merged_outputs = _merge_json_field("outputs")
            merged_steps = _merge_json_field("steps")
            merged_failure = _merge_json_field("failure_modes")
            merged_validation = _merge_json_field("validation")

            # description：canonical 为空时用第一个非空补齐
            desc = canonical["description"]
            if not str(desc or "").strip():
                for r in dup_rows:
                    d = r["description"]
                    if str(d or "").strip():
                        desc = d
                        break

            action_item = {
                "key": key,
                "canonical_skill_id": canonical_id,
                "canonical_from_version": canonical_version,
                "canonical_to_version": next_version,
                "duplicates": [int(r["id"]) for r in dup_rows],
            }
            actions.append(action_item)

            if dry_run:
                continue

            updated = update_skill(
                skill_id=int(canonical_id),
                description=desc,
                tags=merged_tags,
                triggers=merged_triggers,
                aliases=merged_aliases,
                prerequisites=merged_prereq,
                inputs=merged_inputs,
                outputs=merged_outputs,
                steps=merged_steps,
                failure_modes=merged_failure,
                validation=merged_validation,
                version=str(next_version),
                change_notes=f"dedupe_merge:{key}:{note}",
                conn=conn,
            )
            if updated:
                merged_count += 1
                publish_ids.append(int(canonical_id))

            for r in dup_rows:
                dup_id = int(r["id"])
                status = str(r["status"] or "approved").strip().lower() or "approved"
                target = "deprecated" if status == "approved" else ("abandoned" if status == "draft" else status)
                if target not in VALID_SKILL_STATUSES:
                    target = "deprecated"
                if target == status:
                    continue
                _ = update_skill_status(skill_id=int(dup_id), status=str(target), conn=conn)
                marked_duplicates += 1

    # 落盘 canonical（失败不阻塞去重）
    if publish_ids and not dry_run:
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
        "include_draft": bool(include_draft),
        "merge_across_domains": bool(merge_across_domains),
        "merged": int(merged_count),
        "marked_duplicates": int(marked_duplicates),
        "actions": actions[:200],
        "published": len(publish_ids) if not dry_run else 0,
        "publish_errors": publish_errors[:50],
        "at": now_iso(),
    }
