from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from backend.src.common.utils import now_iso, parse_json_list
from backend.src.constants import (
    DEFAULT_SKILL_VERSION,
    TOOL_APPROVAL_STATUS_REJECTED,
    TOOL_METADATA_APPROVAL_KEY,
)
from backend.src.repositories.skills_repo import (
    VALID_SKILL_STATUSES,
    get_skill,
    update_skill,
    update_skill_status,
)
from backend.src.repositories.tool_call_records_repo import (
    get_skill_reuse_quality_map,
    get_tool_reuse_quality_map,
)
from backend.src.repositories.tools_repo import get_tool, update_tool
from backend.src.services.skills.skills_publish import publish_skill_file
from backend.src.services.knowledge.skill_tag_policy import normalize_skill_tags
from backend.src.services.tools.tools_store import publish_tool_file
from backend.src.storage import get_connection


def _safe_json_obj(value: Any) -> dict:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            obj = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return obj if isinstance(obj, dict) else {}
    return {}


def _extract_tool_approval(meta: dict) -> dict:
    approval = meta.get(TOOL_METADATA_APPROVAL_KEY)
    return approval if isinstance(approval, dict) else {}


def _bump_patch_version(version: Optional[str]) -> str:
    """
    最小版本策略：语义化版本 x.y.z 的 patch + 1；不符合则回退 DEFAULT_SKILL_VERSION。
    """
    value = str(version or "").strip()
    m = re.match(r"^(\d+)\.(\d+)\.(\d+)$", value)
    if not m:
        return str(DEFAULT_SKILL_VERSION or "0.1.0")
    major, minor, patch = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return f"{major}.{minor}.{patch + 1}"


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
        if include_draft:
            status_where = "status IN ('approved', 'draft') OR status IS NULL"
        else:
            status_where = "status = 'approved' OR status IS NULL"

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
        if include_draft:
            status_where = "status IN ('approved', 'draft') OR status IS NULL"
        else:
            status_where = "status = 'approved' OR status IS NULL"

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
            next_version = _bump_patch_version(str(canonical_version))

            # 合并：canonical 优先，duplicates 追加
            def _merge_json_field(field: str) -> List[Any]:
                merged_list: List[Any] = []
                for r in [canonical, *dup_rows]:
                    merged_list.extend(parse_json_list(r[field]))
                # 去重：以 JSON key 去重，保持顺序
                seen = set()
                out: List[Any] = []
                for it in merged_list:
                    try:
                        k = json.dumps(it, ensure_ascii=False, sort_keys=True)
                    except TypeError:
                        k = str(it)
                    if k in seen:
                        continue
                    seen.add(k)
                    out.append(it)
                return out

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

def rollback_knowledge_from_run(
    *,
    run_id: int,
    dry_run: bool = False,
    include_skills: bool = True,
    include_tools: bool = True,
    draft_skill_target_status: str = "abandoned",
    approved_skill_target_status: str = "deprecated",
    tool_target_status: str = TOOL_APPROVAL_STATUS_REJECTED,
    reason: Optional[str] = None,
) -> dict:
    """
    一键回滚/废弃：将某次 run 产生/更新的知识批量降级，避免错误知识长期影响后续任务。

    当前覆盖：
    - skills_items：匹配 source_run_id==run_id 或 tags 含 run:{run_id}
      - draft -> abandoned（默认）
      - approved -> deprecated（默认）
    - tools_items：匹配 metadata.approval.created_run_id==run_id 的工具，标记为 rejected（默认）

    注意：
    - 这是“知识治理”操作：默认不会删除 DB 行，只做状态降级并同步落盘文件；
    - dry_run=True 时只返回影响面预览，不写库不落盘。
    """
    try:
        rid = int(run_id)
    except Exception:
        return {"ok": False, "error": "invalid_run_id"}
    if rid <= 0:
        return {"ok": False, "error": "invalid_run_id"}

    now_value = now_iso()
    reason_text = str(reason or "").strip() or "maintenance_rollback"

    # 兜底：状态合法性
    draft_target = str(draft_skill_target_status or "").strip().lower() or "abandoned"
    if draft_target not in VALID_SKILL_STATUSES:
        draft_target = "abandoned"
    approved_target = str(approved_skill_target_status or "").strip().lower() or "deprecated"
    if approved_target not in VALID_SKILL_STATUSES:
        approved_target = "deprecated"

    tool_target = str(tool_target_status or "").strip().lower() or TOOL_APPROVAL_STATUS_REJECTED
    if tool_target not in {TOOL_APPROVAL_STATUS_REJECTED}:
        tool_target = TOOL_APPROVAL_STATUS_REJECTED

    changed_skill_ids: List[int] = []
    skill_changes: List[dict] = []
    skill_errors: List[str] = []

    changed_tool_ids: List[int] = []
    tool_changes: List[dict] = []
    tool_errors: List[str] = []

    with get_connection() as conn:
        if include_skills:
            # tags 是 JSON 数组字符串：用包含双引号的 LIKE 尽量避免误匹配（例如 run:12 命中 run:123）。
            tag_pattern = f"%\"run:{rid}\"%"
            rows = conn.execute(
                "SELECT id, name, status, skill_type, source_path FROM skills_items WHERE (source_run_id = ? OR tags LIKE ?) ORDER BY id ASC",
                (int(rid), tag_pattern),
            ).fetchall()

            for row in rows or []:
                try:
                    sid = int(row["id"])
                except Exception:
                    continue
                if sid <= 0:
                    continue
                current_status = str(row["status"] or "approved").strip().lower()
                if not current_status:
                    current_status = "approved"

                target_status = None
                if current_status == "draft":
                    target_status = draft_target
                elif current_status in {"approved", "deprecated", "abandoned"}:
                    # deprecated/abandoned 保持不变；approved 才需要降级
                    target_status = approved_target if current_status == "approved" else current_status
                else:
                    # 非法/未知值按 approved 处理
                    target_status = approved_target

                changed = target_status != current_status
                skill_changes.append(
                    {
                        "skill_id": sid,
                        "name": str(row["name"] or ""),
                        "skill_type": str(row["skill_type"] or "methodology"),
                        "source_path": str(row["source_path"] or "").strip() or None,
                        "from_status": current_status,
                        "to_status": target_status,
                        "changed": bool(changed),
                    }
                )
                if not changed:
                    continue
                if dry_run:
                    changed_skill_ids.append(sid)
                    continue
                try:
                    updated = update_skill_status(skill_id=sid, status=str(target_status), conn=conn)
                    if updated is not None:
                        changed_skill_ids.append(sid)
                except Exception as exc:
                    skill_errors.append(f"skill:{sid}: {exc}")

        if include_tools:
            # 先用 LIKE 粗筛（不依赖 SQLite json1），再解析 JSON 精确匹配 created_run_id。
            rows = conn.execute(
                "SELECT id, name, metadata, source_path FROM tools_items WHERE metadata LIKE ? ORDER BY id ASC",
                ("%created_run_id%",),
            ).fetchall()

            for row in rows or []:
                try:
                    tid = int(row["id"])
                except Exception:
                    continue
                if tid <= 0:
                    continue
                meta = _safe_json_obj(row["metadata"])
                approval = _extract_tool_approval(meta)
                if not approval:
                    continue
                created_run_id = approval.get("created_run_id")
                try:
                    created_run_id = int(created_run_id) if created_run_id is not None else None
                except Exception:
                    created_run_id = None
                if created_run_id != int(rid):
                    continue

                current_status = str(approval.get("status") or "").strip().lower() or "approved"
                changed = current_status != tool_target
                tool_changes.append(
                    {
                        "tool_id": tid,
                        "name": str(row["name"] or ""),
                        "source_path": str(row["source_path"] or "").strip() or None,
                        "from_status": current_status,
                        "to_status": tool_target,
                        "changed": bool(changed),
                    }
                )
                if not changed:
                    continue
                if dry_run:
                    changed_tool_ids.append(tid)
                    continue

                try:
                    # 避免覆盖其他字段：只更新 approval 子对象
                    approval_next = dict(approval)
                    approval_next["status"] = tool_target
                    approval_next["rejected_at"] = now_value
                    approval_next["rejected_reason"] = reason_text
                    meta[TOOL_METADATA_APPROVAL_KEY] = approval_next

                    update_tool(
                        tool_id=int(tid),
                        name=None,
                        description=None,
                        version=None,
                        metadata=meta,
                        change_notes=f"maintenance_rollback(run:{rid})",
                        updated_at=now_value,
                        conn=conn,
                    )
                    changed_tool_ids.append(tid)

                    # 同步落盘（失败不阻塞回滚）
                    try:
                        publish_tool_file(int(tid), conn=conn)
                    except Exception:
                        pass
                except Exception as exc:
                    tool_errors.append(f"tool:{tid}: {exc}")

    # 技能文件落盘：需要独立连接（publish_skill_file 内部会读写 DB）
    if include_skills and not dry_run and changed_skill_ids:
        for sid in changed_skill_ids:
            try:
                _source_path, publish_err = publish_skill_file(int(sid))
                if publish_err:
                    skill_errors.append(f"skill:{sid}: publish_failed:{publish_err}")
            except Exception as exc:
                skill_errors.append(f"skill:{sid}: publish_failed:{exc}")

    # 兜底：返回 payload 中补充 tool 的最新 metadata（便于 UI 直接刷新）
    if include_tools and not dry_run and changed_tool_ids:
        try:
            with get_connection() as conn:
                for tid in changed_tool_ids[:50]:
                    row = get_tool(tool_id=int(tid), conn=conn)
                    if not row:
                        continue
        except Exception:
            pass

    return {
        "ok": True,
        "dry_run": bool(dry_run),
        "run_id": int(rid),
        "at": now_value,
        "skills": {
            "matched": len(skill_changes),
            "changed": len(changed_skill_ids),
            "changes": skill_changes[:200],
            "errors": skill_errors[:50],
        },
        "tools": {
            "matched": len(tool_changes),
            "changed": len(changed_tool_ids),
            "changes": tool_changes[:200],
            "errors": tool_errors[:50],
        },
    }


def auto_deprecate_low_quality_knowledge(
    *,
    since_days: int = 30,
    min_calls: int = 3,
    success_rate_threshold: float = 0.3,
    dry_run: bool = False,
    include_skills: bool = True,
    include_tools: bool = True,
    reason: Optional[str] = None,
) -> dict:
    """
    过期机制（质量驱动）：按“最近 success_rate + 最小样本量”自动废弃低质量知识。

    判定信号：
    - tool_call_records.reuse_status in (pass/fail) 的统计（unknown/NULL 不计入成功率分母）
    - 仅当 (pass+fail) >= min_calls 且 success_rate < 阈值 才会触发降级

    动作：
    - skills_items（status=approved）-> deprecated
    - tools_items（approval.status=approved）-> rejected
    """
    try:
        days = int(since_days)
    except Exception:
        days = 30
    if days <= 0:
        days = 30

    try:
        min_calls_value = int(min_calls)
    except Exception:
        min_calls_value = 3
    if min_calls_value < 1:
        min_calls_value = 1

    try:
        threshold = float(success_rate_threshold)
    except Exception:
        threshold = 0.3
    if threshold < 0:
        threshold = 0.0
    if threshold > 1:
        threshold = 1.0

    now_value = now_iso()
    reason_text = str(reason or "").strip() or "maintenance_auto_deprecate"

    # since：ISO string（与 tool_call_records.created_at 字段一致）
    since = None
    try:
        now_dt = datetime.fromisoformat(now_value.replace("Z", "+00:00"))
        since_dt = now_dt - timedelta(days=int(days))
        since = since_dt.isoformat().replace("+00:00", "Z")
    except Exception:
        since = None

    skill_actions: List[dict] = []
    tool_actions: List[dict] = []
    errors: List[str] = []

    changed_skill_ids: List[int] = []
    changed_tool_ids: List[int] = []

    with get_connection() as conn:
        if include_skills:
            # 仅对“存在复用验证信号”的 skill_id 做治理，避免误伤纯文本技能。
            rows = conn.execute(
                "SELECT DISTINCT skill_id FROM tool_call_records WHERE skill_id IS NOT NULL"
                + (" AND created_at >= ?" if since else ""),
                (since,) if since else (),
            ).fetchall()
            skill_ids = []
            for row in rows or []:
                try:
                    sid = int(row["skill_id"])
                except Exception:
                    continue
                if sid > 0 and sid not in skill_ids:
                    skill_ids.append(sid)
                if len(skill_ids) >= 2000:
                    break

            stats_map = get_skill_reuse_quality_map(skill_ids=skill_ids, since=since, conn=conn)
            for sid in skill_ids:
                stats = stats_map.get(int(sid)) or {}
                pass_calls = int(stats.get("pass_calls") or 0)
                fail_calls = int(stats.get("fail_calls") or 0)
                denom = pass_calls + fail_calls
                if denom < int(min_calls_value):
                    continue
                success_rate = float(pass_calls) / float(denom) if denom else 0.0
                if success_rate >= float(threshold):
                    continue

                row = conn.execute(
                    "SELECT id, name, status FROM skills_items WHERE id = ? LIMIT 1",
                    (int(sid),),
                ).fetchone()
                if not row:
                    continue
                current_status = str(row["status"] or "approved").strip().lower() or "approved"
                if current_status != "approved":
                    continue

                skill_actions.append(
                    {
                        "skill_id": int(sid),
                        "name": str(row["name"] or ""),
                        "from_status": current_status,
                        "to_status": "deprecated",
                        "pass_calls": pass_calls,
                        "fail_calls": fail_calls,
                        "success_rate": round(success_rate, 4),
                    }
                )
                if dry_run:
                    changed_skill_ids.append(int(sid))
                    continue
                try:
                    updated = update_skill_status(skill_id=int(sid), status="deprecated", conn=conn)
                    if updated is not None:
                        changed_skill_ids.append(int(sid))
                except Exception as exc:
                    errors.append(f"skill:{sid}: {exc}")

        if include_tools:
            rows = conn.execute(
                "SELECT DISTINCT tool_id FROM tool_call_records"
                + (" WHERE created_at >= ?" if since else ""),
                (since,) if since else (),
            ).fetchall()
            tool_ids = []
            for row in rows or []:
                try:
                    tid = int(row["tool_id"])
                except Exception:
                    continue
                if tid > 0 and tid not in tool_ids:
                    tool_ids.append(tid)
                if len(tool_ids) >= 2000:
                    break

            stats_map = get_tool_reuse_quality_map(tool_ids=tool_ids, since=since, conn=conn)
            for tid in tool_ids:
                stats = stats_map.get(int(tid)) or {}
                pass_calls = int(stats.get("pass_calls") or 0)
                fail_calls = int(stats.get("fail_calls") or 0)
                denom = pass_calls + fail_calls
                if denom < int(min_calls_value):
                    continue
                success_rate = float(pass_calls) / float(denom) if denom else 0.0
                if success_rate >= float(threshold):
                    continue

                tool_row = get_tool(tool_id=int(tid), conn=conn)
                if not tool_row:
                    continue
                meta = _safe_json_obj(tool_row["metadata"])
                approval = _extract_tool_approval(meta)
                current_status = str(approval.get("status") or "approved").strip().lower() or "approved"
                if current_status != "approved":
                    continue

                tool_actions.append(
                    {
                        "tool_id": int(tid),
                        "name": str(tool_row["name"] or ""),
                        "from_status": current_status,
                        "to_status": TOOL_APPROVAL_STATUS_REJECTED,
                        "pass_calls": pass_calls,
                        "fail_calls": fail_calls,
                        "success_rate": round(success_rate, 4),
                    }
                )
                if dry_run:
                    changed_tool_ids.append(int(tid))
                    continue
                try:
                    approval_next = dict(approval)
                    approval_next["status"] = TOOL_APPROVAL_STATUS_REJECTED
                    approval_next["rejected_at"] = now_value
                    approval_next["rejected_reason"] = reason_text
                    meta[TOOL_METADATA_APPROVAL_KEY] = approval_next
                    update_tool(
                        tool_id=int(tid),
                        name=None,
                        description=None,
                        version=None,
                        metadata=meta,
                        change_notes=f"maintenance_auto_deprecate(since_days:{days})",
                        updated_at=now_value,
                        conn=conn,
                    )
                    changed_tool_ids.append(int(tid))
                    try:
                        publish_tool_file(int(tid), conn=conn)
                    except Exception:
                        pass
                except Exception as exc:
                    errors.append(f"tool:{tid}: {exc}")

    # skills：状态更新后同步落盘文件（失败不阻塞）
    if include_skills and not dry_run and changed_skill_ids:
        for sid in changed_skill_ids:
            try:
                _source_path, publish_err = publish_skill_file(int(sid))
                if publish_err:
                    errors.append(f"skill:{sid}: publish_failed:{publish_err}")
            except Exception as exc:
                errors.append(f"skill:{sid}: publish_failed:{exc}")

    return {
        "ok": True,
        "dry_run": bool(dry_run),
        "since_days": int(days),
        "since": since,
        "min_calls": int(min_calls_value),
        "success_rate_threshold": float(threshold),
        "at": now_value,
        "skills": {
            "candidates": len(skill_actions),
            "changed": len(changed_skill_ids),
            "actions": skill_actions[:200],
        },
        "tools": {
            "candidates": len(tool_actions),
            "changed": len(changed_tool_ids),
            "actions": tool_actions[:200],
        },
        "errors": errors[:50],
    }


def rollback_skill_to_previous_version(
    *,
    skill_id: int,
    dry_run: bool = False,
    reason: Optional[str] = None,
) -> dict:
    """
    一键回滚技能到上一版本（基于 skill_version_records 快照）。

    说明：
    - 仅支持“回滚一步”（latest record）；
    - 回滚会写入一条新的 skill_version_records（记录当前 -> 回滚目标），便于再次回滚/追溯；
    - 默认不修改 source_path（保持文件路径稳定），但会重新发布文件内容。
    """
    try:
        sid = int(skill_id)
    except Exception:
        return {"ok": False, "error": "invalid_skill_id"}
    if sid <= 0:
        return {"ok": False, "error": "invalid_skill_id"}

    note = str(reason or "").strip() or "maintenance_rollback_version"

    with get_connection() as conn:
        current = get_skill(skill_id=int(sid), conn=conn)
        if not current:
            return {"ok": False, "error": "skill_not_found", "skill_id": int(sid)}

        rec = conn.execute(
            "SELECT id, previous_version, next_version, previous_snapshot, created_at FROM skill_version_records "
            "WHERE skill_id = ? ORDER BY id DESC LIMIT 1",
            (int(sid),),
        ).fetchone()
        if not rec:
            return {"ok": False, "error": "no_previous_version", "skill_id": int(sid)}

        snap = _safe_json_obj(rec["previous_snapshot"])
        if not snap:
            return {"ok": False, "error": "invalid_snapshot", "skill_id": int(sid), "record_id": int(rec["id"])}

        target_version = snap.get("version")
        from_version = current["version"]
        if dry_run:
            return {
                "ok": True,
                "dry_run": True,
                "skill_id": int(sid),
                "record_id": int(rec["id"]),
                "from_version": from_version,
                "to_version": target_version,
                "from_name": str(current["name"] or ""),
                "to_name": str(snap.get("name") or ""),
                "at": now_iso(),
            }

        # 记录“回滚动作”（当前 -> 目标），便于追溯与二次回滚
        try:
            current_snapshot = {k: current[k] for k in current.keys()}
            conn.execute(
                "INSERT INTO skill_version_records (skill_id, previous_version, next_version, previous_snapshot, change_notes, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    int(sid),
                    current["version"],
                    target_version,
                    json.dumps(current_snapshot, ensure_ascii=False),
                    f"rollback(record:{int(rec['id'])}):{note}",
                    now_iso(),
                ),
            )
        except Exception:
            pass

        # 回滚：用快照字段覆盖（source_path/created_at 保持不变，避免路径漂移）
        conn.execute(
            """
            UPDATE skills_items
            SET name = ?,
                description = ?,
                scope = ?,
                category = ?,
                tags = ?,
                triggers = ?,
                aliases = ?,
                prerequisites = ?,
                inputs = ?,
                outputs = ?,
                steps = ?,
                failure_modes = ?,
                validation = ?,
                version = ?,
                task_id = ?,
                domain_id = ?,
                skill_type = ?,
                status = ?,
                source_task_id = ?,
                source_run_id = ?
            WHERE id = ?
            """,
            (
                snap.get("name") if snap.get("name") is not None else current["name"],
                snap.get("description"),
                snap.get("scope"),
                snap.get("category"),
                snap.get("tags"),
                snap.get("triggers"),
                snap.get("aliases"),
                snap.get("prerequisites"),
                snap.get("inputs"),
                snap.get("outputs"),
                snap.get("steps"),
                snap.get("failure_modes"),
                snap.get("validation"),
                target_version if target_version is not None else current["version"],
                snap.get("task_id"),
                snap.get("domain_id"),
                snap.get("skill_type"),
                snap.get("status"),
                snap.get("source_task_id"),
                snap.get("source_run_id"),
                int(sid),
            ),
        )

    # 落盘（失败不阻塞主流程）
    publish_err = None
    source_path = None
    try:
        source_path, publish_err = publish_skill_file(int(sid))
    except Exception as exc:
        publish_err = str(exc)

    return {
        "ok": True,
        "dry_run": False,
        "skill_id": int(sid),
        "record_id": int(rec["id"]),
        "from_version": from_version,
        "to_version": target_version,
        "source_path": source_path,
        "publish_error": publish_err,
        "at": now_iso(),
    }


def rollback_tool_to_previous_version(
    *,
    tool_id: int,
    dry_run: bool = False,
    reason: Optional[str] = None,
) -> dict:
    """
    一键回滚工具到上一版本（基于 tool_version_records.previous_snapshot）。

    说明：
    - 仅支持“回滚一步”（latest record）；
    - 回滚会写入一条新的 tool_version_records（记录当前 -> 回滚目标），便于追溯；
    - 默认不修改 source_path（保持文件路径稳定），但会重新发布文件内容。
    """
    try:
        tid = int(tool_id)
    except Exception:
        return {"ok": False, "error": "invalid_tool_id"}
    if tid <= 0:
        return {"ok": False, "error": "invalid_tool_id"}

    note = str(reason or "").strip() or "maintenance_rollback_version"

    with get_connection() as conn:
        current = get_tool(tool_id=int(tid), conn=conn)
        if not current:
            return {"ok": False, "error": "tool_not_found", "tool_id": int(tid)}

        rec = conn.execute(
            "SELECT id, previous_version, next_version, previous_snapshot, created_at FROM tool_version_records "
            "WHERE tool_id = ? ORDER BY id DESC LIMIT 1",
            (int(tid),),
        ).fetchone()
        if not rec:
            return {"ok": False, "error": "no_previous_version", "tool_id": int(tid)}

        snap = _safe_json_obj(rec["previous_snapshot"])
        if not snap:
            return {"ok": False, "error": "invalid_snapshot", "tool_id": int(tid), "record_id": int(rec["id"])}

        target_version = snap.get("version") or rec["previous_version"]
        from_version = current["version"]
        if dry_run:
            return {
                "ok": True,
                "dry_run": True,
                "tool_id": int(tid),
                "record_id": int(rec["id"]),
                "from_version": from_version,
                "to_version": target_version,
                "from_name": str(current["name"] or ""),
                "to_name": str(snap.get("name") or ""),
                "at": now_iso(),
            }

        # 记录“回滚动作”（当前 -> 目标），便于追溯与二次回滚
        try:
            current_snapshot = {k: current[k] for k in current.keys()}
            conn.execute(
                "INSERT INTO tool_version_records (tool_id, previous_version, next_version, previous_snapshot, change_notes, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    int(tid),
                    current["version"],
                    target_version,
                    json.dumps(current_snapshot, ensure_ascii=False),
                    f"rollback(record:{int(rec['id'])}):{note}",
                    now_iso(),
                ),
            )
        except Exception:
            pass

        # metadata：允许回滚到 NULL
        meta_value = snap.get("metadata")
        if meta_value is None:
            metadata_text = None
        elif isinstance(meta_value, dict):
            metadata_text = json.dumps(meta_value, ensure_ascii=False)
        elif isinstance(meta_value, str):
            # snap 中的 metadata 通常是 raw DB string
            metadata_text = meta_value
        else:
            metadata_text = None

        conn.execute(
            "UPDATE tools_items SET name = ?, description = ?, version = ?, metadata = ?, updated_at = ? WHERE id = ?",
            (
                snap.get("name") if snap.get("name") is not None else current["name"],
                snap.get("description") if snap.get("description") is not None else current["description"],
                str(target_version or current["version"]),
                metadata_text,
                now_iso(),
                int(tid),
            ),
        )

        # 同步落盘（失败不阻塞）
        publish_err = None
        try:
            publish_tool_file(int(tid), conn=conn)
        except Exception as exc:
            publish_err = str(exc)

    return {
        "ok": True,
        "dry_run": False,
        "tool_id": int(tid),
        "record_id": int(rec["id"]),
        "from_version": from_version,
        "to_version": target_version,
        "publish_error": publish_err,
        "at": now_iso(),
    }
