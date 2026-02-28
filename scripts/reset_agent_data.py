from __future__ import annotations

import os
from pathlib import Path
import sqlite3
import shutil
import sys
from datetime import datetime, timezone


PROJECT_ROOT = Path(__file__).resolve().parents[1]
try:
    # 允许脚本在任意工作目录执行，并能 import backend 模块
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
except Exception:
    pass


def resolve_target_db_path() -> Path:
    """
    解析需要清理的 SQLite DB 路径。

    优先级：
    1) 环境变量 AGENT_DB_PATH（与后端一致）
    2) 默认路径 backend/data/agent.db

    注意：
    - reset 只支持“文件 DB”（不支持 ':memory:' 或 SQLite URI）；若检测到不支持，将回退到默认 DB。
    """
    try:
        from backend.src.storage import resolve_db_path

        raw = str(resolve_db_path() or "").strip()
    except Exception:
        raw = str(os.environ.get("AGENT_DB_PATH") or "").strip()

    if not raw:
        return (PROJECT_ROOT / "backend" / "data" / "agent.db").resolve()

    if raw == ":memory:" or raw.startswith("file:"):
        return (PROJECT_ROOT / "backend" / "data" / "agent.db").resolve()

    path = Path(raw)
    if not path.is_absolute():
        return (PROJECT_ROOT / path).resolve()
    return path.resolve()


def resolve_target_prompt_root() -> Path:
    """
    解析需要清理的 prompt 根目录（与后端一致）。

    优先级：
    1) 环境变量 AGENT_PROMPT_ROOT
    2) 默认 backend/prompt
    """
    try:
        from backend.src.prompt.paths import prompt_root

        return Path(prompt_root()).resolve()
    except Exception:
        raw = str(os.environ.get("AGENT_PROMPT_ROOT") or "").strip()
        if raw:
            path = Path(raw).expanduser()
            if not path.is_absolute():
                return (PROJECT_ROOT / path).resolve()
            return path.resolve()
        return (PROJECT_ROOT / "backend" / "prompt").resolve()


def remove_path(path: Path, removed: list[str]) -> None:
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
        removed.append(str(path))
        return
    if path.is_file():
        path.unlink(missing_ok=True)
        removed.append(str(path))


def _rel_path(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root)).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def remove_glob(
    root: Path,
    pattern: str,
    keep_names: set[str],
    removed: list[str],
    decisions: list[str] | None = None,
    keep_reason: str = "kept_by_name",
    delete_reason: str = "matched_pattern",
) -> None:
    if not root.exists():
        return
    for item in root.glob(pattern):
        rel = _rel_path(item, root)
        if item.name in keep_names:
            if decisions is not None:
                decisions.append(f"KEEP {rel} [{keep_reason}]")
            continue
        try:
            if item.is_file():
                item.unlink(missing_ok=True)
                removed.append(str(item))
                if decisions is not None:
                    decisions.append(f"DELETE {rel} [{delete_reason}]")
            elif item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
                removed.append(str(item))
                if decisions is not None:
                    decisions.append(f"DELETE {rel} [{delete_reason}]")
        except Exception:
            if decisions is not None:
                decisions.append(f"KEEP {rel} [delete_failed]")


def _read_frontmatter_meta(path: Path) -> dict:
    """
    读取 prompt markdown 的 frontmatter meta（兼容 YAML/JSON）。
    """
    try:
        from backend.src.prompt.skill_files import parse_skill_markdown
    except Exception:
        return {}

    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return {}

    try:
        parsed = parse_skill_markdown(text=text, source_path=str(path))
        return parsed.meta or {}
    except Exception:
        return {}


def _is_draft_tool_file(path: Path) -> bool:
    """
    判断 tools/*.md 是否为“draft 工具”文件。

    说明：
    - Agent 执行阶段创建的新工具默认 approval.status=draft；
    - reset 时优先清理 draft 工具，保留内置/已批准工具（避免把系统资源也清空）。
    """
    meta = _read_frontmatter_meta(path)
    if not isinstance(meta, dict):
        return False

    metadata = meta.get("metadata")
    if not isinstance(metadata, dict):
        return False
    approval = metadata.get("approval")
    if not isinstance(approval, dict):
        return False
    return str(approval.get("status") or "").strip().lower() == "draft"


def remove_draft_tool_files(
    prompt_root: Path,
    removed: list[str],
    tool_decisions: list[str],
) -> None:
    tools_root = prompt_root / "tools"
    if not tools_root.exists():
        return
    for path in tools_root.rglob("*.md"):
        if not path.is_file():
            continue
        rel = _rel_path(path, prompt_root)
        name = path.name.lower()
        if name in {"readme.md", "_readme.md"}:
            tool_decisions.append(f"KEEP {rel} [readme]")
            continue
        # 跳过隐藏目录/文件（例如 .trash）
        if any(part.startswith(".") for part in path.parts):
            tool_decisions.append(f"KEEP {rel} [hidden_path]")
            continue
        if _is_draft_tool_file(path):
            try:
                path.unlink(missing_ok=True)
                removed.append(str(path))
                tool_decisions.append(f"DELETE {rel} [draft_tool]")
            except Exception:
                tool_decisions.append(f"KEEP {rel} [delete_failed]")
        else:
            tool_decisions.append(f"KEEP {rel} [approved_or_builtin]")


def _as_lowered_text_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        out: list[str] = []
        for item in value:
            text = str(item or "").strip().lower()
            if text:
                out.append(text)
        return out
    text = str(value or "").strip().lower()
    if not text:
        return []
    if "," in text:
        return [s.strip().lower() for s in text.split(",") if s.strip()]
    return [text]


def _parse_positive_int(value: object) -> int:
    try:
        num = int(value)
    except Exception:
        return 0
    return num if num > 0 else 0


def _is_system_skill_file(path: Path, skills_root: Path) -> bool:
    """
    判断 skills/*.md 是否为系统技能文件（reset 时保留）。

    判定规则（任一命中即保留）：
    1) 路径位于 skills/system/**；
    2) frontmatter 的 scope/category/domain(_id) 以 system 开头；
    3) tags 包含 system / system:* / domain:system。
    """
    try:
        rel = path.relative_to(skills_root)
        parts = [str(p).strip().lower() for p in rel.parts if str(p).strip()]
    except Exception:
        parts = [str(path.name).strip().lower()]
    if parts and parts[0] == "system":
        return True

    meta = _read_frontmatter_meta(path)
    if not isinstance(meta, dict):
        return False

    scope = str(meta.get("scope") or "").strip().lower()
    category = str(meta.get("category") or "").strip().lower()
    domain = str(meta.get("domain_id") or meta.get("domain") or "").strip().lower()
    for value in (scope, category, domain):
        if value == "system" or value.startswith("system."):
            return True

    tags = _as_lowered_text_list(meta.get("tags"))
    for tag in tags:
        if tag == "system" or tag.startswith("system:") or tag.startswith("domain:system"):
            return True

    return False


def _is_generated_skill_file(path: Path) -> bool:
    """
    判断 skill 是否由运行期自动生成（reset 时可清理）。

    判定规则（任一命中即视为 generated）：
    1) source_task_id/source_run_id 为正整数；
    2) scope 以 tool: 或 solution:run: 开头；
    3) tags 包含 task:* 或 run:*。
    """
    meta = _read_frontmatter_meta(path)
    if not isinstance(meta, dict):
        return False

    if _parse_positive_int(meta.get("source_task_id")) > 0:
        return True
    if _parse_positive_int(meta.get("source_run_id")) > 0:
        return True

    scope = str(meta.get("scope") or "").strip().lower()
    if scope.startswith("tool:") or scope.startswith("solution:run:"):
        return True

    tags = _as_lowered_text_list(meta.get("tags"))
    for tag in tags:
        if tag.startswith("task:") or tag.startswith("run:"):
            return True

    return False


def remove_non_system_skill_files(
    prompt_root: Path,
    removed: list[str],
    skill_decisions: list[str],
) -> None:
    """
    清理 skills 目录中的“自动生成且非系统”的技能文件。

    说明：
    - 系统 skill 始终保留；
    - 未标记为 generated 的 skill 默认保留（避免误删手工维护/内置技能）。
    """
    skills_root = prompt_root / "skills"
    if not skills_root.exists():
        return

    for path in skills_root.rglob("*.md"):
        if not path.is_file():
            continue
        name = path.name.lower()
        if name in {"readme.md", "_readme.md"}:
            continue
        if any(part.startswith(".") for part in path.parts):
            continue
        rel = _rel_path(path, prompt_root)
        if _is_system_skill_file(path, skills_root):
            skill_decisions.append(f"KEEP {rel} [system_skill]")
            continue
        if not _is_generated_skill_file(path):
            skill_decisions.append(f"KEEP {rel} [non_generated_or_curated]")
            continue
        try:
            path.unlink(missing_ok=True)
            removed.append(str(path))
            skill_decisions.append(f"DELETE {rel} [generated_non_system]")
        except Exception:
            skill_decisions.append(f"KEEP {rel} [delete_failed]")
            pass


def restore_system_skills_into_db(prompt_root: Path, removed: list[str], warnings: list[str]) -> None:
    """
    将保留下来的系统 skill 文件同步回 DB，避免 reset 后 skills_items 全空。
    """
    skills_root = prompt_root / "skills"
    if not skills_root.exists():
        return
    try:
        from backend.src.services.skills.skills_sync import sync_skills_from_files
    except Exception as exc:
        warnings.append(f"skills_sync_import_failed: {exc}")
        return

    try:
        result = sync_skills_from_files(base_dir=skills_root, prune=True)
        inserted = int(result.get("inserted") or 0)
        updated = int(result.get("updated") or 0)
        deleted = int(result.get("deleted") or 0)
        if inserted > 0 or updated > 0 or deleted > 0:
            removed.append(
                "skills_items 同步完成"
                f" (inserted={inserted}, updated={updated}, deleted={deleted})"
            )
    except Exception as exc:
        warnings.append(f"skills_sync_failed: {exc}")


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _probe_fts(cur: sqlite3.Cursor, table_name: str) -> bool:
    """
    探测 FTS 虚拟表是否可用（避免 vtable constructor failed）。
    """
    try:
        exists = cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (str(table_name),),
        ).fetchone()
    except Exception:
        exists = None
    if not exists:
        return False
    try:
        cur.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def _fts_delete_all(cur: sqlite3.Cursor, table_name: str) -> None:
    """
    清空 FTS5 索引（不删除 shadow tables，避免 reset 误删导致 vtable 损坏）。
    """
    cur.execute(f"INSERT INTO {table_name}({table_name}) VALUES('delete-all');")


def _fts_rebuild_if_needed(cur: sqlite3.Cursor, table_name: str, content_table: str) -> None:
    """
    若主表非空而 FTS 为空，则触发 rebuild（补齐历史数据）。
    """
    try:
        base_count = int(cur.execute(f"SELECT COUNT(*) FROM {content_table}").fetchone()[0])
        fts_count = int(cur.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
    except Exception:
        return
    if base_count > 0 and fts_count == 0:
        cur.execute(f"INSERT INTO {table_name}({table_name}) VALUES('rebuild');")


def _fts_table_state(cur: sqlite3.Cursor, table_name: str) -> str:
    """
    判断 FTS 虚拟表的状态：missing / ok / broken。

    说明：
    - 仅看 sqlite_master 不够：如果误删了 shadow tables（*_fts_config/_data/_idx/_docsize），
      会出现 `vtable constructor failed`；
    - reset 时若继续触发主表上的 FTS trigger，会导致清理失败或数据库进入半清理状态。
    """
    try:
        exists = cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (str(table_name),),
        ).fetchone()
    except Exception:
        exists = None
    if not exists:
        return "missing"
    try:
        cur.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone()
        return "ok"
    except Exception:
        return "broken"


def _backup_db_files(db_path: Path, removed: list[str]) -> Path:
    """
    备份 agent.db（以及 wal/shm）到同目录下的 .bak 文件，避免误操作不可恢复。
    """
    tag = _now_tag()
    backup_path = db_path.with_suffix(f".db.bak.{tag}")
    db_path.rename(backup_path)
    removed.append(f"{db_path} -> {backup_path}")

    wal = db_path.with_suffix(".db-wal")
    shm = db_path.with_suffix(".db-shm")
    for side in (wal, shm):
        try:
            if side.exists():
                side_backup = side.with_suffix(side.suffix + f".bak.{tag}")
                side.rename(side_backup)
                removed.append(f"{side} -> {side_backup}")
        except Exception:
            # side files 可能被占用：忽略，不阻塞主流程
            pass

    return backup_path


def _rebuild_db_with_preserved_tables(
    *,
    db_path: Path,
    preserved_config_rows: list[dict],
    removed: list[str],
    warnings: list[str],
) -> None:
    """
    当 DB 已处于不一致/FTS 损坏状态时，采用“重建 DB + 回灌少量保留表”的方式复位。

    保留内容：
    - config_store：用户配置（LLM/开关）
    """
    try:
        _backup_db_files(db_path, removed)
    except Exception as exc:
        warnings.append(f"db_backup_failed: {exc}")
        raise

    try:
        sys.path.insert(0, str(PROJECT_ROOT))
    except Exception:
        pass

    # 让 backend/src/storage.py 使用与脚本一致的 DB 路径
    os.environ["AGENT_DB_PATH"] = str(db_path)

    try:
        from backend.src import storage

        storage.reset_db_cache()
        storage.init_db()
    except Exception as exc:
        warnings.append(f"db_rebuild_failed: {exc}")
        raise

    # 回灌保留表
    try:
        con = sqlite3.connect(db_path, timeout=10)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        try:
            cur.execute("PRAGMA busy_timeout = 8000")
        except Exception:
            pass

        # config_store：重建后会有 seed 默认行；这里用“全量覆盖”的方式回灌。
        if preserved_config_rows:
            cur.execute("DELETE FROM config_store")
            for row in preserved_config_rows:
                cur.execute(
                    "INSERT INTO config_store (id, tray_enabled, pet_enabled, panel_enabled, llm_provider, llm_api_key, llm_base_url, llm_model) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        row.get("id"),
                        row.get("tray_enabled"),
                        row.get("pet_enabled"),
                        row.get("panel_enabled"),
                        row.get("llm_provider"),
                        row.get("llm_api_key"),
                        row.get("llm_base_url"),
                        row.get("llm_model"),
                    ),
                )

        con.commit()
        con.close()
    except Exception as exc:
        try:
            con.close()
        except Exception:
            pass
        warnings.append(f"db_restore_failed: {exc}")
        raise


def main() -> None:
    removed: list[str] = []
    warnings: list[str] = []
    memory_decisions: list[str] = []
    tool_decisions: list[str] = []
    skill_decisions: list[str] = []

    # 清理 SQLite 数据库（仅保留配置）
    db_path = resolve_target_db_path()
    if db_path.exists():
        try:
            # 设置超时与 busy_timeout：在后端短暂占用 DB 时，等待锁释放而不是直接失败。
            con = sqlite3.connect(db_path, timeout=10)
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            try:
                cur.execute("PRAGMA busy_timeout = 8000")
            except Exception:
                pass
            cur.execute("PRAGMA foreign_keys = OFF")

            # 先读取需要保留的少量表，以便在 DB 损坏时走“重建回灌”路径
            preserved_config_rows: list[dict] = []
            need_rebuild = False
            try:
                preserved_config_rows = [dict(r) for r in cur.execute("SELECT * FROM config_store").fetchall()]
            except sqlite3.OperationalError as exc:
                # 老库/半清理库：config_store 不存在，直接重建（用户配置无法保留）
                if "no such table" in str(exc).lower() and "config_store" in str(exc).lower():
                    need_rebuild = True
                    warnings.append("config_store_missing: will rebuild db")
                else:
                    raise

            # 检测 FTS 虚拟表是否可用（shadow tables 被误删会导致 vtable constructor failed）
            memory_fts_state = _fts_table_state(cur, "memory_items_fts")
            skills_fts_state = _fts_table_state(cur, "skills_items_fts")

            # 若已损坏：直接重建 DB（否则继续“表清空”会触发 triggers 导致半清理/失败）
            if need_rebuild or (memory_fts_state == "broken") or (skills_fts_state == "broken"):
                con.close()
                _rebuild_db_with_preserved_tables(
                    db_path=db_path,
                    preserved_config_rows=preserved_config_rows,
                    removed=removed,
                    warnings=warnings,
                )
                removed.append(str(db_path) + " (重建数据库并回灌 config_store)")
                con = None
                cur = None
                # 继续执行文件清理（backend/.agent + prompt）
            else:
                cur.execute("select name from sqlite_master where type='table'")
                tables = [row[0] for row in cur.fetchall()]

                keep_tables = {"config_store"}
                # 关键修复：不要直接清空 FTS5 shadow tables（会导致 vtable constructor failed）
                def _is_fts_shadow(name: str) -> bool:
                    n = str(name or "")
                    return n.endswith("_fts") or ("_fts_" in n)

                clear_tables = [
                    t
                    for t in tables
                    if t not in keep_tables and t != "sqlite_sequence" and not _is_fts_shadow(t)
                ]

                for table in clear_tables:
                    cur.execute(f"delete from {table}")
                for table in clear_tables:
                    cur.execute("delete from sqlite_sequence where name = ?", (table,))

                # 额外清理：FTS 索引可能因 trigger 被禁用而残留（skills_items_fts 等会出现“主表为空但 FTS 仍有行”）。
                # 这里使用 FTS5 自带 delete-all 指令，避免误删 shadow tables 导致 vtable 损坏。
                for fts_table, content_table in (("memory_items_fts", "memory_items"), ("skills_items_fts", "skills_items")):
                    if _probe_fts(cur, fts_table):
                        try:
                            _fts_delete_all(cur, fts_table)
                            _fts_rebuild_if_needed(cur, fts_table, content_table)
                            removed.append(f"{fts_table} (delete-all)")
                        except Exception as exc:
                            warnings.append(f"fts_delete_all_failed:{fts_table}:{exc}")

                con.commit()
                con.close()
                removed.append(
                    str(db_path)
                    + " (清空数据，仅保留 config_store；FTS shadow tables 未直接清空；FTS 索引已 delete-all)"
                )
        except Exception as exc:
            try:
                con.close()
            except Exception:
                pass
            msg = (
                "数据库清理失败，已跳过删除 agent.db 以避免丢失历史/配置。"
                f"\n原因: {exc}"
                "\n建议: 请先退出后端/Electron（确保没有进程占用 backend/data/agent.db），再重试 reset。"
            )
            print(msg)
            warnings.append(f"db_cleanup_failed: {exc}")
    else:
        print("未发现 agent.db，跳过数据库清理。")
    # WAL/SHM 不再强制删除：数据库占用时删除 WAL/SHM 可能导致误清理或行为不确定

    # 清理运行时工作目录
    remove_path(PROJECT_ROOT / "backend/.agent", removed)

    # 清理 prompt 数据（保留 README）
    prompt_root = resolve_target_prompt_root()
    # memory：允许层级目录（未来扩展），统一用 **/*.md
    remove_glob(
        prompt_root,
        "memory/**/*.md",
        {"README.md"},
        removed,
        decisions=memory_decisions,
        keep_reason="readme",
        delete_reason="memory_data",
    )
    # tools：只清理 draft 工具，保留内置/已批准工具（避免系统资源被 reset 掉）
    remove_draft_tool_files(prompt_root, removed, tool_decisions)
    # skills：仅清理“自动生成且非系统”的技能，保留系统/手工维护技能
    remove_non_system_skill_files(prompt_root, removed, skill_decisions)
    remove_glob(prompt_root, "graph/nodes/**/*.md", {"README.md"}, removed)
    remove_glob(prompt_root, "graph/edges/**/*.md", {"README.md"}, removed)

    # 清理 .trash
    for trash_dir in prompt_root.rglob(".trash"):
        remove_path(trash_dir, removed)

    # 回灌系统 skill 到 DB（保证 reset 后系统技能可立即检索）
    restore_system_skills_into_db(prompt_root, removed, warnings)

    if memory_decisions:
        print("memory 判定：")
        for item in memory_decisions:
            print(f"- {item}")

    if tool_decisions:
        print("tools 判定：")
        for item in tool_decisions:
            print(f"- {item}")

    if skill_decisions:
        print("skills 判定：")
        for item in skill_decisions:
            print(f"- {item}")

    print("已清理 Agent 数据，恢复初始状态。")
    if removed:
        print("清理内容：")
        for item in removed:
            print(f"- {item}")
    if warnings:
        print("注意：存在清理未完成的项：")
        for w in warnings:
            print(f"- {w}")
    if not removed and not warnings:
        print("没有发现可清理的数据。")


if __name__ == "__main__":
    main()
