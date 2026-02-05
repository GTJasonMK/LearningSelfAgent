from __future__ import annotations

from pathlib import Path
import sqlite3
import shutil


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def remove_path(path: Path, removed: list[str]) -> None:
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
        removed.append(str(path))
        return
    if path.is_file():
        path.unlink(missing_ok=True)
        removed.append(str(path))


def remove_glob(root: Path, pattern: str, keep_names: set[str], removed: list[str]) -> None:
    for item in root.glob(pattern):
        if item.name in keep_names:
            continue
        if item.is_file():
            item.unlink(missing_ok=True)
            removed.append(str(item))
        elif item.is_dir():
            shutil.rmtree(item, ignore_errors=True)
            removed.append(str(item))


def main() -> None:
    removed: list[str] = []
    warnings: list[str] = []

    # 清理 SQLite 数据库（保留配置与历史输入）
    db_path = PROJECT_ROOT / "backend/data/agent.db"
    if db_path.exists():
        try:
            # 设置超时与 busy_timeout：在后端短暂占用 DB 时，等待锁释放而不是直接失败。
            con = sqlite3.connect(db_path, timeout=10)
            cur = con.cursor()
            try:
                cur.execute("PRAGMA busy_timeout = 8000")
            except Exception:
                pass
            cur.execute("PRAGMA foreign_keys = OFF")
            cur.execute("select name from sqlite_master where type='table'")
            tables = [row[0] for row in cur.fetchall()]

            keep_tables = {"config_store", "chat_messages"}
            clear_tables = [t for t in tables if t not in keep_tables and t != "sqlite_sequence"]

            for table in clear_tables:
                cur.execute(f"delete from {table}")
            for table in clear_tables:
                cur.execute("delete from sqlite_sequence where name = ?", (table,))

            con.commit()
            con.close()
            removed.append(str(db_path) + " (清空数据，保留 config_store/chat_messages)")
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
    remove_glob(PROJECT_ROOT, "backend/prompt/memory/*.md", set(), removed)
    remove_glob(PROJECT_ROOT, "backend/prompt/tools/*.md", {"README.md"}, removed)
    remove_glob(PROJECT_ROOT, "backend/prompt/skills/**/*.md", {"README.md"}, removed)
    remove_glob(PROJECT_ROOT, "backend/prompt/graph/nodes/*.md", set(), removed)
    remove_glob(PROJECT_ROOT, "backend/prompt/graph/edges/*.md", set(), removed)

    # 清理 .trash
    for trash_dir in (PROJECT_ROOT / "backend/prompt").rglob(".trash"):
        remove_path(trash_dir, removed)

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
