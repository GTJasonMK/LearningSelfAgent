import re
import sqlite3
from typing import List


def fts_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """
    判断 FTS 虚拟表是否存在。

    说明：
    - 某些 Python/SQLite 构建可能未开启 FTS5；此时 init_db 创建 FTS 会失败并被忽略。
    - 查询侧必须容错：表不存在就回退到 LIKE 方案。
    """
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            (str(table_name),),
        ).fetchone()
        return bool(row and row["name"])
    except Exception:
        return False


def extract_search_terms(text: str, limit: int = 8) -> List[str]:
    """
    从自然语言里提取“用于检索”的少量关键词。

    设计目标：
    - 不依赖额外 LLM 调用（更快、更稳定）
    - 让 FTS/LIKE 都能吃到更干净的 query（减少噪声）

    规则（尽量保守）：
    - 英文/数字/路径片段：长度 >= 3
    - 中文片段：长度 2-6（避免把整句塞给 FTS 导致召回不准）
    """
    raw = str(text or "").strip()
    if not raw:
        return []
    terms: List[str] = []
    seen = set()

    for token in re.findall(r"[A-Za-z0-9_./:\\-]{3,}", raw):
        t = token.strip()
        if t and t not in seen:
            seen.add(t)
            terms.append(t)
        if len(terms) >= limit:
            return terms

    def _push(value: str) -> None:
        v = str(value or "").strip()
        if not v:
            return
        if v in seen:
            return
        seen.add(v)
        terms.append(v)

    for token in re.findall(r"[\u4e00-\u9fff]{2,6}", raw):
        t = token.strip()
        if not t:
            continue

        # 保留原始片段（短句通常能直接命中）
        _push(t)
        if len(terms) >= limit:
            return terms

        # 对较长中文片段，再拆出 2 字关键词（提高“部分匹配”召回）
        if len(t) >= 4:
            _push(t[:2])
            if len(terms) >= limit:
                return terms
            _push(t[-2:])
            if len(terms) >= limit:
                return terms
            mid_start = max(0, (len(t) // 2) - 1)
            _push(t[mid_start : mid_start + 2])
            if len(terms) >= limit:
                return terms

    return terms


def build_fts_or_query(text: str, limit: int = 8) -> str:
    """
    将自然语言转换为 FTS5 的 MATCH 查询（OR + 前缀匹配）。

    说明：
    - FTS5 的高级语法很强，但用户输入往往包含标点/引号/括号，容易触发语法错误。
    - 我们只生成“安全子集”：term 或 term*，并用 OR 连接，避免语法炸裂。
    - 对中文检索，unicode61 分词通常按连续中文段落切词；加上前缀匹配能提升“部分匹配”的可用性。
    """
    terms = extract_search_terms(text, limit=limit)
    if not terms:
        return ""
    safe: List[str] = []
    for t in terms:
        value = str(t).strip()
        if not value:
            continue
        # 仅允许字母/数字/下划线/中文，避免引号等字符导致 MATCH 语法错误
        cleaned = re.sub(r"[^A-Za-z0-9_\u4e00-\u9fff]+", "", value)
        if not cleaned:
            continue
        # 统一使用前缀匹配：提升“部分词/中文片段”召回
        safe.append(f"{cleaned}*")
    return " OR ".join(safe)
