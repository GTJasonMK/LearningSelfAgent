import json
import logging
import sqlite3
import threading
import time
from collections import deque
from typing import Deque, Optional, Tuple

from backend.src.common.utils import extract_json_object, is_test_env, now_iso
from backend.src.constants import (
    GRAPH_EXTRACT_MAX_ATTEMPTS,
    GRAPH_EXTRACT_STATUS_DONE,
    GRAPH_EXTRACT_STATUS_FAILED,
    GRAPH_EXTRACT_STATUS_QUEUED,
    GRAPH_EXTRACT_STATUS_RUNNING,
    GRAPH_LLM_MAX_CHARS,
    GRAPH_LLM_PROMPT_TEMPLATE,
)
from backend.src.services.llm.llm_client import call_openai, resolve_default_model
from backend.src.storage import get_connection, resolve_db_path

logger = logging.getLogger(__name__)

# item: (db_path, extract_id, task_id, run_id, content)
_graph_extract_queue: Deque[Tuple[str, int, int, int, str]] = deque()
_graph_extract_lock = threading.Lock()
_graph_extract_thread_started = False
_graph_extract_db_path: Optional[str] = None


def _parse_graph_payload(raw):
    if raw is None:
        return None
    payload = raw
    if isinstance(raw, str):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = extract_json_object(raw)
            if payload is None:
                return None
    if not isinstance(payload, dict):
        return None
    if isinstance(payload.get("graph"), dict):
        payload = payload["graph"]
    if "nodes" not in payload and "edges" not in payload:
        response_value = payload.get("response") or payload.get("result")
        response_payload = None
        if isinstance(response_value, str):
            response_payload = extract_json_object(response_value)
        elif isinstance(response_value, dict):
            response_payload = response_value
        if isinstance(response_payload, dict):
            if isinstance(response_payload.get("graph"), dict):
                response_payload = response_payload["graph"]
            payload = response_payload
    if "nodes" not in payload and "edges" not in payload:
        return None
    return payload


def extract_graph_updates(task_id: int, run_id: int, step_rows, output_rows) -> dict:
    """
    尝试从 step_rows/result 与 output_rows/content 中提取图谱变更并写入 DB。

    策略：
    1) 若已有结构化 graph JSON（含 nodes/edges），直接落库；
    2) 否则把证据文本拼接后进入 graph_extract_tasks 队列，后台线程用 LLM 抽取。
    """
    graphs = []
    for row in step_rows:
        graphs.append(_parse_graph_payload(row["result"]))
    for row in output_rows:
        graphs.append(_parse_graph_payload(row["content"]))
    graphs = [item for item in graphs if item]
    if graphs:
        return _apply_graphs(task_id, run_id, graphs)

    evidence_texts = []
    for row in step_rows:
        if row["result"]:
            evidence_texts.append(str(row["result"]))
    for row in output_rows:
        if row["content"]:
            evidence_texts.append(str(row["content"]))
    if not evidence_texts:
        return {"nodes_created": 0, "edges_created": 0}

    content = "\n".join(evidence_texts)
    content = content[:GRAPH_LLM_MAX_CHARS]
    extract_id = _enqueue_graph_extraction(task_id, run_id, content)
    return {"nodes_created": 0, "edges_created": 0, "queued": True, "extract_id": extract_id}


def _apply_graphs(task_id: int, run_id: int, graphs, *, db_path: Optional[str] = None) -> dict:
    nodes_created = 0
    edges_created = 0
    label_map = {}
    from backend.src.services.graph.graph_store import publish_graph_edge_file, publish_graph_node_file

    # 说明：逐条落库 + 落盘，避免一次事务里部分文件已写但 DB 回滚导致不一致。
    for graph in graphs:
        for node in graph.get("nodes", []) or []:
            if not isinstance(node, dict):
                continue
            label = node.get("label")
            if not label:
                continue
            # 若已存在节点（按 label 去重），仅记录映射
            with get_connection(db_path=db_path) as conn:
                existing = conn.execute(
                    "SELECT id FROM graph_nodes WHERE label = ? ORDER BY id ASC LIMIT 1",
                    (label,),
                ).fetchone()
                if existing:
                    label_map[label] = int(existing["id"])
                    continue

                attributes = node.get("attributes")
                attributes_value = json.dumps(attributes, ensure_ascii=False) if isinstance(attributes, dict) else None
                created_at = now_iso()
                try:
                    cursor = conn.execute(
                        "INSERT INTO graph_nodes (label, created_at, node_type, attributes, task_id, evidence) VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            label,
                            created_at,
                            node.get("node_type"),
                            attributes_value,
                            task_id,
                            node.get("evidence") or f"auto:task:{task_id}:run:{run_id}",
                        ),
                    )
                    node_id = int(cursor.lastrowid)
                    file_info = publish_graph_node_file(node_id, conn=conn)
                    if not file_info.get("ok"):
                        # 落盘失败：回滚该条 insert（避免 DB 有、文件无）
                        raise RuntimeError(str(file_info.get("error") or "publish_graph_node_failed"))
                    label_map[label] = node_id
                    nodes_created += 1
                except Exception:
                    # 单条失败不影响后续
                    continue

        for edge in graph.get("edges", []) or []:
            if not isinstance(edge, dict):
                continue
            source = edge.get("source")
            target = edge.get("target")
            if isinstance(source, str):
                source = label_map.get(source)
            if isinstance(target, str):
                target = label_map.get(target)
            if not isinstance(source, int) or not isinstance(target, int):
                continue
            relation = edge.get("relation")
            if not relation:
                continue

            created_at = now_iso()
            with get_connection(db_path=db_path) as conn:
                try:
                    # 去重：避免重试/重复抽取导致相同边被插入多次
                    exists = conn.execute(
                        "SELECT id FROM graph_edges WHERE source = ? AND target = ? AND relation = ? ORDER BY id ASC LIMIT 1",
                        (int(source), int(target), str(relation)),
                    ).fetchone()
                    if exists:
                        continue
                    cursor = conn.execute(
                        "INSERT INTO graph_edges (source, target, relation, created_at, confidence, evidence) VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            int(source),
                            int(target),
                            relation,
                            created_at,
                            edge.get("confidence"),
                            edge.get("evidence") or f"auto:task:{task_id}:run:{run_id}",
                        ),
                    )
                    edge_id = int(cursor.lastrowid)
                    file_info = publish_graph_edge_file(edge_id, conn=conn)
                    if not file_info.get("ok"):
                        raise RuntimeError(str(file_info.get("error") or "publish_graph_edge_failed"))
                    edges_created += 1
                except Exception:
                    continue
    return {"nodes_created": nodes_created, "edges_created": edges_created}


def _enqueue_graph_extraction(task_id: int, run_id: int, content: str) -> int:
    db_path = resolve_db_path()
    created_at = now_iso()
    with get_connection(db_path=db_path) as conn:
        cursor = conn.execute(
            "INSERT INTO graph_extract_tasks (task_id, run_id, content, status, attempts, error, created_at, updated_at, finished_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                task_id,
                run_id,
                content,
                GRAPH_EXTRACT_STATUS_QUEUED,
                0,
                None,
                created_at,
                created_at,
                None,
            ),
        )
        extract_id = int(cursor.lastrowid)
    _start_graph_extractor()
    with _graph_extract_lock:
        _graph_extract_queue.append((db_path, extract_id, task_id, run_id, content))
    return extract_id


def enqueue_existing_graph_extract(extract_id: int) -> Optional[dict]:
    """
    将已存在的 graph_extract_tasks 重新入队（用于 /retry）。
    """
    db_path = resolve_db_path()
    with get_connection(db_path=db_path) as conn:
        row = conn.execute(
            "SELECT * FROM graph_extract_tasks WHERE id = ?",
            (int(extract_id),),
        ).fetchone()
        if not row:
            return None
        conn.execute(
            "UPDATE graph_extract_tasks SET status = ?, error = ?, updated_at = ? WHERE id = ?",
            (GRAPH_EXTRACT_STATUS_QUEUED, None, now_iso(), int(extract_id)),
        )
    _start_graph_extractor()
    with _graph_extract_lock:
        _graph_extract_queue.append(
            (
                db_path,
                int(row["id"]),
                int(row["task_id"]),
                int(row["run_id"]),
                str(row["content"] or ""),
            )
        )
    return {
        "id": row["id"],
        "task_id": row["task_id"],
        "run_id": row["run_id"],
        "status": GRAPH_EXTRACT_STATUS_QUEUED,
        "attempts": row["attempts"],
        "error": None,
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "finished_at": row["finished_at"],
    }


def _start_graph_extractor() -> None:
    global _graph_extract_thread_started, _graph_extract_db_path
    # 测试环境不启动常驻后台线程：
    # - pytest/unittest 常用 TemporaryDirectory 作为隔离 DB/Prompt Root；
    # - 常驻线程会跨用例持续轮询 DB，导致临时目录清理出现竞态（Directory not empty）。
    if is_test_env():
        return
    if _graph_extract_thread_started:
        current = resolve_db_path()
        if _graph_extract_db_path and _graph_extract_db_path != current:
            logger.warning(
                "graph_extract.db_path_changed: started=%s current=%s",
                _graph_extract_db_path,
                current,
            )
            # 仅告警一次，避免后续每次入队都刷屏；worker 会跟随 resolve_db_path() 使用当前库。
            _graph_extract_db_path = current
        return
    db_path = resolve_db_path()
    _graph_extract_db_path = db_path
    # 兜底：确保 schema 已初始化（worker 会轮询 graph_extract_tasks；未初始化会直接崩溃）
    try:
        from backend.src.storage import init_db

        init_db()
    except Exception as exc:
        # 初始化失败时 worker 仍会轮询；这里只做告警，避免启动阶段被阻塞。
        logger.exception("graph_extract.init_db_failed: %s", exc)
    _graph_extract_thread_started = True
    thread = threading.Thread(target=_graph_extract_worker, daemon=True)
    thread.start()


def _graph_extract_worker() -> None:
    """
    图谱抽取后台线程。

    设计说明：
    - 该 worker 以“队列优先 + DB 兜底轮询”的方式取任务：
      1) 优先消费进程内队列（低延迟）；
      2) 若队列为空，则从 graph_extract_tasks 里捞 queued 的任务（避免进程重启丢队列）。
    - worker 必须“永不崩溃”：单条任务失败只更新状态，不影响后续任务。
    - LLM 抽取失败会按 GRAPH_EXTRACT_MAX_ATTEMPTS 做重试（失败->queued，直到次数耗尽）。
    """
    while True:
        item = None
        with _graph_extract_lock:
            if _graph_extract_queue:
                item = _graph_extract_queue.popleft()

        if not item:
            try:
                db_path = resolve_db_path()
                with get_connection(db_path=db_path) as conn:
                    row = conn.execute(
                        "SELECT * FROM graph_extract_tasks WHERE status = ? ORDER BY id ASC LIMIT 1",
                        (GRAPH_EXTRACT_STATUS_QUEUED,),
                    ).fetchone()
                    if row:
                        item = (
                            db_path,
                            int(row["id"]),
                            int(row["task_id"]),
                            int(row["run_id"]),
                            str(row["content"] or ""),
                        )
            except sqlite3.OperationalError:
                # 数据库尚未初始化/旧库缺表时不应让 worker 线程崩溃
                time.sleep(0.5)
                continue
            if not item:
                time.sleep(0.5)
                continue

        db_path, extract_id, task_id, run_id, content = item
        try:
            _process_graph_extract_item(
                db_path=db_path,
                extract_id=int(extract_id),
                task_id=int(task_id),
                run_id=int(run_id),
                content=str(content or ""),
            )
        except sqlite3.OperationalError:
            # DB 被删除/切换路径/缺表时：不应让线程崩溃或刷屏；回退为 queued 并重试。
            try:
                with get_connection(db_path=db_path) as conn:
                    conn.execute(
                        "UPDATE graph_extract_tasks SET status = ?, updated_at = ? WHERE id = ? AND status = ?",
                        (GRAPH_EXTRACT_STATUS_QUEUED, now_iso(), int(extract_id), GRAPH_EXTRACT_STATUS_RUNNING),
                    )
            except Exception:
                pass
            with _graph_extract_lock:
                _graph_extract_queue.appendleft(item)
            time.sleep(0.5)
        except Exception as exc:
            # 防止线程因单条任务的意外异常直接崩溃（崩溃后队列会卡死直到进程重启）
            try:
                _mark_graph_extract_result(
                    int(extract_id),
                    GRAPH_EXTRACT_STATUS_FAILED,
                    f"worker_error:{exc}",
                    db_path=db_path,
                )
            except Exception as mark_exc:
                logger.exception("graph_extract.mark_failed: %s", mark_exc)


def _process_graph_extract_item(
    *,
    db_path: str,
    extract_id: int,
    task_id: int,
    run_id: int,
    content: str,
) -> None:
    """
    处理一条 graph_extract_tasks 任务（供 worker 与单测复用）。

    重要：
    - 必须使用 db_path 对应的数据库，避免进程内切换 AGENT_DB_PATH 时“跨库污染”。
    """
    with get_connection(db_path=db_path) as conn:
        conn.execute(
            "UPDATE graph_extract_tasks SET status = ?, attempts = attempts + 1, updated_at = ? WHERE id = ?",
            (GRAPH_EXTRACT_STATUS_RUNNING, now_iso(), int(extract_id)),
        )

    prompt = GRAPH_LLM_PROMPT_TEMPLATE.format(content=content)
    model = None
    try:
        model = resolve_default_model()
    except Exception:
        model = None

    response_text, _, error_message = call_openai(prompt, model, {"temperature": 0})
    if error_message or not response_text:
        ok = _mark_graph_extract_result(
            int(extract_id),
            GRAPH_EXTRACT_STATUS_FAILED,
            f"{error_message or 'empty_response'}",
            db_path=db_path,
        )
        if not ok:
            raise sqlite3.OperationalError("mark_graph_extract_result_failed")
        return

    response_payload = extract_json_object(response_text)
    if not response_payload:
        ok = _mark_graph_extract_result(
            int(extract_id),
            GRAPH_EXTRACT_STATUS_FAILED,
            "invalid_json",
            db_path=db_path,
        )
        if not ok:
            raise sqlite3.OperationalError("mark_graph_extract_result_failed")
        return

    graph_payload = _parse_graph_payload(response_payload)
    if not graph_payload:
        ok = _mark_graph_extract_result(
            int(extract_id),
            GRAPH_EXTRACT_STATUS_FAILED,
            "invalid_graph_payload",
            db_path=db_path,
        )
        if not ok:
            raise sqlite3.OperationalError("mark_graph_extract_result_failed")
        return

    _apply_graphs(int(task_id), int(run_id), [graph_payload], db_path=db_path)
    ok = _mark_graph_extract_result(
        int(extract_id), GRAPH_EXTRACT_STATUS_DONE, None, db_path=db_path
    )
    if not ok:
        raise sqlite3.OperationalError("mark_graph_extract_result_failed")


def _mark_graph_extract_result(
    extract_id: int, status: str, error: Optional[str], *, db_path: Optional[str]
) -> bool:
    finished_at = (
        now_iso()
        if status in {GRAPH_EXTRACT_STATUS_DONE, GRAPH_EXTRACT_STATUS_FAILED}
        else None
    )
    try:
        with get_connection(db_path=db_path) as conn:
            row = conn.execute(
                "SELECT attempts FROM graph_extract_tasks WHERE id = ?",
                (int(extract_id),),
            ).fetchone()
            attempts = row["attempts"] if row else 0
            if (
                status == GRAPH_EXTRACT_STATUS_FAILED
                and attempts < GRAPH_EXTRACT_MAX_ATTEMPTS
            ):
                status = GRAPH_EXTRACT_STATUS_QUEUED
                finished_at = None
            conn.execute(
                "UPDATE graph_extract_tasks SET status = ?, error = ?, updated_at = ?, finished_at = ? WHERE id = ?",
                (status, error, now_iso(), finished_at, int(extract_id)),
            )
        return True
    except sqlite3.OperationalError:
        # 缺表/DB 被清理时允许丢弃更新（避免 worker 进入“失败->标记失败->再次失败”的死循环刷屏）。
        return False
