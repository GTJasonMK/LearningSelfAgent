import asyncio
import json
import threading
import time
import traceback
from typing import AsyncGenerator, Generator, TypeVar

from backend.src.constants import AGENT_SSE_PLAN_MIN_INTERVAL_SECONDS

T = TypeVar("T")


def _normalize_interval_seconds(value: object) -> float:
    try:
        v = float(value)  # type: ignore[arg-type]
    except Exception:
        return 0.0
    return v if v > 0 else 0.0


def _sse_extract_data_line(msg: str) -> str:
    """
    从 SSE 文本中提取 data 行并拼接（兼容多行 data:）。
    - 返回空字符串表示不存在 data
    """
    if not isinstance(msg, str) or not msg:
        return ""
    data_lines: list[str] = []
    for line in msg.splitlines():
        if line.startswith("data:"):
            data_lines.append(line[len("data:") :].lstrip())
    return "\n".join(data_lines)


def _sse_json_type(msg: str) -> str:
    """
    解析 SSE 的 data JSON 并返回 obj.type（失败返回空字符串）。
    说明：仅用于 plan 事件节流判断，解析失败不影响主流程。
    """
    data_str = _sse_extract_data_line(msg)
    if not data_str:
        return ""
    try:
        obj = json.loads(data_str)
    except Exception:
        return ""
    if isinstance(obj, dict) and isinstance(obj.get("type"), str):
        return str(obj.get("type") or "")
    return ""


def _try_parse_sse_data_json(msg: str) -> dict | None:
    """
    尝试解析 SSE data JSON（失败返回 None）。
    说明：仅用于 plan/plan_delta 的节流与合并，解析失败不影响主流程。
    """
    data_str = _sse_extract_data_line(msg)
    if not data_str:
        return None
    try:
        obj = json.loads(data_str)
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _sse_data_json(obj: dict) -> str:
    """构造标准 SSE data 行（不带 event）。"""
    return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n"


async def pump_sync_generator(
    *,
    inner: Generator[str, None, T],
    label: str,
    poll_interval_seconds: int,
    idle_timeout_seconds: int,
) -> AsyncGenerator[tuple[str, object], None]:
    """
    将“同步 generator”的产出桥接为“异步事件流”。

    事件格式：(kind, payload)
    - ("msg", <str>)  : 需要继续向 SSE 输出的内容
    - ("done", <T>)   : 同步 generator 正常结束，payload 为 return 值
    - ("err", <exc>)  : 同步 generator 内部异常（payload 为异常对象）

    重要：这里使用 tick_task + asyncio.wait 定期唤醒 event loop。
    在 httpx.ASGITransport 的测试场景中，单纯 await queue.get() 可能出现“回调已投递但 loop
    没有及时唤醒”的卡死现象；tick 可以显著降低该类卡死概率。
    """
    q: "asyncio.Queue[tuple[str, object]]" = asyncio.Queue()
    loop = asyncio.get_running_loop()
    # 重要：当 SSE 客户端断开/外层 async generator 被 cancel 时，需要通知线程停止继续泵数据；
    # 否则同步 generator 可能持续 yield，导致队列无限增长（内存泄漏）或后台线程长期空转。
    stop_event = threading.Event()
    pump_error: dict[str, str] = {"label": str(label or "").strip() or "(empty)"}

    def _enqueue(kind: str, payload: object) -> None:
        try:
            q.put_nowait((kind, payload))
        except Exception as exc:
            pump_error["queue_put_error"] = f"{type(exc).__name__}: {exc}"
            pump_error["queue_put_trace"] = traceback.format_exc()

    def _try_put(kind: str, payload: object) -> bool:
        if stop_event.is_set():
            return False
        try:
            loop.call_soon_threadsafe(_enqueue, kind, payload)
            return True
        except Exception as exc:
            pump_error["call_soon_error"] = f"{type(exc).__name__}: {exc}"
            pump_error["call_soon_trace"] = traceback.format_exc()
            return False

    def _pump() -> None:
        sent = 0
        pump_error["phase"] = "start"
        try:
            while True:
                if stop_event.is_set():
                    pump_error["phase"] = "cancelled"
                    try:
                        inner.close()
                    except Exception as exc:
                        pump_error["close_error"] = f"{type(exc).__name__}: {exc}"
                        pump_error["close_trace"] = traceback.format_exc()
                    return
                pump_error["phase"] = "next"
                try:
                    item = next(inner)
                except StopIteration as exc:
                    pump_error["phase"] = "stop"
                    pump_error["sent_count"] = str(sent)
                    _try_put("done", exc.value)
                    return
                sent += 1
                pump_error["phase"] = "enqueue_msg"
                pump_error["sent_count"] = str(sent)
                if not _try_put("msg", item):
                    pump_error["phase"] = "cancelled"
                    try:
                        inner.close()
                    except Exception as exc:
                        pump_error["close_error"] = f"{type(exc).__name__}: {exc}"
                        pump_error["close_trace"] = traceback.format_exc()
                    return
        except BaseException as exc:  # noqa: BLE001
            pump_error["phase"] = "exception"
            pump_error["error"] = f"{type(exc).__name__}: {exc}"
            pump_error["trace"] = traceback.format_exc()
            _try_put("err", exc)
            return

    t = threading.Thread(target=_pump, daemon=True)
    t.start()

    last_recv_at = time.monotonic()
    # plan 事件节流：
    # - plan payload 通常包含完整 plan_items，大计划下频繁广播会导致前端渲染抖动；
    # - 在 pump 层统一合并短时间内密集的 plan 事件（只保留最后一次），降低 JSON 洪泛；
    # - need_input/done/error 等关键事件前会强制 flush，避免 UI 丢失最终状态。
    plan_min_interval = _normalize_interval_seconds(AGENT_SSE_PLAN_MIN_INTERVAL_SECONDS)
    last_plan_emit_at = 0.0
    pending_plan_msg: str | None = None
    last_plan_delta_emit_at = 0.0
    pending_plan_delta_meta: dict | None = None
    pending_plan_delta_changes: dict[int, dict] = {}

    def _flush_plan_delta() -> str | None:
        nonlocal pending_plan_delta_meta
        nonlocal pending_plan_delta_changes
        if not pending_plan_delta_meta or not pending_plan_delta_changes:
            pending_plan_delta_meta = None
            pending_plan_delta_changes = {}
            return None
        meta = dict(pending_plan_delta_meta)
        # 统一输出：按 step_order 排序，便于前端/调试稳定
        changes = list(pending_plan_delta_changes.values())
        try:
            changes = sorted(changes, key=lambda it: int(it.get("step_order") or 0))
        except Exception:
            pass
        meta["changes"] = changes
        pending_plan_delta_meta = None
        pending_plan_delta_changes = {}
        return _sse_data_json(meta)

    get_task = asyncio.create_task(q.get())
    tick_task = asyncio.create_task(asyncio.sleep(poll_interval_seconds))
    try:
        while True:
            done, pending = await asyncio.wait(
                {get_task, tick_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            if get_task in done:
                kind, payload = get_task.result()
                last_recv_at = time.monotonic()
                get_task = asyncio.create_task(q.get())

                if tick_task in pending:
                    tick_task.cancel()
                    await asyncio.gather(tick_task, return_exceptions=True)
                tick_task = asyncio.create_task(asyncio.sleep(poll_interval_seconds))

                if kind in {"done", "err"}:
                    # 同步 generator 结束前尽量把 pending plan 刷出去，避免“最后状态”被节流吞掉。
                    if plan_min_interval > 0:
                        delta_msg = _flush_plan_delta()
                        if delta_msg:
                            yield "msg", str(delta_msg)
                    if pending_plan_msg:
                        yield "msg", str(pending_plan_msg)
                        pending_plan_msg = None
                    yield kind, payload
                    return

                if kind == "msg":
                    msg_text = str(payload or "")

                    if plan_min_interval <= 0:
                        yield "msg", msg_text
                        continue

                    msg_type = _sse_json_type(msg_text)
                    if msg_type == "plan":
                        now_value = time.monotonic()
                        if (now_value - last_plan_emit_at) >= plan_min_interval:
                            pending_plan_msg = None
                            last_plan_emit_at = now_value
                            yield "msg", msg_text
                        else:
                            # 短时间内的 plan 更新只保留最后一次
                            pending_plan_msg = msg_text
                        continue

                    if msg_type == "plan_delta":
                        now_value = time.monotonic()
                        obj = _try_parse_sse_data_json(msg_text)
                        changes = obj.get("changes") if isinstance(obj, dict) else None
                        if not isinstance(changes, list) or not changes:
                            yield "msg", msg_text
                            continue

                        # 若 meta 不一致（不同 task/run），先 flush 再开始新一组，避免跨 run 混合
                        meta = {
                            "type": "plan_delta",
                            "task_id": obj.get("task_id"),
                        }
                        if obj.get("run_id") is not None:
                            meta["run_id"] = obj.get("run_id")
                        if pending_plan_delta_meta and pending_plan_delta_meta != meta:
                            delta_msg = _flush_plan_delta()
                            if delta_msg:
                                yield "msg", str(delta_msg)
                                last_plan_delta_emit_at = now_value

                        pending_plan_delta_meta = meta
                        for ch in changes:
                            if not isinstance(ch, dict):
                                continue
                            key = None
                            try:
                                cid = int(ch.get("id") or 0)
                            except Exception:
                                cid = 0
                            if cid > 0:
                                key = cid
                            else:
                                try:
                                    order = int(ch.get("step_order") or 0)
                                except Exception:
                                    order = 0
                                if order > 0:
                                    key = order
                            if key is None:
                                continue
                            pending_plan_delta_changes[int(key)] = dict(ch)

                        # 节流：短时间内的 plan_delta 合并输出（按 step 合并最新状态）
                        if (now_value - last_plan_delta_emit_at) >= plan_min_interval:
                            delta_msg = _flush_plan_delta()
                            if delta_msg:
                                last_plan_delta_emit_at = now_value
                                yield "msg", str(delta_msg)
                                continue
                        continue

                    # need_input/done/error 等关键事件前强制 flush plan
                    if (
                        msg_type in {"need_input", "done", "review"}
                        or "event: error" in msg_text
                        or "event: done" in msg_text
                    ):
                        if plan_min_interval > 0:
                            delta_msg = _flush_plan_delta()
                            if delta_msg:
                                yield "msg", str(delta_msg)
                                last_plan_delta_emit_at = time.monotonic()
                        if pending_plan_msg:
                            yield "msg", str(pending_plan_msg)
                            pending_plan_msg = None
                            last_plan_emit_at = time.monotonic()
                    yield "msg", msg_text
                    continue

                yield kind, payload
                continue

            # tick：检查线程/回传异常，并让 loop 周期性 wake up
            tick_task = asyncio.create_task(asyncio.sleep(poll_interval_seconds))

            # tick flush：避免 plan 在“短 burst 后长时间无后续 plan”时被永久吞掉
            if plan_min_interval > 0 and pending_plan_msg:
                now_value = time.monotonic()
                if (now_value - last_plan_emit_at) >= plan_min_interval:
                    yield "msg", str(pending_plan_msg)
                    pending_plan_msg = None
                    last_plan_emit_at = now_value
                    continue

            # tick flush：同理，避免 plan_delta 在 burst 后被永久合并不输出
            if plan_min_interval > 0 and pending_plan_delta_meta and pending_plan_delta_changes:
                now_value = time.monotonic()
                if (now_value - last_plan_delta_emit_at) >= plan_min_interval:
                    delta_msg = _flush_plan_delta()
                    if delta_msg:
                        yield "msg", str(delta_msg)
                        last_plan_delta_emit_at = now_value
                        continue

            if (
                pump_error.get("queue_put_error")
                or pump_error.get("call_soon_error")
                or pump_error.get("error")
            ):
                detail = (
                    pump_error.get("error")
                    or pump_error.get("queue_put_error")
                    or pump_error.get("call_soon_error")
                    or "pump error"
                )
                phase = pump_error.get("phase")
                sent_count = pump_error.get("sent_count")
                suffix = f" (label={pump_error.get('label')} phase={phase} sent={sent_count})"
                raise RuntimeError(f"stream pump 回传失败: {detail}{suffix}")  # noqa: TRY301

            if not t.is_alive():
                phase = pump_error.get("phase")
                sent_count = pump_error.get("sent_count")
                suffix = f" (label={pump_error.get('label')} phase={phase} sent={sent_count})"
                raise RuntimeError(f"stream pump 线程异常退出{suffix}")  # noqa: TRY301

            if time.monotonic() - last_recv_at > idle_timeout_seconds:
                suffix = f" (label={pump_error.get('label')})"
                raise RuntimeError(f"stream pump 长时间无输出，判定卡死{suffix}")  # noqa: TRY301
    finally:
        stop_event.set()
        get_task.cancel()
        tick_task.cancel()
        await asyncio.gather(get_task, tick_task, return_exceptions=True)
