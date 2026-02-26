from __future__ import annotations

import asyncio
import os
import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional

from backend.src.agent.contracts.stream_events import coerce_session_key
from backend.src.common.utils import coerce_int

_STATE_LOCK = threading.Lock()
_LANE_QUEUES: Dict[str, asyncio.Queue] = {}
_LANE_REF_COUNTS: Dict[str, int] = {}
_GLOBAL_QUEUE: Optional[asyncio.Queue] = None
_GLOBAL_LIMIT_CACHE: Optional[int] = None


def _resolve_global_limit() -> int:
    raw = os.getenv("AGENT_STREAM_GLOBAL_CONCURRENCY", "3")
    value = coerce_int(raw, default=3)
    return max(1, value)


def _build_token_queue(limit: int) -> asyncio.Queue:
    size = max(1, coerce_int(limit, default=1))
    q: asyncio.Queue = asyncio.Queue(maxsize=size)
    for _ in range(size):
        q.put_nowait(object())
    return q


async def _get_global_queue() -> asyncio.Queue:
    global _GLOBAL_QUEUE, _GLOBAL_LIMIT_CACHE
    limit = _resolve_global_limit()
    with _STATE_LOCK:
        # 限流配置变化时重建队列（测试/调参生效）。
        if _GLOBAL_QUEUE is None or _GLOBAL_LIMIT_CACHE != limit:
            _GLOBAL_QUEUE = _build_token_queue(limit)
            _GLOBAL_LIMIT_CACHE = limit
        return _GLOBAL_QUEUE


def _normalize_lane_key(session_key: str) -> str:
    normalized = coerce_session_key(session_key)
    if normalized:
        return normalized
    return "sess_unknown"


def _get_or_create_lane_queue(lane_key: str) -> asyncio.Queue:
    lane = _LANE_QUEUES.get(lane_key)
    if lane is not None:
        return lane
    lane = _build_token_queue(1)
    _LANE_QUEUES[lane_key] = lane
    return lane


def _decrement_lane_ref_unlocked(*, lane_key: str, lane_queue: asyncio.Queue) -> None:
    current_ref = coerce_int(_LANE_REF_COUNTS.get(lane_key), default=0)
    next_ref = max(0, current_ref - 1)
    if next_ref <= 0:
        _LANE_REF_COUNTS.pop(lane_key, None)
        cached_lane = _LANE_QUEUES.get(lane_key)
        if cached_lane is lane_queue and cached_lane.qsize() >= 1:
            _LANE_QUEUES.pop(lane_key, None)
        return
    _LANE_REF_COUNTS[lane_key] = next_ref


async def _return_token(queue_obj: asyncio.Queue, token: object) -> None:
    try:
        queue_obj.put_nowait(token)
    except asyncio.QueueFull:
        # 并发/重复释放时忽略（release 保持幂等）。
        return


@dataclass
class StreamQueueTicket:
    lane_key: str
    lane_queue: asyncio.Queue
    lane_token: object
    global_queue: asyncio.Queue
    global_token: object
    acquired_at: float
    _released: bool = False

    async def release(self) -> None:
        if self._released:
            return
        self._released = True
        try:
            await _return_token(self.lane_queue, self.lane_token)
        finally:
            await _return_token(self.global_queue, self.global_token)

        with _STATE_LOCK:
            _decrement_lane_ref_unlocked(lane_key=self.lane_key, lane_queue=self.lane_queue)

    def is_hold_timeout(self, max_hold_seconds: float) -> bool:
        try:
            limit = float(max_hold_seconds)
        except Exception:
            return False
        if limit <= 0:
            return False
        return (time.monotonic() - float(self.acquired_at)) > limit


async def acquire_stream_queue_ticket(
    *,
    session_key: str,
    timeout_seconds: float = 120.0,
) -> StreamQueueTicket:
    lane_key = _normalize_lane_key(session_key)
    timeout = max(0.01, float(timeout_seconds))
    global_queue = await _get_global_queue()
    global_token = await asyncio.wait_for(global_queue.get(), timeout=timeout)

    lane_queue: Optional[asyncio.Queue] = None
    lane_token: Optional[object] = None
    try:
        with _STATE_LOCK:
            lane_queue = _get_or_create_lane_queue(lane_key)
            _LANE_REF_COUNTS[lane_key] = coerce_int(_LANE_REF_COUNTS.get(lane_key), default=0) + 1

        lane_token = await asyncio.wait_for(lane_queue.get(), timeout=timeout)
        return StreamQueueTicket(
            lane_key=lane_key,
            lane_queue=lane_queue,
            lane_token=lane_token,
            global_queue=global_queue,
            global_token=global_token,
            acquired_at=time.monotonic(),
        )
    except Exception:
        if lane_queue is not None:
            with _STATE_LOCK:
                _decrement_lane_ref_unlocked(lane_key=lane_key, lane_queue=lane_queue)
        if lane_queue is not None and lane_token is not None:
            await _return_token(lane_queue, lane_token)
        await _return_token(global_queue, global_token)
        raise


def get_stream_queue_snapshot() -> dict:
    with _STATE_LOCK:
        lanes = {
            key: {
                "ref_count": coerce_int(_LANE_REF_COUNTS.get(key), default=0),
                "qsize": coerce_int(queue_obj.qsize(), default=0),
                "maxsize": coerce_int(queue_obj.maxsize, default=0),
            }
            for key, queue_obj in _LANE_QUEUES.items()
        }
        global_qsize = coerce_int(_GLOBAL_QUEUE.qsize(), default=0) if _GLOBAL_QUEUE is not None else 0
        global_maxsize = coerce_int(_GLOBAL_QUEUE.maxsize, default=0) if _GLOBAL_QUEUE is not None else 0
        return {
            "global": {
                "limit": coerce_int(_GLOBAL_LIMIT_CACHE or _resolve_global_limit(), default=1),
                "qsize": global_qsize,
                "maxsize": global_maxsize,
            },
            "lanes": lanes,
        }


async def reset_stream_queue_state_for_tests() -> None:
    global _GLOBAL_QUEUE, _GLOBAL_LIMIT_CACHE
    with _STATE_LOCK:
        _LANE_QUEUES.clear()
        _LANE_REF_COUNTS.clear()
        _GLOBAL_QUEUE = None
        _GLOBAL_LIMIT_CACHE = None
