from __future__ import annotations

from typing import Awaitable, Callable, Tuple, Type


async def ensure_failed_task_output_shared(
    *,
    task_id: int,
    run_id: int,
    run_status: str,
    run_status_failed: str,
    yield_func: Callable,
    has_text_task_output_func: Callable[..., bool],
    build_failed_task_output_content_func: Callable[..., str],
    create_task_output_func: Callable,
    task_output_type_text: str,
    safe_write_debug_func: Callable,
    to_thread_func: Callable[..., Awaitable],
    sse_json_func: Callable[..., str],
    inject_delta: str = "【失败总结】已写入结构化失败报告。\n",
    handled_errors: Tuple[Type[BaseException], ...] = (Exception,),
) -> None:
    if str(run_status) != str(run_status_failed):
        return

    if has_text_task_output_func(task_id=int(task_id), run_id=int(run_id)):
        return

    content = build_failed_task_output_content_func(task_id=int(task_id), run_id=int(run_id))

    try:
        await to_thread_func(
            create_task_output_func,
            task_id=int(task_id),
            run_id=int(run_id),
            output_type=str(task_output_type_text),
            content=content,
        )
        safe_write_debug_func(
            task_id,
            run_id,
            message="agent.failed_output.injected",
            data={"bytes": len(content)},
            level="info",
        )
        try:
            yield_func(sse_json_func({"delta": str(inject_delta)}))
        except BaseException:
            pass
    except handled_errors as exc:
        safe_write_debug_func(
            task_id,
            run_id,
            message="agent.failed_output.inject_failed",
            data={"error": str(exc)},
            level="warning",
        )
