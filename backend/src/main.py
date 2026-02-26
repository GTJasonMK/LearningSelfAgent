import logging
import os
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from backend.src.api.routes import router as api_router
from backend.src.common.app_error_utils import app_error_response
from backend.src.common.errors import AppError
from backend.src.constants import APP_TITLE
from backend.src.storage import init_db
from backend.src.services.tasks.task_recovery import stop_running_task_records

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    @asynccontextmanager
    async def _lifespan(app: FastAPI):
        _ = app
        # 统一工作目录：大量 file_* / tool_exec 默认以 os.getcwd() 为基准。
        # 若用户以非项目根目录启动 uvicorn，会导致路径漂移（写错位置/找不到文件）。
        try:
            from backend.src.prompt.paths import repo_root

            root = str(repo_root())
            if root and os.path.isdir(root) and os.getcwd() != root:
                os.chdir(root)
        except Exception as exc:
            logger.exception("chdir repo_root failed: %s", exc)
        # 初始化数据库（放在 lifespan：避免 import 即产生副作用，且兼容 FastAPI 未来版本）
        init_db()

        # 启动时同步本地 skills 文件到数据库，保证 Agent 能立即检索到最新技能库（失败不阻塞启动）。
        try:
            from backend.src.services.skills.skills_sync import sync_skills_from_files

            sync_skills_from_files()
        except Exception as exc:
            logger.exception("sync_skills_from_files failed: %s", exc)

        # 启动时同步本地 memory 文件到数据库，并补齐 uid/落盘（失败不阻塞启动）。
        try:
            from backend.src.services.memory.memory_store import sync_memory_from_files

            sync_memory_from_files(prune=True)
        except Exception as exc:
            logger.exception("sync_memory_from_files failed: %s", exc)

        # 启动时同步本地 tools 文件到数据库（失败不阻塞启动）。
        try:
            from backend.src.services.tools.tools_store import sync_tools_from_files

            sync_tools_from_files(prune=True)
        except Exception as exc:
            logger.exception("sync_tools_from_files failed: %s", exc)

        # 启动时同步本地 graph 文件到数据库（失败不阻塞启动）。
        try:
            from backend.src.services.graph.graph_store import sync_graph_from_files

            sync_graph_from_files(prune=True)
        except Exception as exc:
            logger.exception("sync_graph_from_files failed: %s", exc)

        # 启动时兜底修复：将上一次异常退出遗留的 running 任务标记为 stopped，避免 UI 永久卡在“执行中”。
        try:
            stop_running_task_records(reason="startup")
        except Exception as exc:
            logger.exception("stop_running_task_records(startup) failed: %s", exc)

        # 启动兜底：补齐最近的“已完成但缺评估”的 Agent runs（后台线程，避免阻塞启动）。
        try:
            from backend.src.services.tasks.task_postprocess import (
                backfill_missing_agent_reviews,
                backfill_waiting_feedback_agent_reviews,
            )

            def _backfill_agent_reviews() -> None:
                try:
                    result = backfill_missing_agent_reviews(limit=10)
                    logger.info("backfill_missing_agent_reviews: %s", result)
                    result2 = backfill_waiting_feedback_agent_reviews(limit=10)
                    logger.info("backfill_waiting_feedback_agent_reviews: %s", result2)
                except Exception as exc:
                    logger.exception("backfill_missing_agent_reviews failed: %s", exc)

            threading.Thread(target=_backfill_agent_reviews, daemon=True).start()
        except Exception as exc:
            logger.exception("start backfill_missing_agent_reviews thread failed: %s", exc)

        yield

        # 尽量在正常退出时落库（例如 Electron 先发 stop-running 再 kill、或 uvicorn 收到 SIGTERM）。
        try:
            stop_running_task_records(reason="shutdown")
        except Exception as exc:
            logger.exception("stop_running_task_records(shutdown) failed: %s", exc)

    app = FastAPI(title=APP_TITLE, lifespan=_lifespan)

    @app.exception_handler(AppError)
    async def _handle_app_error(request: Request, exc: AppError):
        # 统一服务层异常协议：services 层只 raise AppError；API 层/全局 handler 转为 HTTP JSONResponse。
        return app_error_response(exc)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # For development convenience
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(api_router, prefix="/api")
    return app


app = create_app()
