from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import API_ROOT_PATH
from app.config import APP_NAME
from app.config import CORS_ORIGINS
from app.db.storage import Storage
from app.routes.api import router as api_router
from app.services.deepseek import DeepSeek
from app.services.mediator import Mediator
from app.services.telegram import Telegram


@asynccontextmanager
async def lifespan(app: FastAPI):
    storage = Storage()
    await storage.init()
    telegram = Telegram()
    await telegram.init()
    deepseek = DeepSeek()
    mediator = Mediator(telegram, deepseek, storage)
    app.state.storage = storage
    app.state.telegram = telegram
    app.state.deepseek = deepseek
    app.state.mediator = mediator
    yield
    await telegram.close()
    await storage.close()


def build_app() -> FastAPI:
    app = FastAPI(title=APP_NAME, lifespan=lifespan, root_path=API_ROOT_PATH)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*'],
    )
    app.include_router(api_router)
    return app


app = build_app()
