from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import APP_HOST
from app.config import APP_NAME
from app.config import APP_PORT
from app.config import CORS_ORIGINS
from app.config import POSTGRES_URL
from app.db.storage import Storage
from app.db.utils import apply_migrations
from app.logging_config import setup_logging
from app.routes.api.routes import router
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
    app = FastAPI(title=APP_NAME, lifespan=lifespan, root_path='/api')
    app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*'],
    )
    app.include_router(router)
    return app


app = build_app()


def run() -> None:
    setup_logging()
    asyncio.run(apply_migrations(POSTGRES_URL))
    uvicorn.run(app, host=APP_HOST, port=APP_PORT)


if __name__ == '__main__':
    run()
