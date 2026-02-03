from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.channels import router as channels_router
from .api.other import router as other_router
from .api.users import router as users_router
from .config import API_ROOT_PATH
from .config import APP_TITLE
from .config import CORS_ORIGINS
from .deepseek import DeepSeek
from .exception_handlers import register_exception_handlers
from .mediator import Mediator
from .storage import Storage
from .telegram import Telegram


@asynccontextmanager
async def lifespan(app: FastAPI):
    storage = Storage()
    telegram = Telegram()
    deepseek = DeepSeek()
    mediator = Mediator(telegram, deepseek, storage)
    await storage.init()
    await telegram.init()
    app.state.storage = storage
    app.state.telegram = telegram
    app.state.deepseek = deepseek
    app.state.mediator = mediator
    yield
    await telegram.close()
    await storage.close()


app = FastAPI(
    title=APP_TITLE,
    root_path=API_ROOT_PATH,
    docs_url='/docs',
    redoc_url='/redoc',
    openapi_url='/openapi.json',
    lifespan=lifespan,
)
register_exception_handlers(app)
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)
app.include_router(channels_router)
app.include_router(users_router)
app.include_router(other_router)
