from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi import Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .api.channels import router as channels_router
from .api.other import router as other_router
from .api.prompts import router as prompts_router
from .api.users import router as users_router
from .config import API_ROOT_PATH
from .config import APP_TITLE
from .config import CORS_ORIGINS
from .deepseek import DeepSeek
from .exceptions import AppException
from .mediator import Mediator
from .monitoring import ChannelMonitoringService
from .storage import Storage
from .telegram import Telegram


@asynccontextmanager
async def lifespan(app: FastAPI):
    storage = Storage()
    telegram = Telegram()
    deepseek = DeepSeek()
    mediator = Mediator(telegram, deepseek, storage)
    monitoring_service = ChannelMonitoringService(
        telegram=telegram,
        deepseek=deepseek,
        storage=storage,
        mediator=mediator,
    )
    await storage.init()
    await telegram.init()
    await monitoring_service.init()
    app.state.storage = storage
    app.state.telegram = telegram
    app.state.deepseek = deepseek
    app.state.mediator = mediator
    app.state.monitoring_service = monitoring_service
    yield
    await monitoring_service.close()
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


@app.exception_handler(AppException)
async def app_exception_handler(_: Request, exc: AppException):
    return JSONResponse(
        status_code=exc.status_code,
        content={'detail': exc.detail},
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)
app.include_router(channels_router)
app.include_router(prompts_router)
app.include_router(users_router)
app.include_router(other_router)
