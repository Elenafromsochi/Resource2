from fastapi import FastAPI
from fastapi import Request
from fastapi.responses import JSONResponse

from .exceptions import AppException


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppException)
    async def app_exception_handler(_: Request, exc: AppException):
        return JSONResponse(
            status_code=exc.status_code,
            content={'detail': exc.detail},
        )
