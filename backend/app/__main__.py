from __future__ import annotations

import asyncio

import uvicorn

from app.app import app
from app.config import APP
from app.config import POSTGRES_URL
from app.db.utils import apply_migrations
from app.logging_config import setup_logging


def run() -> None:
    setup_logging()
    asyncio.run(apply_migrations(POSTGRES_URL))
    uvicorn.run(app, **APP)


if __name__ == '__main__':
    run()
