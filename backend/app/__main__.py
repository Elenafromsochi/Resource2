from __future__ import annotations

import asyncio
import logging
import sys

import colorlog
import uvicorn

from app.app import app
from app.config import APP
from app.config import LOG_LEVEL
from app.config import POSTGRES_URL
from app.db.utils import migrate


handler = colorlog.StreamHandler(sys.stdout)
handler.setFormatter(
    colorlog.ColoredFormatter(
        '%(log_color)s%(levelname)s%(reset)s '
        '%(asctime)s %(name)s: %(message)s',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'bold_red',
        },
    ),
)
logging.basicConfig(level=LOG_LEVEL, handlers=[handler])


def run() -> None:
    asyncio.run(migrate(POSTGRES_URL))
    uvicorn.run(app, **APP)


if __name__ == '__main__':
    run()
