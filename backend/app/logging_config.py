import logging
import sys

import colorlog

from app.config import LOG_LEVEL


def setup_logging() -> None:
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
