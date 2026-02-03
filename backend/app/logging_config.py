import logging
import sys

import colorlog

from app import config


def setup_logging():
    handler = colorlog.StreamHandler(sys.stdout)
    handler.setFormatter(
        colorlog.ColoredFormatter(
            "%(log_color)s%(levelname)s%(reset)s "
            "%(asctime)s %(name)s: %(message)s",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red",
            },
        ),
    )
    logging.basicConfig(level=config.LOG_LEVEL, handlers=[handler])
