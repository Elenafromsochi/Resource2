import logging

import uvicorn
from colorlog import ColoredFormatter

from .app import app


handler = logging.StreamHandler()
handler.setFormatter(ColoredFormatter(
    '%(log_color)s%(asctime)s.%(msecs)03d [%(levelname).1s] (%(name)s.%(funcName)s:%(lineno)d): %(message)s',
    log_colors={
        'DEBUG': 'light_blue',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
    },
    datefmt='%Y-%m-%d %H:%M:%S',
))
logging.basicConfig(level=logging.DEBUG, handlers=[handler])
logging.getLogger('telethon').setLevel(logging.INFO)
logging.getLogger('httpcore').setLevel(logging.INFO)

uvicorn.run(app, host='0.0.0.0', port=8000)
