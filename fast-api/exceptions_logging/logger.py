####################################################################
# logger.py
#
# Returns a logger that writes to <LEVEL>.log (INFO.log, WARNING.log, ERROR.log)
####################################################################

import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Union


LOG_DIR = "/shared/logs"
os.makedirs(LOG_DIR, exist_ok=True)

def get_logger(level: Union[int, str] = logging.INFO):
    """
    Returns a logger that writes to <LEVEL>.log (INFO.log,
    WARNING.log, ERROR.log).
    with rotating file handler
    """
    if isinstance(level, str):
        level = logging.getLevelName(level.upper())

    level_name = logging.getLevelName(level)
    log_file = os.path.join(LOG_DIR, f"{level_name}.log")

    logger = logging.getLogger(level_name)
    logger.setLevel(level)

    if not logger.handlers:
        handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 mb
            backupCount=5,  # keep last 5 files
            encoding="utf-8",
        )
        handler.setLevel(level)

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%H:%M:%S",
        )
        handler.setFormatter(formatter)

        logger.addHandler(handler)
        logger.propagate = False

    return logger


# define all loggers
info_logger = get_logger(logging.INFO)
warn_logger = get_logger(logging.WARNING)
error_logger = get_logger(logging.ERROR)