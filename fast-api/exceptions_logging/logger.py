####################################################################
# logger.py
#
# Returns a logger that writes to <LEVEL>.log (INFO.log, WARNING.log, ERROR.log)
####################################################################

import logging
import os
from typing import Union


LOG_DIR = "/shared/logs"
os.makedirs(LOG_DIR, exist_ok=True)

def get_logger(level: Union[int, str] = logging.INFO):
    """
    Returns a logger that writes to <LEVEL>.log (INFO.log,
    WARNING.log, ERROR.log)
    """
    # Check if level is 'str'
    if isinstance(level, str):
        level = logging.getLevelName(level.upper())

    level_name = logging.getLevelName(level)
    log_file = os.path.join(LOG_DIR, f"{level_name}.log")

    logger = logging.getLogger(level_name)
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.FileHandler(log_file)
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