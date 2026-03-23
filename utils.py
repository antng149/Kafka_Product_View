import logging
from logging.handlers import RotatingFileHandler


def setup_logger(log_file: str) -> logging.Logger:
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    file_handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=3)
    file_handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO, handlers=[console_handler, file_handler])
    return logging.getLogger(__name__)
