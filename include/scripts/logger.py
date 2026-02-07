import logging
from pathlib import Path

from launch_sentiment_analysis.include.scripts import config


LOG_FILE = config.LOG_FILE_DIR

def get_logger(name: str) -> logging.Logger:
    """Create and return a configured logger.

    Args:
        name (str): Logger name.

    Returns:
        logging.Logger: Configured logger instance.
    """
    # Ensure the directory for the log file exists before initializing the handler
    Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        file_handler = logging.FileHandler(LOG_FILE)

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(message)s"
        )

        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
