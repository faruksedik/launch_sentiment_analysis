import logging
from pathlib import Path

LOG_FILE = "/opt/airflow/dags/launch_sentiment_analysis/include/logs/pipeline.log"

def get_logger(name: str) -> logging.Logger:
    """Create and return a configured logger.

    Args:
        name (str): Logger name.

    Returns:
        logging.Logger: Configured logger instance.
    """
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
