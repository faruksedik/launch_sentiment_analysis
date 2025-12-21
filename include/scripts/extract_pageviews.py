import gzip
import shutil
from pathlib import Path
from launch_sentiment.include.scripts.logger import get_logger

logger = get_logger(__name__)

def extract_gzip(input_path: str, output_path: str) -> str:
    """Extract gzip file into plain text.

    Args:
        input_path (str): Path to gzip file.
        output_path (str): Path to extracted file.

    Returns:
        str: Path to extracted file.
    """
    logger.info("Extracting file %s", input_path)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with gzip.open(input_path, "rb") as f_in:
        with open(output_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    logger.info("Extraction completed: %s", output_path)
    return output_path
