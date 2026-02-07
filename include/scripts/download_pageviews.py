from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import requests
from pathlib import Path

from launch_sentiment_analysis.include.scripts.logger import get_logger
from launch_sentiment_analysis.include.scripts import config


logger = get_logger("download_pageviews")


def get_adjusted_execution_time() -> datetime:
    """
    Get the current runtime datetime (UTC) and subtract 2 months.

    This is used to avoid downloading incomplete Wikimedia data,
    since pageviews data is published with a delay.

    This uses the actual wall-clock time at task execution.

    Returns:
        datetime: Adjusted runtime execution time (2 months earlier).
    """
    runtime_time = datetime.now(timezone.utc)
    adjusted_execution_time = runtime_time - relativedelta(months=2)
    return adjusted_execution_time


def generate_pageviews_url(adjusted_execution_time: datetime) -> str:
    """
    Generate a Wikimedia pageviews download URL from a datetime object.

    Args:
        adjusted_execution_time (datetime): Adjusted execution datetime.

    Returns:
        str: Fully qualified Wikimedia pageviews download URL.
    """
    base_url = config.BASE_URL
    year = adjusted_execution_time.strftime("%Y")
    year_month = adjusted_execution_time.strftime("%Y-%m")
    date_hour = adjusted_execution_time.strftime("%Y%m%d-%H")

    return (
        f"{base_url}"
        f"{year}/{year_month}/"
        f"pageviews-{date_hour}0000.gz"
    )


def generate_output_path(output_dir: str, adjusted_execution_time: datetime) -> Path:
    """
    Generate a dynamic output file path based on execution time.

    Args:
        output_dir (str): Base directory for raw .gz files.
        adjusted_execution_time (datetime): Adjusted execution datetime.

    Returns:
        Path: Full path to the output .gz file.
    """
    date_hour = adjusted_execution_time.strftime("%Y%m%d-%H")
    filename = f"pageviews_{date_hour}.gz"

    return Path(output_dir) / filename



def download_gz_file(url: str, output_path: Path) -> str:
    """
    Download a gzip file from a URL and save it locally.

    Args:
        url (str): Download URL.
        output_path (Path): Path where the file will be saved.

    Returns:
        str: Path to the downloaded file.
    """
    logger.info("Starting download from URL: %s", url)

    # Ensure destination directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    # Write the response content to disk in chunks
    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    logger.info("Download completed successfully: %s", output_path)
    return str(output_path)


def download_pageviews(output_dir: str) -> str:
    """
    Orchestrate Wikimedia pageviews download.

    Steps:
    1. Get Airflow runtime timestamp and subtract 2 months
    2. Generate download URL
    3. Generate dynamic output file path
    4. Download the gzip file

    Args:
        output_dir (str): Base directory where raw files are stored.

    Returns:
        str: Path to the downloaded gzip file.
    """
    try:
        # Adjust execution time
        adjusted_execution_time = get_adjusted_execution_time()

        # Generate URL
        url = generate_pageviews_url(adjusted_execution_time)
        logger.info("Generated download URL: %s", url)

        # Generate output path
        output_path = generate_output_path(output_dir, adjusted_execution_time)

        # Download file
        return download_gz_file(url, output_path)

    except Exception:
        logger.exception("Failed to download Wikimedia pageviews data")
        raise