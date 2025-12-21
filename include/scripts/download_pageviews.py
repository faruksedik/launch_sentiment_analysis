import requests
from pathlib import Path

from launch_sentiment_analysis.include.scripts.logger import get_logger

logger = get_logger(__name__)


def download_pageviews(url: str, output_path: str) -> str:
    """
    Download a Wikimedia Wikipedia pageviews gzip file and save it locally.

    This function handles directory creation, streaming downloads for
    memory efficiency, and robust error handling for network and I/O failures.

    Args:
        url (str): Wikimedia pageviews URL.
        output_path (str): Local file path where the file will be saved.

    Returns:
        str: Path to the successfully downloaded file.

    Raises:
        Exception: Re-raises any exception after logging for Airflow visibility.
    """
    try:
        # Log the start of the download process
        logger.info(f"Starting download from URL: {url}")

        # Ensure the destination directory exists
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        # Perform a streaming HTTP GET request to avoid loading large files into memory
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        # Write the response content to disk in chunks
        with open(output_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)

        # Log successful completion
        logger.info(f"Download completed successfully: {output_path}")
        return output_path

    except requests.exceptions.RequestException as exc:
        # Handle network-related errors (timeouts, DNS issues, 4xx/5xx responses)
        logger.error(f"HTTP error occurred while downloading {url}: {exc}")
        raise

    except OSError as exc:
        # Handle file system errors (permission issues, disk space, etc.)
        logger.error(f"File system error while writing to {output_path}: {exc}")
        raise

    except Exception as exc:
        # Catch-all for any unexpected failures
        logger.exception(
            f"Unexpected error during pageviews download "
            f"(url={url}, output_path={output_path})"
        )
        raise
