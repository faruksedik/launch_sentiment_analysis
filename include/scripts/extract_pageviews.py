import gzip
import shutil
from pathlib import Path

from launch_sentiment_analysis.include.scripts.logger import get_logger

logger = get_logger("extract_pageviews")


def normalize_input_path(input_path: str) -> Path:
    """
    Convert input string to Path and validate that it is
    an existing gzip (.gz) file.

    This function acts as a guard clause to prevent
    downstream extraction errors.

    Args:
        input_path (str): Path to the gzip file.

    Returns:
        Path: Validated Path object pointing to a .gz file.

    Raises:
        FileNotFoundError: If the input file does not exist.
        ValueError: If the file is not a .gz file.
    """
    logger.debug("Normalizing input path: %s", input_path)

    path = Path(input_path)

    if not path.exists():
        logger.error("Input file does not exist: %s", path)
        raise FileNotFoundError(f"Input file does not exist: {path}")

    if path.suffix != ".gz":
        logger.error("Invalid file type (expected .gz): %s", path)
        raise ValueError(f"Expected a .gz file, got: {path}")

    logger.info("Validated gzip input file: %s", path)
    return path




def generate_extracted_output_path(input_path: Path, 
                                   output_dir: str | None = None
                                ) -> Path:
    """
    Generate the output path for the extracted text file.

    If an output directory is provided, the extracted file
    is written there. Otherwise, it is written alongside
    the input gzip file.

    Args:
        input_path (Path): Path to the gzip file.
        output_dir (str | None): Optional directory for extracted files.

    Returns:
        Path: Full path to the extracted .txt file.
    """
    # Replace .gz with .txt while preserving filename
    base_name = input_path.with_suffix("").name + ".txt"

    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = output_dir / base_name
        logger.info(
            "Using custom output directory: %s → %s",
            output_dir,
            output_path
        )
        return output_path

    output_path = input_path.parent / base_name
    logger.info(
        "Using input file directory for extraction: %s",
        output_path
    )
    return output_path




def extract_gz_to_txt(input_path: Path, output_path: Path) -> str:
    """
    Extract a gzip-compressed file into a plain text file.

    This function performs a streaming copy to avoid
    loading the entire file into memory.

    Args:
        input_path (Path): Path to the gzip file.
        output_path (Path): Path where extracted text file will be written.

    Returns:
        str: Path to the extracted file as a string.
    """
    logger.info("Starting extraction: %s → %s", input_path, output_path)

    with gzip.open(input_path, "rb") as f_in:
        with open(output_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    logger.info("Extraction completed successfully: %s", output_path)
    return str(output_path)



def extract_pageviews(input_path: str, output_dir: str | None = None) -> str:
    """
    Orchestrate the extraction of a Wikimedia pageviews gzip file.

    Workflow:
    1. Validate input gzip path
    2. Generate output .txt path
    3. Extract gzip contents

    This function is designed to be called directly
    from an Airflow PythonOperator.

    Args:
        input_path (str): Path to the downloaded .gz file.
        output_dir (str | None): Optional directory for extracted files.

    Returns:
        str: Path to the extracted .txt file.
    """
    logger.info("Beginning pageviews extraction workflow")

    try:
        # Step 1: Validate input
        gz_path = normalize_input_path(input_path)

        # Step 2: Determine output path
        output_path = generate_extracted_output_path(gz_path, output_dir)

        # Step 3: Extract gzip file
        extracted_file = extract_gz_to_txt(gz_path, output_path)

        logger.info(
            "Pageviews extraction workflow completed: %s",
            extracted_file
        )
        return extracted_file

    except Exception as exc:
        logger.exception(
            "Pageviews extraction failed for input: %s",
            input_path
        )
        raise

