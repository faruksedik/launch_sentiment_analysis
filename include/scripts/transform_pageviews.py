import csv
import hashlib
from datetime import datetime
from pathlib import Path

from launch_sentiment_analysis.include.scripts.logger import get_logger
from launch_sentiment_analysis.include.scripts.config import COMPANIES

logger = get_logger("transform_pageviews")



def extract_event_time_from_filename(input_file: str) -> datetime:
    """
    Extract event timestamp from a Wikimedia pageviews text filename.

    Expected filename format:
        pageviews_YYYYMMDD-HH.txt

    Example:
        pageviews_20251230-10.txt → 2025-12-30 10:00:00

    This timestamp represents the hour for which the pageviews
    data was generated.

    Args:
        input_file (str): Path to extracted pageviews text file.

    Returns:
        datetime: Parsed event timestamp.

    Raises:
        ValueError: If filename does not match expected format.
    """
    logger.debug("Extracting event time from file path: %s", input_file)

    # Extract filename without extension (removes ".txt")
    filename = Path(input_file).stem

    # Expected stem: projectviews_YYYYMMDD-HH
    logger.debug("Parsed filename stem: %s", filename)

    try:
        # Split on underscore and extract date-hour segment
        # Example: projectviews_20251230-10 → 20251230-10
        date_part = filename.split("_")[1]

        # Remove dash to normalize into YYYYMMDDHH
        normalized = date_part.replace("-", "")
        logger.debug("Normalized date-hour string: %s", normalized)

        # Parse into datetime object
        event_time = datetime.strptime(normalized, "%Y%m%d%H")

        logger.info(
            "Successfully extracted event time %s from filename %s",
            event_time,
            filename
        )
        return event_time

    except Exception as exc:
        logger.error(
            "Failed to extract event time from filename: %s",
            filename,
            exc_info=True
        )
        raise ValueError(
            f"Invalid filename format for event time extraction: {filename}"
        ) from exc


def generate_csv_output_path(input_file: str, output_dir: str,) -> Path:
    """
    Generate the output CSV file path using the same base filename
    as the input text file.

    Example:
        projectviews_20251230-10.txt -> projectviews_20251230-10.csv

    This function ensures that the output directory exists
    before returning the final output path.

    Args:
        input_file (str): Path to extracted pageviews text file.
        output_dir (str): Directory where transformed CSV files
            should be written.

    Returns:
        Path: Full path to the output CSV file.
    """
    logger.debug(
        "Generating CSV output path from input file: %s, output_dir: %s",
        input_file,
        output_dir
    )

    # Convert inputs to Path objects for safe path operations
    input_path = Path(input_file)
    output_dir_path = Path(output_dir)

    # Ensure output directory exists (idempotent operation)
    output_dir_path.mkdir(parents=True, exist_ok=True)
    logger.debug("Ensured output directory exists: %s", output_dir_path)

    # Replace .txt extension with .csv while preserving filename stem
    output_filename = input_path.stem + ".csv"

    output_path = output_dir_path / output_filename
    logger.info("Generated CSV output path: %s", output_path)

    return output_path


def parse_pageviews_line(line: str):
    """
    Parse a single raw Wikipedia pageviews record line.

    Expected raw format (space-delimited):
        <project> <page_title> <pageviews> <bytes>

    Example:
        en Main_Page 12345 678901

    Args:
        line (str): Raw line from the Wikimedia pageviews file.

    Returns:
        tuple[str, int] | None:
            (page_title, pageviews) if the line is valid,
            otherwise None if parsing fails.
    """
    # Remove leading/trailing whitespace and split by spaces
    parts = line.strip().split()

    # A valid pageviews line must contain at least:
    # project, page_title, pageviews
    if len(parts) < 3:
        logger.debug("Skipping malformed line (too few fields): %s", line.strip())
        return None

    # Extract relevant fields by position
    page_title = parts[1]
    pageviews_raw = parts[2]

    try:
        # Convert pageviews count to integer
        pageviews = int(pageviews_raw)
    except ValueError:
        logger.debug(
            "Skipping line with non-integer pageviews: %s",
            line.strip()
        )
        return None

    return page_title, pageviews


def generate_page_title_id(
    raw_line: str,
    event_timestamp: datetime,
) -> str:
    """
    Generate a deterministic surrogate key for a pageviews record
    using SHA-256 hashing.

    The hash is derived from:
        - the event timestamp (hour-level granularity)
        - the original raw input line

    This guarantees:
        - identical inputs always produce the same ID
        - different hours or different records produce different IDs

    Args:
        raw_line (str): Original raw pageviews line.
        event_timestamp (datetime): Event timestamp associated
            with the pageviews record.

    Returns:
        str: SHA-256 hexadecimal hash string.
    """
    # Combine timestamp and raw line into a single canonical string
    # The pipe character avoids accidental ambiguity between fields
    hash_input = f"{event_timestamp.isoformat()}|{raw_line}"

    # Generate SHA-256 hash as a hexadecimal string
    record_id = hashlib.sha256(
        hash_input.encode("utf-8")
    ).hexdigest()

    logger.debug(
        "Generated page title ID for timestamp=%s",
        event_timestamp.isoformat()
    )

    return record_id


def transform_pageviews(input_file: str, output_dir: str,) -> str:
    """
    Transform Wikipedia pageviews data into a filtered CSV dataset.

    This function performs the following steps:
    1. Extract the event timestamp from the input filename
    2. Generate the output CSV file path
    3. Parse and filter raw pageviews lines by target companies
    4. Generate deterministic surrogate keys per record
    5. Write transformed records to a CSV file

    Args:
        input_file (str): Path to extracted .txt pageviews file.
        output_dir (str): Directory for transformed CSV files.

    Returns:
        str: Path to the transformed CSV file.
    """
    try:
        logger.info("Starting pageviews transformation | input=%s", input_file)

        # Step 1: Extract logical event timestamp from filename
        event_timestamp = extract_event_time_from_filename(input_file)
        logger.info("Extracted event timestamp: %s", event_timestamp)

        # Step 2: Generate output CSV file path
        output_path = generate_csv_output_path(input_file, output_dir)
        logger.info("Output CSV path resolved: %s", output_path)

        record_count = 0
        processed_lines = 0

        # Step 3 – 5: Parse, filter, generate IDs, and write CSV
        with open(input_file, "r", encoding="utf-8") as infile, open(
            output_path, "w", newline="", encoding="utf-8"
        ) as outfile:

            writer = csv.writer(outfile)

            # Write CSV header
            writer.writerow(
                [
                    "page_title_id",
                    "page_title",
                    "pageviews",
                    "event_timestamp",
                ]
            )

            for line in infile:
                processed_lines += 1

                # Parse raw pageviews line
                parsed = parse_pageviews_line(line)
                if not parsed:
                    continue

                page_title, pageviews = parsed

                # Filter to only target companies
                if page_title not in COMPANIES:
                    continue

                # Generate deterministic surrogate key
                page_title_id = generate_page_title_id(
                    line.strip(), event_timestamp
                )

                # Write transformed record
                writer.writerow(
                    [
                        page_title_id,
                        page_title,
                        pageviews,
                        event_timestamp,
                    ]
                )

                record_count += 1

                # Periodic debug logging to avoid log flooding
                if record_count % 100_000 == 0:
                    logger.debug(
                        "Processed %d lines | written %d records",
                        processed_lines,
                        record_count,
                    )

        logger.info(
            "Transformation completed successfully | output=%s | "
            "processed_lines=%d | records_written=%d",
            output_path,
            processed_lines,
            record_count,
        )

        return str(output_path)

    except Exception:
        logger.exception(
            "Pageviews transformation failed | input=%s",
            input_file,
        )
        raise