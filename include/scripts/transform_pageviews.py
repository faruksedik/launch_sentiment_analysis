import csv
from datetime import datetime
from pathlib import Path

from launch_sentiment_analysis.include.scripts.logger import get_logger

logger = get_logger(__name__)

# Target companies to filter from Wikipedia pageviews
COMPANIES = {
    "Amazon",
    "Apple_Inc.",
    "Facebook",
    "Google",
    "Microsoft",
}


def transform_pageviews(
    input_file: str,
    output_file: str,
    event_time: str,
) -> str:
    """
    Transform raw Wikipedia pageviews data by filtering for selected companies
    and enriching records with time-based partition columns.

    Args:
        input_file (str): Path to extracted pageviews text file.
        output_file (str): Path to output transformed CSV file.
        event_time (str): Execution timestamp (YYYYMMDD-HH).

    Returns:
        str: Path to the transformed CSV file.
    """
    try:
        logger.info(f"Starting pageviews transformation | input={input_file}")

        # Ensure output directory exists
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)

        # Normalize and parse execution timestamp
        normalized_time = event_time.replace("-", "")
        timestamp = datetime.strptime(normalized_time, "%Y%m%d%H")

        year = timestamp.year
        month = timestamp.month
        day = timestamp.day
        hour = timestamp.hour

        # Surrogate key counter
        page_title_id = 1

        with open(input_file, "r", encoding="utf-8") as infile, open(
            output_file, "w", newline="", encoding="utf-8"
        ) as outfile:

            writer = csv.writer(outfile)

            # DB-friendly schema with partition columns
            writer.writerow(
                [
                    "page_title_id",
                    "page_title",
                    "pageviews",
                    "year",
                    "month",
                    "day",
                    "hour",
                ]
            )

            for line in infile:
                # Defensive parsing to handle malformed rows
                parts = line.strip().split()
                if len(parts) < 4:
                    continue
                
                # Select and unpack useful parts
                useful_part = parts[1:3]
                page_title, pageviews = useful_part

                # Filter Wikipedia records for target companies
                if page_title in COMPANIES:
                    writer.writerow(
                        [
                            page_title_id,
                            page_title,
                            int(pageviews),
                            year,
                            month,
                            day,
                            hour,
                        ]
                    )
                    page_title_id += 1

        logger.info(
            f"Transformation completed successfully | output={output_file} | records={page_title_id - 1}"
        )
        return output_file

    except Exception as exc:
        logger.exception(
            f"Pageviews transformation failed | input={input_file} | output={output_file}"
        )
        raise
