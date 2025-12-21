import csv
from contextlib import contextmanager
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook
from launch_sentiment_analysis.include.scripts.logger import get_logger

logger = get_logger(__name__)

# Persistent analysis log location (outside task logs)
ANALYSIS_LOG_DIR = Path("/opt/airflow/dags/launch_sentiment_analysis/include/logs")

ANALYSIS_LOG_FILE = ANALYSIS_LOG_DIR/"analysis_result.log"

@contextmanager
def postgres_cursor(conn_id: str):
    """
    Yield a PostgreSQL cursor using Airflow's PostgresHook.

    Ensures:
    - Automatic commit on success
    - Rollback on failure
    - Proper cleanup of cursor and connection

    Args:
        conn_id (str): Airflow Postgres connection ID.

    Yields:
        psycopg2.cursor: Active database cursor.
    """
    logger.debug(f"Opening Postgres connection | conn_id={conn_id}")

    hook = PostgresHook(postgres_conn_id=conn_id)
    connection = hook.get_conn()
    cursor = connection.cursor()

    try:
        yield cursor
        connection.commit()
    except Exception:
        connection.rollback()
        logger.exception("Postgres transaction failed and was rolled back")
        raise
    finally:
        cursor.close()
        connection.close()
        logger.debug("Postgres connection closed")


def load_to_postgres(csv_file: str, conn_id: str) -> None:
    """
    Load transformed Wikipedia pageviews data into PostgreSQL.

    The table schema is aligned with the transformed dataset
    (year, month, day, hour) for analytics and partitioning.

    Args:
        csv_file (str): Path to transformed CSV file.
        conn_id (str): Airflow Postgres connection ID.
    """
    logger.info(f"Loading transformed data into Postgres | file={csv_file}")

    with postgres_cursor(conn_id) as cursor:
        # Create table aligned with transformed schema
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS wikipedia_pageviews (
                page_title_id INTEGER PRIMARY KEY,
                page_title TEXT NOT NULL,
                pageviews INTEGER NOT NULL,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                day INTEGER NOT NULL,
                hour INTEGER NOT NULL
            );
            """
        )

        # Insert transformed records with conflict protection
        with open(csv_file, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            for row in reader:
                cursor.execute(
                    """
                    INSERT INTO wikipedia_pageviews (
                        page_title_id,
                        page_title,
                        pageviews,
                        year,
                        month,
                        day,
                        hour
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (page_title_id) DO NOTHING;
                    """,
                    (
                        row["page_title_id"],
                        row["page_title"],
                        row["pageviews"],
                        row["year"],
                        row["month"],
                        row["day"],
                        row["hour"],
                    ),
                )

    logger.info("Data successfully loaded into Postgres")


def get_most_viewed_page(conn_id: str) -> dict:
    """
    Analyze Wikipedia pageviews and return the most viewed page
    along with the time window (year, month, day, hour).

    Results are logged to:
    - A persistent analysis log file

    Args:
        conn_id (str): Airflow Postgres connection ID.

    Returns:
        dict: Most viewed page details including time dimensions.
    """
    logger.info("Running analysis: most viewed Wikipedia page with time context")

    query = """
        SELECT
            page_title,
            year,
            month,
            day,
            hour,
            SUM(pageviews) AS total_pageviews
        FROM wikipedia_pageviews
        GROUP BY page_title, year, month, day, hour
        ORDER BY total_pageviews DESC
        LIMIT 1;
    """

    with postgres_cursor(conn_id) as cursor:
        cursor.execute(query)
        row = cursor.fetchone()

    if not row:
        logger.warning("No data found in wikipedia_pageviews table")
        return {}

    result = {
        "page_title": row[0],
        "year": row[1],
        "month": row[2],
        "day": row[3],
        "hour": row[4],
        "total_pageviews": row[5],
    }

    # Ensure persistent analysis log directory exists
    ANALYSIS_LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Well-drafted, report message
    report_message = (
        f"Most viewed Wikipedia page: '{result['page_title']}' | "
        f"Total views: {result['total_pageviews']} | "
        f"Time window: "
        f"{result['year']}-{result['month']:02d}-{result['day']:02d} "
        f"at {result['hour']:02d}:00"
    )

    # Write analysis result to persistent log file
    with open(ANALYSIS_LOG_FILE, "a", encoding="utf-8") as file:
        file.write(report_message + "\n")

    logger.info(report_message)

    return result
