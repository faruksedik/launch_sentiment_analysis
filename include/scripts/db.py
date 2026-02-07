from pathlib import Path
from airflow.providers.postgres.hooks.postgres import PostgresHook
from launch_sentiment_analysis.include.scripts.logger import get_logger

logger = get_logger("db")

def validate_load_files(csv_file: str, sql_file_path: str) -> None:
    """
    Validate that all required files exist before executing a load step.

    Args:
        csv_file (str): Path to the transformed CSV file.
        sql_file_path (str): Path to the SQL load (COPY) script.

    Raises:
        FileNotFoundError: If the CSV file or SQL script does not exist.
    """
    logger.debug(
        "Validating load input files | csv_file=%s | sql_file=%s",
        csv_file,
        sql_file_path,
    )

    # Convert input strings to Path objects for safe file checks
    csv_path = Path(csv_file)
    sql_path = Path(sql_file_path)

    # Validate transformed CSV file exists
    if not csv_path.exists():
        logger.error("Transformed CSV file not found: %s", csv_path)
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    # Validate SQL load script exists
    if not sql_path.exists():
        logger.error("SQL load script not found: %s", sql_path)
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    logger.info(
        "Validated load files successfully | csv=%s | sql=%s",
        csv_path,
        sql_path,
    )

def read_copy_sql(sql_file_path: str) -> str:
    """
    Read a SQL COPY script from disk.

    Args:
        sql_file_path (str): Path to the SQL file.

    Returns:
        str: Full SQL script content as a string.

    Raises:
        FileNotFoundError: If the SQL file does not exist.
        IOError: If the file cannot be read.
    """
    logger.debug("Reading SQL file from path: %s", sql_file_path)

    try:
        with open(sql_file_path, "r", encoding="utf-8") as sql_file:
            sql_content = sql_file.read()

        logger.info(
            "Successfully read SQL file | path=%s | size=%d bytes",
            sql_file_path,
            len(sql_content),
        )
        return sql_content

    except Exception:
        logger.exception(
            "Failed to read SQL file from path: %s",
            sql_file_path,
        )
        raise


def execute_copy_to_postgres(csv_file: str, conn_id: str, sql: str,) -> None:
    """
    Execute a PostgreSQL COPY command using Airflow's PostgresHook.

    Args:
        csv_file (str): Path to the CSV file to be loaded.
        conn_id (str): Airflow Postgres connection ID.
        sql (str): SQL COPY command to execute.

    Raises:
        Exception: If the COPY operation fails.
    """
    logger.info(
        "Starting COPY operation | csv_file=%s | conn_id=%s",
        csv_file,
        conn_id,
    )

    try:
        # Initialize Postgres hook using Airflow connection
        hook = PostgresHook(postgres_conn_id=conn_id)
        logger.debug("PostgresHook initialized successfully")

        # Execute COPY FROM STDIN using copy_expert
        hook.copy_expert(sql=sql, filename=csv_file)

        logger.info(
            "COPY operation completed successfully | csv_file=%s",
            csv_file,
        )

    except Exception:
        logger.exception(
            "COPY operation failed | csv_file=%s | conn_id=%s",
            csv_file,
            conn_id,
        )
        raise


def load_to_postgres(csv_file: str, conn_id: str, sql_file_path: str,) -> None:
    """
    Load a transformed CSV file into PostgreSQL using a COPY FROM STDIN operation.

    This function orchestrates the load step by:
        1. Validating required input files (CSV and SQL COPY script)
        2. Reading the SQL COPY script into memory
        3. Executing the COPY operation using PostgresHook

    Args:
        csv_file (str): Path to the transformed CSV file.
        conn_id (str): Airflow Postgres connection ID.
        sql_file_path (str): Path to the SQL COPY script.

    Raises:
        FileNotFoundError: If the CSV or SQL file is missing.
        RuntimeError: If the COPY operation fails.
    """
    try:
        logger.info(
            "Starting Postgres load | csv_file=%s | conn_id=%s",
            csv_file,
            conn_id,
        )

        # Step 1: Validate that required files exist
        validate_load_files(csv_file, sql_file_path)
        logger.debug("Input files validated successfully")

        # Step 2: Read the COPY SQL script into memory
        sql = read_copy_sql(sql_file_path)
        logger.debug("COPY SQL script loaded | size=%d bytes", len(sql))

        # Step 3: Execute the COPY operation
        execute_copy_to_postgres(csv_file, conn_id, sql)

        logger.info(
            "Postgres load completed successfully | csv_file=%s | conn_id=%s",
            csv_file,
            conn_id,
        )

    except FileNotFoundError:
        logger.exception(
            "Load failed due to missing file | csv_file=%s | sql_file=%s",
            csv_file,
            sql_file_path,
        )
        # Re-raise to let Airflow mark task as failed
        raise

    except Exception as exc:
        logger.exception(
            "Load failed during COPY operation | csv_file=%s | conn_id=%s",
            csv_file,
            conn_id,
        )
        # Wrap exception to add context for Airflow or external monitoring
        raise RuntimeError(
            f"CSV load to PostgreSQL failed | csv_file={csv_file} | conn_id={conn_id}"
        ) from exc