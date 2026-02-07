from airflow.models import Variable

# Base Url
BASE_URL = "https://dumps.wikimedia.org/other/pageviews/"

# Airflow Variables
RAW_DIR = Variable.get("SENTIMENT_ANALYSIS_RAW_DATA_DIR")
STAGING_DIR = Variable.get("SENTIMENT_ANALYSIS_STAGING_DATA_DIR")
SQL_FILE_DIR = Variable.get("SENTIMENT_ANALYSIS_SQL_FILE_DIR")
LOG_FILE_DIR = Variable.get("SENTIMENT_ANALYSIS_LOG_FILE_DIR")


# Connection IDs
POSTGRES_CONN_ID = "postgres_connection"

# Static paths
SQL_FILE_PATH = f"{SQL_FILE_DIR}/load_to_postgres.sql"

# Target companies to filter from Wikipedia pageviews
COMPANIES = {
    "Amazon",
    "Apple_Inc.",
    "Facebook",
    "Google",
    "Microsoft",
}

