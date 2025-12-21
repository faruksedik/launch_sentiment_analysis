from airflow.sdk import DAG, Variable
from pendulum import datetime, duration
from airflow.providers.standard.operators.python import PythonOperator

from launch_sentiment_analysis.include.scripts.db import get_most_viewed_page, load_to_postgres
from launch_sentiment_analysis.include.scripts.transform_pageviews import transform_pageviews
from launch_sentiment_analysis.include.scripts.extract_pageviews import extract_gzip
from launch_sentiment_analysis.include.scripts.download_pageviews import download_pageviews




BASE_URL = Variable.get("PAGEVIEWS_BASE_URL")
RAW_DIR = Variable.get("SENTIMENT_ANALYSIS_RAW_DATA_DIR")
STAGING_DIR = Variable.get("SENTIMENT_ANALYSIS_STAGING_DATA_DIR")

EXECUTION_HOUR = "20251210-16"
FILENAME = f"pageviews-{EXECUTION_HOUR}0000.gz"

default_args = {
    "owner": "Faruk",
    "retries": 3,
    "retry_delay": duration(seconds=30),
}

with DAG(
    dag_id="launch_sentiment_analysis_dag",
    start_date=datetime(2025, 12, 20),
    default_args=default_args,
    # schedule="0 0 * * * ",
    schedule=None
):
    


    download = PythonOperator(
        task_id="download_pageviews",
        python_callable=download_pageviews,
        op_kwargs={
            "url": f"{BASE_URL}/{FILENAME}",
            "output_path": f"{RAW_DIR}/{FILENAME}",
        },
    )

    extract = PythonOperator(
        task_id="extract_pageviews",
        python_callable=extract_gzip,
        op_kwargs={
            "input_path": "{{ ti.xcom_pull(task_ids='download_pageviews') }}",
            "output_path": f"{STAGING_DIR}/pageviews.txt",
        },
    )

    transform = PythonOperator(
        task_id="transform_pageviews",
        python_callable=transform_pageviews,
        op_kwargs={
            "input_file": "{{ ti.xcom_pull(task_ids='extract_pageviews') }}",
            "output_file": f"{STAGING_DIR}/pageviews.csv",
            "event_time": EXECUTION_HOUR,
        },
    )

    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        op_kwargs={
            "csv_file": "{{ ti.xcom_pull(task_ids='transform_pageviews') }}",
            "conn_id": "postgres_connection",
        },
    )


    analyze = PythonOperator(
        task_id="get_most_viewed_page",
        python_callable=get_most_viewed_page,
        op_kwargs={
            "conn_id": "postgres_connection"
        },
    )


    download >> extract >> transform >> load >> analyze