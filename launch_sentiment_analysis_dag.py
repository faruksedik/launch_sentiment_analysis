from airflow.sdk import DAG
from pendulum import datetime, duration
from airflow.providers.standard.operators.python import PythonOperator

from launch_sentiment_analysis.include.scripts import config
from launch_sentiment_analysis.include.scripts.db import  load_to_postgres
from launch_sentiment_analysis.include.scripts.transform_pageviews import transform_pageviews
from launch_sentiment_analysis.include.scripts.extract_pageviews import extract_pageviews
from launch_sentiment_analysis.include.scripts.download_pageviews import download_pageviews


default_args = {
    "owner": "Faruk",
    "retries": 3,
    "retry_delay": duration(seconds=30),
}

with DAG(
    dag_id="launch_sentiment_analysis_dag",
    start_date=datetime(2026, 2, 2),
    default_args=default_args,
    # schedule="0 0 * * * ",
    schedule=None
):
    
    
    download = PythonOperator(
        task_id="download_pageviews",
        python_callable=download_pageviews,
        op_kwargs={
            "output_dir": config.RAW_DIR,
        },
    )

    extract = PythonOperator(
        task_id="extract_pageviews",
        python_callable=extract_pageviews,
        op_kwargs={
            "input_path": "{{ ti.xcom_pull(task_ids='download_pageviews') }}",
            "output_dir": config.STAGING_DIR,
        },
    )

    transform = PythonOperator(
        task_id="transform_pageviews",
        python_callable=transform_pageviews,
        op_kwargs={
            "input_file": "{{ ti.xcom_pull(task_ids='extract_pageviews') }}",
            "output_dir": config.STAGING_DIR,
        },
    )

    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        op_kwargs={
            "csv_file": "{{ ti.xcom_pull(task_ids='transform_pageviews') }}",
            "conn_id": config.POSTGRES_CONN_ID,
            "sql_file_path": config.SQL_FILE_PATH,
        },
    )


    download >> extract >> transform >> load