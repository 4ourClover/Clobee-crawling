from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from crawling_code.crawling_card_events import card_events_crawler

default_args = {
    'start_date': datetime(2025, 5, 23),
    'catchup': False,
    'retries': 0
}

with DAG(
    dag_id="card_events_info_dag",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="0 1 * * *", # 매일 1:00
    catchup=False,
    tags=["crawling"],
) as dag:

    events_task = PythonOperator(
        task_id="card_events_info",
        python_callable=card_events_crawler,
    )
    events_task
