from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from crawling_code.crawling_card_events import card_events_crawler

default_args = {
    'start_date': datetime(2025, 1, 1),
    'catchup': False
}

with DAG(
    dag_id="card_events_info_dag",
    default_args=default_args,
    schedule_interval="0 1 * * *", # 매일 오전 1시
    tags=["crawling"],
) as dag:

    events_task = PythonOperator(
        task_id="card_events_info",
        python_callable=card_events_crawler,
    )
    events_task
