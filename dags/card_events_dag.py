from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from event_info_card.crawling_card_events import card_events_crawler

default_args = {
    'start_date': datetime(2025, 5, 19),
    'catchup': False
}

with DAG(
    dag_id="card_events_dag",
    default_args=default_args,
    schedule_interval=None,  # 수동 실행
    tags=["crawling"],
) as dag:

    events_task = PythonOperator(
        task_id="card_events_info",
        python_callable=card_events_crawler,
    )
    
    events_task
