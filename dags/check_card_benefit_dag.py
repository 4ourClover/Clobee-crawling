from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from crawling_code.crawling_check_benefit import run_check_cards_benefit_crawler

default_args = {
    'start_date': datetime(2025, 5, 23),
    'catchup': False,
    'retries': 0
}

with DAG(
    dag_id="check_card_benefit_dag",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="30 2 * * *", # 매일 2:30
    catchup=False,
    tags=["crawling"],
) as dag:
    events_task = PythonOperator(
        task_id="check_card_benefit",
        python_callable=run_check_cards_benefit_crawler,
    )
    events_task
