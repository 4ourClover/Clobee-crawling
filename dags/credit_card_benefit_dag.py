from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from crawling_code.crawling_credit_benefit import run_credit_cards_benefit_crawler

default_args = {
    'start_date': datetime(2025, 5, 23),
    'catchup': False,
    'retries': 0
}

with DAG(
    dag_id="credit_card_benefit_dag",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval=None,
    tags=["crawling"],
) as dag:
    events_task = PythonOperator(
        task_id="credit_card_benefit",
        python_callable=run_credit_cards_benefit_crawler,
    )
    events_task
