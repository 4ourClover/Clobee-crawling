from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from crawling_code.crawling_credit_cards import run_credit_cards_crawler

default_args = {
    'start_date': datetime(2025, 5, 23),
    'catchup': False,
    'retries': 0
}

with DAG(
    dag_id="credit_card_info_dag",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="30 2 * * *", # 매일 2:30
    catchup=False,
    tags=["crawling"],
) as dag:
    
    credit_cards_task = PythonOperator(
        task_id="credit_card_info",
        python_callable=run_credit_cards_crawler,
    )
    credit_cards_task
