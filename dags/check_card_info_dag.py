from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from crawling_code.crawling_check_cards import run_check_cards_crawler

default_args = {
    'start_date': datetime(2025, 5, 23),
    'catchup': False,
    'retries': 0
}

with DAG(
    dag_id="check_card_info_dag",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="30 1 * * *", # 매일 1:30
    catchup=False,
    tags=["crawling"],
) as dag:

    check_cards_task = PythonOperator(
        task_id="check_card_info",
        python_callable=run_check_cards_crawler,
    )
    
    check_cards_task
