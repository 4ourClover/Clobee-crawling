from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from crawling_code.crawling_credit_cards import run_credit_cards_crawler

default_args = {
    'start_date': datetime(2025, 1, 1),
    'catchup': False
}

with DAG(
    dag_id="credit_card_info_dag",
    default_args=default_args,
    schedule_interval=None,  # 수동 실행
    tags=["crawling"],
) as dag:
    
    credit_cards_task = PythonOperator(
        task_id="credit_card_info",
        python_callable=run_credit_cards_crawler,
    )
    credit_cards_task
