from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from card_info_crawling.crawling_check_cards import run_check_cards_crawler
from card_info_crawling.crawling_credit_cards import run_credit_cards_crawler

default_args = {
    'start_date': datetime(2025, 5, 19),
    'catchup': False
}

with DAG(
    dag_id="card_info_dag",
    default_args=default_args,
    schedule_interval=None,  # 수동 실행
    tags=["crawling"],
) as dag:

    check_cards_task = PythonOperator(
        task_id="check_card_info",
        python_callable=run_check_cards_crawler,
    )

    credit_cards_task = PythonOperator(
        task_id="credit_card_info",
        python_callable=run_credit_cards_crawler,
    )
