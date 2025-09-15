from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'property_scraper',
    default_args=default_args,
    description='Run property scraper every morning at 9AM',
    schedule_interval='0 9 * * *',  # cron: 9 AM every day
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

run_scraper = BashOperator(
    task_id='run_scraper',
    bash_command='python /app/main_scraper.py',
    dag=dag,
)
