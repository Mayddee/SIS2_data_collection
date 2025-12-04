from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
from pathlib import Path

# Add project root to path so we can import from src
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.scraper import run_scraper
from src.cleaner import run_cleaner
from src.loader import run_loader


# Default arguments for the DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'sxodim_events_pipeline',
    default_args=default_args,
    description='Scrape events from sxodim.com, clean and load to SQLite',
    schedule_interval='@daily',  # Runs once per day
    catchup=False,
    tags=['webscraping', 'events', 'sxodim'],
)

# Task 1: Scrape data
scrape_task = PythonOperator(
    task_id='scrape_events',
    python_callable=run_scraper,
    op_kwargs={'min_events': 200},
    dag=dag,
)

# Task 2: Clean data
clean_task = PythonOperator(
    task_id='clean_events',
    python_callable=run_cleaner,
    dag=dag,
)

# Task 3: Load to database
load_task = PythonOperator(
    task_id='load_to_database',
    python_callable=run_loader,
    dag=dag,
)

# Define task dependencies: scrape → clean → load
scrape_task >> clean_task >> load_task
