from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

# ensure Airflow recognizes project root so src/ imports resolve
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# import callables exactly as they are defined in src/
from src.scraper import run_scraper
from src.cleaner import run_cleaner
from src.loader import run_loader

# default retry behavior for all tasks
default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sxodim_events_pipeline",
    default_args=default_args,
    description="Scrape, clean and load events from sxodim.com every 24 hours",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["web_scraping", "events", "etl", "sxodim"],
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_events",
        python_callable=run_scraper,
        op_kwargs={"min_events": 200}
    )

    clean_task = PythonOperator(
        task_id="clean_events",
        python_callable=run_cleaner
    )

    load_task = PythonOperator(
        task_id="load_to_database",
        python_callable=run_loader
    )

    scrape_task >> clean_task >> load_task
