from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
from pathlib import Path

# Добавляем корень проекта в PYTHONPATH для импорта модулей
CURRENT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(CURRENT_DIR))

# Импортируем функции из наших модулей
from src.scraper import run_scraper
from src.cleaner import run_cleaner
from src.loader import run_loader


# Параметры по умолчанию для всех задач в DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # Количество попыток при ошибке
    'retry_delay': timedelta(minutes=5),  # Задержка между попытками
}


# Определяем DAG
with DAG(
    dag_id='sxodim_events_pipeline',
    default_args=default_args,
    description='Scrape, clean and load events from sxodim.com every 24 hours',
    schedule_interval='@daily',  # Запуск раз в день (можно изменить на '0 2 * * *' для запуска в 2:00 AM)
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Не запускать пропущенные задачи
    tags=['web_scraping', 'events', 'etl', 'sxodim'],
) as dag:

    # Задача 1: Scraping - парсинг данных с сайта
    scrape_task = PythonOperator(
        task_id='scrape_events',
        python_callable=run_scraper,
        op_kwargs={'min_events': 200},  # Минимум 200 событий
    )

    # Задача 2: Cleaning - очистка и нормализация данных
    clean_task = PythonOperator(
        task_id='clean_events',
        python_callable=run_cleaner,
    )

    # Задача 3: Loading - загрузка в базу данных
    load_task = PythonOperator(
        task_id='load_to_database',
        python_callable=run_loader,
    )

    # Определяем порядок выполнения задач: scrape → clean → load
    scrape_task >> clean_task >> load_task