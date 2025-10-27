from airflow.decorators import task
from airflow import DAG
from airflow.operators.python import PythonOperator
from fill_content_tasks.SelectPlacesFromRds import select_places_from_rds
from stroll_review_crawling.CrawlReview import crawl_review
from fill_content_tasks.FillContent import fill_contents
from fill_content_tasks.SaveContentInRds import save_content_in_rds
from datetime import datetime, timedelta
import os

TMP_PATH = os.path.dirname(os.path.abspath(__file__)) + "/tmp"
print(TMP_PATH)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 6),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,  # 개별 장소 처리 실패 시 2번 재시도
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id='fill_content_dag',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['stroll', 'fill_content', 'crawl', 'openai'],
) as dag:

    @task(task_id="select_empty_places")
    def select_empty_places():
        select_places_from_rds(TMP_PATH)

    @task(task_id="crawl_reviews")
    def crawl_reviews():
        crawl_review(TMP_PATH)

    @task(task_id="fill_contents", trigger_rule="all_done")
    def fill_contents_task():
        fill_contents(TMP_PATH)

    @task(task_id="save_content_in_rds")
    def save_content_in_rds():
        save_content_in_rds(TMP_PATH)


    select_task = select_empty_places()
    crawl_task = crawl_reviews()
    fill_task = fill_contents_task()
    save_task = save_content_in_rds()

    # 실행 순서 정의
    select_task >> crawl_task >> fill_task >> save_task
