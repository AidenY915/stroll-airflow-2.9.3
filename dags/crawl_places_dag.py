from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from datetime import datetime, timedelta
from stroll.crawl.crawl import crawl

# Dataset 정의: 크롤링 결과 파일
places_dataset = Dataset("file://dags/stroll/crawl/tmp/place_obj_list.ndjson")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 6),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,#1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crawl_places",
    default_args=default_args,
    schedule_interval=timedelta(days=7),  # 매주 실행
    catchup=False,
    tags=["stroll", "crawl", "naver", "phase-1"],
    description="네이버 지도 크롤링 (Phase 1: 데이터 수집)",
) as dag:
    
    @task(
        task_id="crawl_and_save",
        outlets=[places_dataset]  # Dataset 업데이트 선언
    )
    def crawl_and_save_task():
        # 크롤링 실행 (내부에서 NDJSON 파일로 저장됨)
        places = crawl(None)
        
        stats = {
            "total_crawled": len(places),
            "timestamp": datetime.now().isoformat(),
        }
        
        print(f"\n{'='*50}")
        print(f"크롤링 완료 및 Dataset 업데이트")
        print(f"{'='*50}")
        print(f"총 크롤링: {stats['total_crawled']}개 장소")
        print(f"저장 위치: dags/stroll/crawl/tmp/place_obj_list.ndjson")
        print(f"다음 단계: process_places DAG가 자동으로 시작됩니다")
        print(f"{'='*50}\n")
        
        return stats
    
    crawl_and_save_task()

