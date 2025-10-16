from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from datetime import datetime, timedelta
from stroll.crawl.process_place import process_chunk_of_places, send_chunk_to_api
import json
import os

# Dataset 정의: crawl_places_dag에서 생성되는 파일
places_dataset = Dataset("file://dags/stroll/crawl/tmp/place_obj_list.ndjson")

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
    dag_id="process_places",
    default_args=default_args,
    schedule=[places_dataset],  # Dataset 업데이트 시 자동 실행
    catchup=False,
    tags=["stroll", "process", "dynamic-mapping", "phase-2"],
    description="장소 처리 파이프라인 (Phase 2: 주소 변환 + API 전송)",
    max_active_tasks=10,  # 동시 처리 최대 10개 장소
) as dag:
    
    @task(task_id="load_places")
    def load_places_task():
        """
        크롤링된 장소 데이터를 NDJSON 파일에서 로드합니다.
        
        Returns:
            list: 장소 객체 리스트
        """
        # crawl_places_dag에서 저장한 파일 읽기
        directory_path = os.path.join(
            os.path.dirname(__file__), 
            "stroll", "crawl", "tmp"
        )
        filepath = os.path.join(directory_path, "place_obj_list.ndjson")
        
        places = []
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():  # 빈 줄 제외
                        places.append(json.loads(line))
            
            print(f"\n{'='*50}")
            print(f"📂 크롤링 데이터 로드 완료")
            print(f"{'='*50}")
            print(f"로드된 장소: {len(places)}개")
            print(f"파일 경로: {filepath}")
            print(f"{'='*50}\n")
            
        except FileNotFoundError:
            print(f"⚠️  파일을 찾을 수 없습니다: {filepath}")
            return []
        except Exception as e:
            print(f"⚠️  파일 로드 중 오류: {e}")
            return []
        place_chunk_list = []
        for idx in range(0, len(places), 100):
            if(idx % 100 == 0 and idx + 100 < len(places)):
                place_chunk_list.append(places[idx:idx+100])
            else:
                place_chunk_list.append(places[idx:])
        return place_chunk_list
    
    @task(task_id="convert_address")
    def convert_address_chunk_task(place_chunk_list: list):
        return process_chunk_of_places(place_chunk_list=place_chunk_list)
    
    @task(task_id="send_to_api")
    def send_chunk_to_api_task(place_chunk_list: list):
        return send_chunk_to_api(place_chunk_list=place_chunk_list)
    
    @task(task_id="summarize_results")
    def summarize_task(results: list):
        success_count = sum(1 for r in results if r.get("status") == "success")
        fail_count = sum(1 for r in results if r.get("status") == "failed")
        
        # 실패 단계별 집계
        conversion_failures = sum(1 for r in results if r.get("stage") == "address_conversion")
        api_failures = sum(1 for r in results if r.get("stage") == "api_send")
        
        print("\n" + "="*50)
        print("📊 최종 처리 결과 요약")
        print("="*50)
        print(f"✓ 성공: {success_count}개")
        print(f"✗ 실패: {fail_count}개")
        print(f"  └─ 주소 변환 실패 (카카오 API): {conversion_failures}개")
        print(f"  └─ API 전송 실패 (Stroll API): {api_failures}개")
        print(f"📍 총 처리: {len(results)}개")
        print(f"성공률: {success_count / len(results) * 100:.1f}%" if results else "N/A")
        
        if fail_count > 0:
            print("\n❌ 실패한 장소 상세:")
            for r in results:
                if r.get("status") == "failed":
                    stage = r.get("stage", "unknown")
                    print(f"  - {r.get('placeName', 'Unknown')} [{stage}]: {r.get('error', 'Unknown error')}")
        
        print("="*50 + "\n")
        
        return {
            "total": len(results),
            "success": success_count,
            "failed": fail_count,
            "conversion_failures": conversion_failures,
            "api_failures": api_failures,
            "success_rate": round(success_count / len(results) * 100, 2) if results else 0,
            "timestamp": datetime.now().isoformat(),
        }
    
    # Task 의존성 정의 with Dynamic Task Mapping (2-Stage Pipeline)
    # 1. 크롤링 데이터 로드
    places = load_places_task()
    
    # 2-1. 각 장소의 주소를 변환 (카카오 API) - 1차 Dynamic Mapping
    converted_places = convert_address_chunk_task.expand(place_chunk_list=places)
    
    # 2-2. 변환된 주소를 API로 전송 (Stroll API) - 2차 Dynamic Mapping
    send_results = send_chunk_to_api_task.expand(place_chunk_list=converted_places)
    
    # 3. 모든 처리 결과 요약
    summarize_task(send_results)

