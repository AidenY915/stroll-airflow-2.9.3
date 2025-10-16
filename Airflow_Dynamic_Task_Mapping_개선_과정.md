# Airflow 크롤링 파이프라인 개선 과정

## 📌 프로젝트 개요

- **프로젝트**: Stroll Airflow 크롤링 시스템
- **목적**: 네이버 지도에서 장소 정보를 크롤링하여 주소 변환 후 API로 전송
- **날짜**: 2025년 10월 12일

---

## 🚨 문제 인식

### 초기 아키텍처의 문제점

기존 시스템은 다음과 같은 3단계 파이프라인으로 구성되어 있었습니다:

```
[크롤링 Task] → [주소 변환 Task] → [API 전송 Task]
```

#### 발견된 문제점들

**1. 크롤링 단계 (`crawl.py`)**

```python
for li_element in li_elements:
    title = li_element.find_element(...)
    # ... 크롤링 작업
    place_obj_list.append(place_obj)

export_ndjson(place_obj_list)  # 마지막에 한 번만 저장
```

**문제**:

- 100개 장소 중 50번째에서 에러 발생 → **전체 크롤링 실패**
- 49개의 성공한 데이터도 모두 손실
- try-except 처리 없음

**2. 주소 변환 단계 (`convert_address.py`)**

```python
for place_obj in place_obj_list:
    road_address = convert_to_road_address(rest_address)
    if road_address is None:
        raise Exception("주소 변환 실패")  # 여기서 전체 중단!
```

**문제**:

- 하나의 주소 변환 실패 → **전체 프로세스 중단**
- 이미 변환된 데이터도 저장되지 않음

**3. API 전송 단계**

- 동일한 문제: 하나 실패 시 전체 중단

---

## 💡 해결 방안 탐색

### 1차 고민: Python적 처리 vs Airflow적 처리

#### ❌ Python적 처리 (기각)

각 단계에서 try-except로 예외 처리

```python
for place_obj in place_obj_list:
    try:
        process(place_obj)
    except Exception as e:
        log_error(e)
        continue
```

**단점**:

- Airflow의 장점을 활용하지 못함
- 실패한 항목의 개별 재시도 어려움
- 모니터링 불편
- 병렬 처리 불가

#### ✅ Airflow적 처리 (채택)

Dynamic Task Mapping 활용

```python
places = crawl_task()
process_results = process_place_task.expand(place_obj=places)
```

**장점**:

- 개별 재시도 가능
- 병렬 처리 가능
- 시각적 모니터링
- 부분 성공 허용

### 2차 고민: URL 직접 접근 제약

**문제**: 네이버 지도 크롤링 특성상 각 장소 페이지에 URL로 직접 접근 불가능

```python
# Selenium 세션 유지하면서 순차적으로 페이지 클릭 필요
page_a.click()  # 페이지 버튼 클릭해야만 접근 가능
```

**해결**: 하이브리드 방식 채택

- **크롤링**: Python적 예외 처리 (Selenium 특성상 순차 처리 필수)
- **변환+전송**: Airflow Dynamic Task Mapping (병렬 처리 가능)

### 3차 고민: 단일 DAG vs 분리된 DAG

**문제**: 크롤링과 처리를 하나의 DAG에서 할지, 분리할지?

**검토한 방식들**:

1. **단일 DAG**: 크롤링 → Dynamic Mapping 처리
2. **TriggerDagRunOperator**: DAG1이 DAG2를 강제 트리거
3. **ExternalTaskSensor**: DAG2가 DAG1 완료를 대기
4. **Dataset (Airflow 2.4+)**: 데이터 중심의 자동 트리거 ⭐

#### ✅ Dataset 방식 채택

**장점**:

- 📊 데이터 중심 설계 (파일이 업데이트되면 자동 실행)
- 🔄 느슨한 결합 (각 DAG 독립적 실행 가능)
- 👁️ UI에서 Dataset 의존성 시각화
- 🎯 명확한 관심사 분리 (크롤링 vs 처리)
- 🔁 실패한 처리만 재실행 가능

**결정**:

- **DAG 1 (crawl_places)**: 크롤링 → Dataset 업데이트
- **DAG 2 (process_places)**: Dataset 트리거 → Dynamic Mapping 처리

---

## 🎯 최종 솔루션: Dataset 기반 2-Phase 아키텍처

### 전체 파이프라인 구조

```
┌─────────────────────────────────────────────────────────┐
│  DAG 1: crawl_places (매일 자동 실행)                   │
│                                                          │
│  [크롤링 Task]                                           │
│      ↓                                                   │
│      모든 장소 크롤링 (try-except로 개별 에러 처리)      │
│      ↓                                                   │
│      NDJSON 파일로 저장                                  │
│      ↓                                                   │
│  🔵 Dataset 업데이트: place_obj_list.ndjson             │
└──────────────────────────────────┬──────────────────────┘
                                   │
                    (Dataset 변경 감지 → 자동 트리거)
                                   │
                                   ↓
┌─────────────────────────────────────────────────────────┐
│  DAG 2: process_places (자동 실행) - 2단계 파이프라인   │
│                                                          │
│  [데이터 로드 Task]                                      │
│      ↓                                                   │
│  [주소 변환 Stage] (카카오 API)                          │
│    ├─ convert[0] convert[1] ... convert[N]              │
│      ↓                                                   │
│  [API 전송 Stage] (Stroll API)                          │
│    ├─ send[0] send[1] ... send[N]                       │
│      ↓                                                   │
│  [결과 요약 Task]                                        │
└─────────────────────────────────────────────────────────┘
```

### Phase 구분

**Phase 1 - 데이터 수집 (crawl_places)**:

- 스케줄: 매일 1회 자동 실행
- 역할: 크롤링 → 파일 저장 → Dataset 업데이트
- 독립 실행 가능

**Phase 2 - 데이터 처리 (process_places)**:

- 스케줄: Dataset 변경 시 자동 실행
- 역할: 파일 로드 → 병렬 처리 → 결과 요약
- 수동 재실행 가능 (크롤링 없이)

---

## 🔧 구현 상세

### 0. Dataset 정의

두 DAG 모두 동일한 Dataset을 참조합니다:

```python
from airflow.datasets import Dataset

# 크롤링 결과 파일을 Dataset으로 정의
places_dataset = Dataset("file://dags/stroll/crawl/tmp/place_obj_list.ndjson")
```

### 1. 크롤링 단계 개선 (`crawl.py`)

**변경 전**:

```python
for li_element in li_elements:
    # 예외 처리 없음
    place_obj_list.append(place_obj)
export_ndjson(place_obj_list)  # 마지막에만 저장
```

**변경 후**:

```python
all_place_obj_list = []
success_count = 0
fail_count = 0

for li_element in li_elements:
    try:
        # 크롤링 로직
        place_obj = {...}
        all_place_obj_list.append(place_obj)
        success_count += 1
        print(f"✓ 크롤링 성공: {title.text}")
    except Exception as e:
        fail_count += 1
        print(f"✗ 크롤링 실패 (장소 #{success_count + fail_count}): {e}")
        continue  # 다음 장소로 계속 진행

# 백업용 NDJSON 저장
export_ndjson(all_place_obj_list)

# XCom으로 장소 리스트 반환 (Dynamic Task Mapping에서 사용)
return all_place_obj_list
```

**개선 사항**:

- ✅ 각 장소별 try-except 처리
- ✅ 하나 실패해도 계속 진행
- ✅ 성공/실패 카운트 로깅
- ✅ 장소 리스트 반환으로 다음 단계 연결

---

### 2. 처리 모듈 생성 (`process_place.py`)

주소 변환과 API 전송을 **분리된 함수**로 구현 (모니터링 용이):

```python
def convert_address_only(place_obj):
    """
    단일 장소의 주소만 변환합니다 (카카오 API)

    Returns: 변환된 place_obj 또는 에러 정보
    """
    try:
        # 주소 변환 로직
        road_address = convert_to_road_address(base_address)
        if road_address is None:
            raise Exception(f"주소 변환 실패 (카카오 API): {base_address}")

        place_obj["address"] = road_address
        place_obj["detailAddress"] = detail_address
        place_obj["status"] = "converted"

        return place_obj
    except Exception as e:
        return {
            "status": "failed",
            "placeName": place_name,
            "error": str(e),
            "stage": "address_conversion"  # 실패 단계 표시
        }


def send_to_api_only(place_obj):
    """
    변환된 장소를 API로 전송합니다 (Stroll API)

    Returns: 전송 결과 또는 에러 정보
    """
    # 이전 단계 실패 시 스킵
    if place_obj.get("status") == "failed":
        place_obj["stage"] = "api_send (skipped)"
        return place_obj

    try:
        result = send_to_api(place_obj)
        return {
            "status": "success",
            "placeName": place_name,
            "result": result
        }
    except Exception as e:
        return {
            "status": "failed",
            "placeName": place_name,
            "error": str(e),
            "stage": "api_send"  # 실패 단계 표시
        }
```

**핵심 포인트**:

- **2개 함수로 분리**: 주소 변환 vs API 전송
- **Stage 표시**: 어느 API에서 실패했는지 명확히 기록
- **이전 실패 전파**: convert 실패 시 send는 스킵
- **독립적 모니터링**: 각 API 호출을 별도로 추적 가능

---

### 3. DAG 1 - 크롤링 DAG (`crawl_places_dag.py`)

**Phase 1: 데이터 수집**

```python
from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from stroll.crawl.crawl import crawl

# Dataset 정의
places_dataset = Dataset("file://dags/stroll/crawl/tmp/place_obj_list.ndjson")

with DAG(
    dag_id="crawl_places",
    schedule_interval=timedelta(days=1),  # 매일 실행
    tags=["stroll", "crawl", "phase-1"],
    ...
) as dag:

    @task(
        task_id="crawl_and_save",
        outlets=[places_dataset]  # Dataset 업데이트 선언
    )
    def crawl_and_save_task():
        """
        크롤링 후 Dataset 업데이트
        완료되면 process_places DAG가 자동으로 트리거됨
        """
        places = crawl(None)  # 내부에서 NDJSON 파일 저장

        print(f"📦 크롤링 완료: {len(places)}개 장소")
        print(f"다음 단계: process_places DAG가 자동으로 시작됩니다")

        return {"total_crawled": len(places)}

    crawl_and_save_task()
```

**핵심 기능**:

- **`outlets=[places_dataset]`**: 이 Task가 Dataset을 업데이트함을 선언
- **자동 트리거**: 파일이 저장되면 process_places DAG가 자동 실행
- **독립 실행**: 수동으로 실행하거나 스케줄에 따라 실행 가능

---

### 4. DAG 2 - 처리 DAG (`process_places_dag.py`)

**Phase 2: 데이터 처리 (2단계 파이프라인)**

```python
from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from stroll.crawl.process_place import convert_address_only, send_to_api_only

# 동일한 Dataset 참조
places_dataset = Dataset("file://dags/stroll/crawl/tmp/place_obj_list.ndjson")

with DAG(
    dag_id="process_places",
    schedule=[places_dataset],  # Dataset 업데이트 시 자동 실행!
    max_active_tasks=10,  # 동시 처리 최대 10개
    tags=["stroll", "process", "phase-2"],
    ...
) as dag:

    @task(task_id="load_places")
    def load_places_task():
        """크롤링된 장소 데이터를 파일에서 로드"""
        places = []
        with open("dags/stroll/crawl/tmp/place_obj_list.ndjson", 'r') as f:
            for line in f:
                places.append(json.loads(line))

        print(f"📂 로드된 장소: {len(places)}개")
        return places

    @task(task_id="convert_address")
    def convert_address_task(place_obj: dict):
        """단일 장소 주소 변환 (카카오 API)"""
        return convert_address_only(place_obj)

    @task(task_id="send_to_api")
    def send_to_api_task(place_obj: dict):
        """변환된 장소 API 전송 (Stroll API)"""
        return send_to_api_only(place_obj)

    @task(task_id="summarize_results")
    def summarize_task(results: list):
        """모든 장소 처리 결과 요약 (API별 실패 통계 포함)"""
        success_count = sum(1 for r in results if r.get("status") == "success")
        fail_count = sum(1 for r in results if r.get("status") == "failed")

        # API별 실패 집계
        conversion_failures = sum(1 for r in results if r.get("stage") == "address_conversion")
        api_failures = sum(1 for r in results if r.get("stage") == "api_send")

        print(f"✓ 성공: {success_count}개")
        print(f"✗ 실패: {fail_count}개")
        print(f"  └─ 주소 변환 실패 (카카오 API): {conversion_failures}개")
        print(f"  └─ API 전송 실패 (Stroll API): {api_failures}개")

        return {
            "success": success_count,
            "failed": fail_count,
            "conversion_failures": conversion_failures,
            "api_failures": api_failures
        }

    # 2단계 Dynamic Task Mapping
    places = load_places_task()
    converted = convert_address_task.expand(place_obj=places)  # 1단계: 주소 변환
    results = send_to_api_task.expand(place_obj=converted)     # 2단계: API 전송
    summarize_task(results)
```

**핵심 기능**:

- **`schedule=[places_dataset]`**: Dataset이 업데이트되면 자동 실행
- **2단계 `.expand()`**: 주소 변환과 API 전송을 별도 Stage로 분리
- **API별 모니터링**: 카카오 API vs Stroll API 실패를 독립적으로 추적
- **병렬 처리**: max_active_tasks=10으로 동시 처리
- **개별 재시도**: retries=2로 실패한 장소만 재시도
- **독립 실행**: 크롤링 없이 처리만 재실행 가능

---

## 📊 개선 효과 비교

| 항목               | 이전 방식 (단일 DAG)           | 새로운 방식 (Dataset 기반 2 DAGs) |
| ------------------ | ------------------------------ | --------------------------------- |
| **실패 처리**      | 하나 실패 = 전체 중단          | 실패한 것만 재시도                |
| **재시도**         | 전체 다시 시작 (시간 낭비)     | 개별 장소만 재시도                |
| **병렬 처리**      | ❌ 순차 처리                   | ✅ 최대 10개 동시                 |
| **처리 속도**      | 느림 (순차)                    | 빠름 (병렬)                       |
| **모니터링**       | ❌ 어디서 실패했는지 불명확    | ✅ UI에서 장소별로 확인           |
| **API별 추적**     | ❌ 통합 로그만                 | ✅ 카카오 API vs Stroll API 분리  |
| **부분 성공**      | ❌ 전체 롤백                   | ✅ 성공한 것은 저장               |
| **안정성**         | 낮음 (하나의 에러로 전체 실패) | 높음 (독립적 처리)                |
| **유지보수**       | 어려움                         | 쉬움 (장소별 로그 확인)           |
| **관심사 분리**    | ❌ 크롤링+처리 혼재            | ✅ 명확한 Phase 구분              |
| **재실행**         | 전체 재실행 필요               | 처리만 재실행 가능                |
| **의존성 관리**    | 암묵적 (코드로만 확인)         | ✅ Dataset으로 시각화             |
| **스케줄 분리**    | ❌ 하나의 스케줄               | ✅ 독립적 스케줄 가능             |
| **에러 원인 파악** | 어려움 (통합 로그)             | ✅ Stage별 명확한 에러 메시지     |

---

## 🎯 주요 학습 포인트

### 1. Airflow의 올바른 활용

**잘못된 사용**:

```python
# Airflow를 단순 스케줄러로만 사용
def big_function():
    for item in items:
        process(item)  # 모든 처리를 하나의 Task에서
```

**올바른 사용**:

```python
# Dynamic Task Mapping으로 세분화
items = get_items()
process_task.expand(item=items)  # 각 item이 독립 Task
```

### 2. Dataset 기반 데이터 파이프라인

**핵심 개념**: 데이터 변경이 워크플로우를 트리거

```python
# Producer DAG
@task(outlets=[my_dataset])
def create_data():
    save_file()  # Dataset 업데이트

# Consumer DAG
with DAG(schedule=[my_dataset]):  # 자동 트리거
    process_data()
```

**장점**:

- 📊 데이터 중심 사고 (Data as a Product)
- 🔄 느슨한 결합 (Loosely Coupled)
- 👁️ 의존성 시각화 (UI에서 확인 가능)

### 3. 하이브리드 접근법

**핵심 원칙**:

- **기술적 제약**이 있는 부분은 Python적 처리
- **독립적으로 실행 가능**한 부분은 Airflow Dynamic Mapping
- **워크플로우 단위**는 Dataset으로 연결

**예시**:

- Selenium 크롤링: Python 예외 처리 (세션 유지 필요)
- 데이터 처리/전송: Airflow Mapping (독립 실행 가능)
- DAG 간 연결: Dataset (자동 트리거)

### 4. 실패 처리 전략

**계층적 에러 처리**:

1. **개별 항목 레벨**: try-except로 계속 진행 (crawl.py)
2. **Task 레벨**: Airflow retry 메커니즘 (process_place)
3. **DAG 레벨**: 전체 워크플로우 모니터링
4. **Phase 레벨**: 실패한 Phase만 재실행 (process_places만 재실행)

### 5. 관심사의 분리 (Separation of Concerns)

**Phase 1 (crawl_places)**:

- 책임: 외부 데이터 수집
- 스케줄: 시간 기반 (매일)
- 출력: Dataset

**Phase 2 (process_places)**:

- 책임: 데이터 처리 및 적재
- 스케줄: 이벤트 기반 (Dataset)
- 입력: Dataset

---

## 🔍 Airflow UI 모니터링

### 변경 전 (3개 Task, 단일 DAG)

```
DAG: crawl_and_rag
├─ ✓ crawl_task
├─ ✗ address_conversion_task  ← 어느 장소에서 실패했는지 알 수 없음
└─ ⊗ send_to_stroll_api_task  ← 실행조차 안 됨
```

### 변경 후 (2개 DAG, Dataset 연결)

#### DAG 1: crawl_places

```
DAG: crawl_places (매일 자동 실행)
└─ ✓ crawl_and_save [성공]
    └─ 🔵 Dataset 업데이트: place_obj_list.ndjson
```

#### DAG 2: process_places (자동 트리거, 2단계 파이프라인)

```
DAG: process_places (Dataset 트리거로 자동 실행)
├─ ✓ load_places [성공]
│
├─ 🔵 Stage 1: 주소 변환 (카카오 API) - 100개 Dynamic Tasks
│   ├─ ✓ convert_address[0] [성공]
│   ├─ ✗ convert_address[1] [실패] ← 카카오 API 에러
│   ├─ ✓ convert_address[2] [성공]
│   └─ ... (나머지 97개)
│       ↓
├─ 🟢 Stage 2: API 전송 (Stroll API) - 100개 Dynamic Tasks
│   ├─ ✓ send_to_api[0] [성공]
│   ├─ ⊗ send_to_api[1] [스킵] ← 이전 단계 실패로 스킵
│   ├─ ✗ send_to_api[2] [실패] ← Stroll API 타임아웃
│   └─ ... (나머지 97개)
│
└─ ✓ summarize_results [성공]
```

#### Dataset 의존성 그래프 (UI에서 시각화)

```
┌────────────────┐       🔵 Dataset        ┌─────────────────┐
│ crawl_places   │  ───► place_obj_list  ──►│ process_places  │
│   (Producer)   │         .ndjson          │   (Consumer)    │
└────────────────┘                          └─────────────────┘
```

**장점**:

- ✅ 어떤 장소가 실패했는지 명확히 확인
- ✅ 어떤 API에서 실패했는지 Stage별로 구분 (카카오 vs Stroll)
- ✅ 실패한 장소만 클릭해서 로그 확인 가능
- ✅ 재시도 진행 상황 실시간 모니터링
- ✅ Dataset 의존성을 UI에서 시각적으로 확인
- ✅ 크롤링과 처리를 독립적으로 모니터링
- ✅ 처리 DAG만 재실행 가능 (크롤링 생략)
- ✅ API별 성공/실패율 통계 제공

---

## 💾 파일 구조

```
dags/
├── crawl_places_dag.py              # 신규: Phase 1 - 크롤링 DAG
├── process_places_dag.py            # 신규: Phase 2 - 처리 DAG
└── stroll/
    └── crawl/
        ├── crawl.py                  # 크롤링 (예외 처리 강화)
        ├── process_place.py          # 신규: 통합 처리 모듈 (변환+전송)
        └── tmp/
            └── place_obj_list.ndjson # Dataset 파일 (자동 생성)

archive/
├── stroll_crawl_and_rag.py.bak      # 구버전: 단일 DAG (백업)
├── convert_address.py.bak           # 구버전: 주소 변환 모듈 (통합됨)
└── send_to_stroll_api.py.bak        # 구버전: API 전송 모듈 (통합됨)
```

### 파일 역할

| 파일                        | 역할                                          | Phase | 상태        |
| --------------------------- | --------------------------------------------- | ----- | ----------- |
| `crawl_places_dag.py`       | 크롤링 DAG, Dataset 생산자                    | 1     | ✅ 사용 중  |
| `process_places_dag.py`     | 처리 DAG, Dataset 소비자                      | 2     | ✅ 사용 중  |
| `crawl.py`                  | 실제 크롤링 로직 (Selenium)                   | 1     | ✅ 사용 중  |
| `process_place.py`          | 단일 장소 처리 로직 (주소 변환+API 전송 통합) | 2     | ✅ 사용 중  |
| `place_obj_list.ndjson`     | Dataset 파일 (DAG 간 데이터 전달)             | 1→2   | ✅ 자동생성 |
| ~~`convert_address.py`~~    | ~~주소 변환 유틸리티~~                        | -     | ⚠️ 백업됨   |
| ~~`send_to_stroll_api.py`~~ | ~~API 전송 유틸리티~~                         | -     | ⚠️ 백업됨   |

---

## 🚀 향후 개선 가능 사항

1. **실패 재처리 DAG**: 실패한 장소만 모아서 재처리하는 Phase 3 DAG
2. **알림 시스템**: 실패율이 임계값을 넘으면 Slack/Email 알림
3. **동적 병렬도 조정**: 시스템 부하에 따라 max_active_tasks 자동 조정
4. **배치 처리**: 너무 많은 Task 생성 방지를 위한 배치 그룹핑
5. **데이터 검증**: 각 단계에서 데이터 품질 검증 추가
6. **다중 Dataset**: 성공/실패 데이터를 별도 Dataset으로 분리
7. **SLA 모니터링**: 각 Phase의 처리 시간 추적 및 알림
8. **증분 처리**: 변경된 장소만 재처리하는 로직

---

## 📚 참고 자료

- [Airflow Dynamic Task Mapping 공식 문서](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)

---

## ✅ 결론

단순히 "에러 처리를 추가하자"는 접근이 아니라, **Airflow의 최신 기능(Dataset)과 모범 사례(Dynamic Task Mapping)를 활용**하여 아키텍처를 재설계함으로써:

### 달성한 개선 사항

1. ✅ **안정성** 향상: 부분 실패에도 전체 파이프라인 유지
2. ✅ **성능** 향상: 병렬 처리로 처리 시간 단축
3. ✅ **관찰성** 향상: 장소별 상태를 UI에서 쉽게 확인
4. ✅ **유지보수성** 향상: 문제 발생 시 빠른 원인 파악
5. ✅ **유연성** 향상: 각 Phase를 독립적으로 실행/재시도 가능
6. ✅ **확장성** 향상: Dataset을 통한 명확한 의존성 관리

### 핵심 교훈

1. **도구의 특성 이해**: Airflow의 Dataset, Dynamic Task Mapping 같은 고급 기능 활용
2. **데이터 중심 설계**: 데이터 흐름을 중심으로 워크플로우 설계
3. **관심사의 분리**: Phase별로 명확한 책임 분리
4. **점진적 개선**: Python → Airflow → Dataset으로 단계적 발전

### 최종 아키텍처 요약

```
Phase 1: 데이터 수집
[crawl_places DAG] → 🔵 Dataset → Phase 2: 데이터 처리 (2단계)
                                    [process_places DAG]
                                    ↓
                                    Stage 1: convert_address[N개]
                                    ↓
                                    Stage 2: send_to_api[N개]
                                    ↓
                                    Summarize
```

**핵심**: 단순히 도구를 사용하는 것이 아니라, 도구의 철학과 패턴을 이해하고 적용하는 것이 중요하다.
