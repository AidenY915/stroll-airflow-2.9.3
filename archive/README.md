# Archive - 백업 파일

이 폴더는 더 이상 사용하지 않는 구버전 파일들을 보관합니다.

## 파일 목록

### `stroll_crawl_and_rag.py.bak`

- **원래 이름**: `stroll_crawl_and_rag.py`
- **이동 날짜**: 2025년 10월 12일
- **이동 이유**: Dataset 기반 2-Phase 아키텍처로 전환
- **대체 파일**:
  - `dags/crawl_places_dag.py` (Phase 1: 크롤링)
  - `dags/process_places_dag.py` (Phase 2: 처리)
- **설명**: 단일 DAG 방식의 구버전 파일. Dynamic Task Mapping은 적용되었으나 Dataset 분리 이전 버전입니다.

### `convert_address.py.bak`

- **원래 이름**: `dags/stroll/crawl/convert_address.py`
- **이동 날짜**: 2025년 10월 12일
- **이동 이유**: process_place.py로 기능 통합
- **대체 파일**: `dags/stroll/crawl/process_place.py` (주소 변환 기능 포함)
- **설명**: 주소 변환 전용 모듈. 카카오 API를 사용한 지번 → 도로명 주소 변환 및 상세주소 추출 기능.

### `send_to_stroll_api.py.bak`

- **원래 이름**: `dags/stroll/crawl/send_to_stroll_api.py`
- **이동 날짜**: 2025년 10월 12일
- **이동 이유**: process_place.py로 기능 통합
- **대체 파일**: `dags/stroll/crawl/process_place.py` (API 전송 기능 포함)
- **설명**: Stroll API 전송 전용 모듈. 장소 데이터를 백엔드 API로 전송하는 기능.

## 아키텍처 변경 내역

### 구버전 (3개 모듈 분리)

```
crawl.py → convert_address.py → send_to_stroll_api.py
```

### 신버전 (통합 모듈)

```
crawl.py → process_place.py (변환+전송 통합)
```

## 참고

이 폴더의 파일들은 Airflow가 스캔하지 않으므로 DAG로 인식되지 않습니다.
참고용으로만 보관하며, 필요시 삭제해도 무방합니다.
