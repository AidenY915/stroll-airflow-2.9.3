### 실행 방법

1. 환경 준비

```bash
docker compose config    # .env 반영 확인
```

2. DB 초기화(progres_data) + 관리자 계정 생성

```bash
docker compose run --rm airflow-init
```

3. Airflow 실행

```bash
docker compose up -d scheduler webserver
```

접속

URL: http://localhost:5000

### .env 예시

```
# Database
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Airflow
AIRFLOW_EXECUTOR=LocalExecutor
AIRFLOW_IMAGE=stroll-airflow
AIRFLOW_PORT=5000
```
