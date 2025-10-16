### 실행 방법

1. 환경 준비

```bash
docker compose -f docker-compose-LocalExecutor-stroll.yml config    # .env 반영 확인
```

2. DB 초기화(progres_data) + 관리자 계정 생성

```bash
docker compose -f docker-compose-LocalExecutor-stroll.yml run --rm airflow-init
```

3. Airflow 실행

```bash
docker compose -f docker-compose-LocalExecutor-stroll.yml up -d scheduler webserver
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

### chrome 설치

https://storage.googleapis.com/chrome-for-testing-public/140.0.7339.207/linux64/chromedriver-linux64.zip
https://storage.googleapis.com/chrome-for-testing-public/140.0.7339.207/linux64/chrome-linux64.zip

1. 위 두 링크에서 설치 후 압축 해제
2. ./chrome에 복사

(https://googlechromelabs.github.io/chrome-for-testing/#stable)
