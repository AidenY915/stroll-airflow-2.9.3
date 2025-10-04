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
AIRFLOW__WEBSERVER__SECRET_KEY=abcd
```

###dags/stroll/crawl/.env 예시

```
# Kakao API 설정
KAKAO_API_KEY=카카오 API

# MySQL 데이터베이스 설정  (RDS)
CRUD_API_URL = http://localhost:8080
CRUD_API_ID = 관리자계정
CRUD_API_PASSWORD = 관리자PW

# ChromeDriver 경로 설정
#리눅스
# CHROMEDRIVER_PATH=./chrome/chromedriver-linux64/chromedriver
# CHROME_PATH=./chrome/chrome-linux64/chrome
#윈도우
CHROMEDRIVER_PATH=chrome/chromedriver-win64/chromedriver.exe
CHROME_PATH=chrome/chrome-win64/chrome.exe
```

### chrome 설치

https://storage.googleapis.com/chrome-for-testing-public/140.0.7339.207/linux64/chromedriver-linux64.zip
https://storage.googleapis.com/chrome-for-testing-public/140.0.7339.207/linux64/chrome-linux64.zip

1. 위 두 링크에서 설치 후 압축 해제
2. .dags/crawl/chrome에 복사

(https://googlechromelabs.github.io/chrome-for-testing/#stable)

### logs 권한 변경

```
sudo chown -R 50000:0 logs
sudo chmod -R 775 logs
```

### chromedriver 권한

```
chmod +x dags/stroll/crawl/chrome/chromedriver-linux64/chromedriver
chmod +x dags/stroll/crawl/chrome/chrome-linux64/chrome
```
