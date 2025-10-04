from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import time

from dotenv import load_dotenv
import os

from stroll.crawl.convert_address import convert_address

CHROMEDRIVER_PATH = None
CHROME_PATH = None
driver = None
DIRECTORY_PATH = __file__[0:__file__.rfind(os.sep)]

def init():
    global driver
    global DIRECTORY_PATH
    global CHROMEDRIVER_PATH
    global CHROME_PATH

     # 이미지 저장을 위한 디렉토리 생성
    if not os.path.exists('./images'):
        os.makedirs('./images')

    # Airflow 환경에서는 .env 파일이 /usr/local/airflow/.env에 마운트됨
    load_dotenv(DIRECTORY_PATH + os.sep + '.env', override=True)
    

    CHROMEDRIVER_PATH = DIRECTORY_PATH + os.sep + os.getenv("CHROMEDRIVER_PATH")
    CHROME_PATH = DIRECTORY_PATH + os.sep + os.getenv("CHROME_PATH")
    print(CHROME_PATH)
    print(CHROMEDRIVER_PATH)

    # 옵션 설정
    options = Options()
    # options.add_argument("--headless")             # 브라우저 창 없이 실행
    options.add_argument("--no-sandbox")            # 보안 옵션 끔 (리눅스 환경 대비용)
    options.add_argument("--disable-dev-shm-usage") # 메모리 부족 방지
    options.binary_location = CHROME_PATH
    # 드라이버 경로 설정 (chromedriver.exe 위치에 맞게 수정) 
    service = Service(executable_path=CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(options = options, service=service)


def crawl():
    init()
    # 접속할 사이트 현재 좌표를 여기에 넣고 있음.
    url = "https://pcmap.place.naver.com/place/list?query=%EA%B0%95%EC%95%84%EC%A7%80&x=127.005941&y=37.268905&clientX=127.005941&clientY=37.268905&display=70&ts=1744774586148&additionalHeight=76&locale=ko&mapUrl=https%3A%2F%2Fmap.naver.com%2Fp%2Fsearch%2F%EA%B0%95%EC%95%84%EC%A7%80%2Fplace%2F12945929"  # 예: 해커뉴스
    driver.get(url)

    # 약간 대기 (동적 로딩 페이지 대비)
    time.sleep(3)

    # 원하는 요소 선택 (여기서는 뉴스 제목들)
    page_a_list = driver.find_elements(By.CSS_SELECTOR, '#app-root > div > div.XUrfU > div.zRM9F > a')
    for page_a in page_a_list:
        page_a.click()
        time.sleep(1)
        li_elements = driver.find_elements(By.CSS_SELECTOR, '#_pcmap_list_scroll_container > ul > li')
        
        for li_element in li_elements:
            title = li_element.find_element(By.CSS_SELECTOR, 'div span.YwYLL')
            if '입양' in title.text or '분양' in title.text:                        #입양 분양 제외
                continue
            category = li_element.find_element(By.CSS_SELECTOR, 'div span.YzBgS')
            address_a = li_element.find_element(By.CSS_SELECTOR, 'div a.uFxr1')
            time.sleep(0.5)
            address_a.click()
            time.sleep(0.5)
            address = li_element.find_element(By.CSS_SELECTOR, 'div span.hAvkz')

            place_obj = {
                "placeName": title.text,
                "category": category.text,
                "address": address.text,
            }
            
            convert_address(place_obj)
            
            #XCom place_obj 전달

            # does_image_exists = False  
            # img_title=str(place_no)+"_1.jpg"
            # #이미지 다운로드
            # try:
            #     img = li_element.find_element(By.CSS_SELECTOR, 'img')
            #     img_src = img.get_attribute("src").replace("type=f160_160", "type=w560_sharpen")
            #     if img_src:
            #         try:
            #             img_data = requests.get(img_src).content
            #             with open(f"C:/stroll_image/"+img_title, "wb") as f:
            #                 f.write(img_data)
            #             does_image_exists = True
            #         except Exception as e:
            #             print(f"Error downloading {img_src}: {e}")
            #         print(img_src)
            # except Exception as e:
            #     print(f"{title.text}은(는) 이미지 없음.")
            # #이미지 S3에 업로드 후 데이터 베이스 image 테이블에 추가해야함.

            # #데이터베이스에 장소 및 이미지 인스턴스 추가
            # finally:
                
            # if does_image_exists:
            #     print(place_no)
            #     # insert_image_to_database(place_no, img_title)
                
    # 종료
    driver.quit()


