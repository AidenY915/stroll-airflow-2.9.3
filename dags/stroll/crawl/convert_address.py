from stroll.crawl.send_to_stroll_api import send_place_to_api
import requests
import re
import os
from dotenv import load_dotenv

DIRECTORY_PATH = __file__[0:__file__.rfind(os.sep)]
load_dotenv(DIRECTORY_PATH + os.sep + '.env', override=True)
KAKAO_API_KEY = os.getenv("KAKAO_API_KEY")

def convert_to_road_address(addr):
    url = f"https://dapi.kakao.com/v2/local/search/address.json?query={addr}"
    headers = {
        "Authorization": f"KakaoAK {KAKAO_API_KEY}"  # 여기에 본인의 REST API 키 삽입
    }

    try:
        res = requests.get(url, headers=headers)
        data = res.json()
        
        # documents 리스트에서 첫 번째 결과 추출
        documents = data.get("documents", [])
        if not documents:
            return None # 검색 결과 없음

        first = documents[0]

        # 도로명 주소 우선, 없으면 지번 주소
        if first.get("road_address"):
            return first["road_address"].get("address_name")
        elif first.get("address"):
            return first["address"].get("address_name")
        else:
            return None
    except Exception as e:
        print(f"주소 변환 중 오류 발생: {e}")
        return None, None, None


def strip_detail_address(addr):
    match = re.search(r'^([\w\s가-힣·\-]+?\s\d+(-\d+)?)(?=\s|$)', addr)
    return match.group(1) if match else addr

def extract_detail_address(addr):
    # 도로명 + 건물번호까지만 매칭
    match = re.search(r'^([\w\s가-힣·\-]+?\s\d+(-\d+)?)(?=\s|$)', addr)
    if match:
        base_addr = match.group(1)
        detail = addr.replace(base_addr, '', 1).strip()
        return detail
    return ""  # 못 찾으면 상세주소도 없다고 판단


def convert_address(place_obj):
    address = place_obj["address"]
    detail_address = extract_detail_address(address)
    rest_address = strip_detail_address(address)
    road_address = convert_to_road_address(rest_address)
    if road_address is None:
        raise Exception("주소 변환 실패, or 카카오 API 인증 실패(IP or API key)")
    place_obj["detailAddress"] = detail_address
    place_obj["address"] = road_address
    send_place_to_api(place_obj)