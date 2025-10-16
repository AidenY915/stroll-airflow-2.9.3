import re
import requests
import os
from dotenv import load_dotenv

DIRECTORY_PATH = __file__[0:__file__.rfind(os.sep)]
load_dotenv(DIRECTORY_PATH + os.sep + '.env', override=True)

KAKAO_API_KEY = os.getenv("KAKAO_API_KEY")
CRUD_API_URL = os.getenv("CRUD_API_URL")
CRUD_API_ID = os.getenv("CRUD_API_ID")
CRUD_API_PASSWORD = os.getenv("CRUD_API_PASSWORD")

# 전역 액세스 토큰 (재사용)
_access_token = None

def get_access_token():
    """Stroll API에서 액세스 토큰을 발급받습니다."""
    global _access_token
    url = CRUD_API_URL + "/api/auth/login"
    body = {"userId": CRUD_API_ID, "password": CRUD_API_PASSWORD}
    response = requests.post(url, json=body)
    _access_token = response.json()["accessToken"]
    print(f"액세스 토큰 발급 완료: {CRUD_API_ID}")
    return _access_token


def convert_to_road_address(addr):
    """카카오 API를 사용하여 지번 주소를 도로명 주소로 변환합니다."""
    url = f"https://dapi.kakao.com/v2/local/search/address.json?query={addr}"
    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}

    try:
        res = requests.get(url, headers=headers)
        data = res.json()
        
        documents = data.get("documents", [])
        if not documents:
            return None

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
        return None


def strip_detail_address(addr):
    """주소에서 기본 주소(도로명 + 건물번호)만 추출합니다."""
    match = re.search(r'^([\w\s가-힣·\-]+?\s\d+(-\d+)?)(?=\s|$)', addr)
    return match.group(1) if match else addr


def extract_detail_address(addr):
    """주소에서 상세주소(층, 호수 등)를 추출합니다."""
    match = re.search(r'^([\w\s가-힣·\-]+?\s\d+(-\d+)?)(?=\s|$)', addr)
    if match:
        base_addr = match.group(1)
        detail = addr.replace(base_addr, '', 1).strip()
        return detail
    return ""


def send_to_api(place_obj):
    """장소 데이터를 Stroll API로 전송합니다 (이미지 포함)."""
    global _access_token
    
    if _access_token is None:
        get_access_token()
    
    url = CRUD_API_URL + "/api/place"
    headers = {"Authorization": f"Bearer {_access_token}"}
    data = {
        "placeName": place_obj["placeName"],
        "category": place_obj["category"],
        "address": place_obj["address"],
        "detailAddress": place_obj["detailAddress"],
        "content": place_obj.get("content", ""),
    }
    
    # 이미지 파일 준비
    files = []
    file_objects = []
    
    if place_obj.get("imageFile"):
        # imageFile이 문자열인 경우 배열로 변환
        image_files = place_obj["imageFile"]
        if isinstance(image_files, str):
            image_files = [image_files]
        
        # 각 이미지 파일 처리
        for image_filename in image_files:
            try:
                image_path = DIRECTORY_PATH + f'/tmp/images/{image_filename}'
                if os.path.exists(image_path):
                    # 파일 열기 (바이너리 읽기 모드)
                    file_obj = open(image_path, 'rb')
                    file_objects.append(file_obj)
                    files.append(('imgs', (image_filename, file_obj, 'image/jpeg'))) #request.post가 파일을 처리하는 순서 방식
                    print(f"  📎 이미지 첨부: {image_filename}")
                else:
                    print(f"  ⚠️ 이미지 파일 없음: {image_path}")
            except Exception as e:
                print(f"  ⚠️ 이미지 파일 열기 실패: {e}")
        print(f"files: {files}")
    # files가 비어있으면 None으로 설정
    if not files:
        files = None
    
    try:
        response = requests.post(url, headers=headers, data=data, files=files)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        # 토큰 만료 시 재발급 후 재시도
        print(f"API 전송 실패, 토큰 재발급 후 재시도: {e}")
        get_access_token()
        headers = {"Authorization": f"Bearer {_access_token}"}
        response = requests.post(url, headers=headers, data=data, files=files)
        response.raise_for_status()
        return response.json()
    finally:
        # 열린 파일 닫기
        for file_obj in file_objects:
            try:
                file_obj.close()
            except:
                pass


def convert_address_only(place_obj):
    place_name = place_obj.get("placeName", "Unknown")
    
    try:
        # 1. 주소 분리
        original_address = place_obj["address"]
        detail_address = extract_detail_address(original_address)
        base_address = strip_detail_address(original_address)
        
        # 2. 주소 변환 (카카오 API)
        road_address = convert_to_road_address(base_address)
        
        if road_address is None:
            raise Exception(f"주소 변환 실패 (카카오 API): {base_address}")
        
        # 3. place_obj 업데이트
        place_obj["address"] = road_address
        place_obj["detailAddress"] = detail_address
        place_obj["status"] = "converted"  # 변환 완료 표시
        
        print(f"✓ 주소 변환 완료: {place_name} → {road_address}")
        return place_obj
        
    except Exception as e:
        error_msg = str(e)
        print(f"✗ 주소 변환 실패: {place_name} - {error_msg}")
        return {
            "status": "failed",
            "placeName": place_name,
            "error": error_msg,
            "stage": "address_conversion"
        }


def send_to_api_only(place_obj):
    place_name = place_obj.get("placeName", "Unknown")
    
    # 이전 단계에서 실패한 경우 전파
    if place_obj.get("status") == "failed":
        print(f"⊗ API 전송 스킵: {place_name} (이전 단계 실패)")
        place_obj["stage"] = "api_send (skipped)"
        return place_obj
    
    try:
        # API 전송 (Stroll API)
        result = send_to_api(place_obj)
        
        print(f"✓ API 전송 완료: {place_name}")
        return {
            "status": "success",
            "placeName": place_name,
            "result": result
        }
        
    except Exception as e:
        error_msg = str(e)
        print(f"✗ API 전송 실패: {place_name} - {error_msg}")
        return {
            "status": "failed",
            "placeName": place_name,
            "error": error_msg,
            "stage": "api_send"
        }

def send_chunk_to_api(place_chunk_list):
    return [send_to_api_only(place_obj) for place_obj in place_chunk_list]

def process_single_place(place_obj):
    place_name = place_obj.get("placeName", "Unknown")
    
    try:
        # 1. 주소 분리
        original_address = place_obj["address"]
        detail_address = extract_detail_address(original_address)
        base_address = strip_detail_address(original_address)
        
        # 2. 주소 변환
        road_address = convert_to_road_address(base_address)
        
        if road_address is None:
            raise Exception(f"주소 변환 실패: {base_address}")
        
        # 3. place_obj 업데이트
        place_obj["address"] = road_address
        place_obj["detailAddress"] = detail_address
        
        # 4. API 전송
        result = send_to_api(place_obj)
        
        print(f"✓ 처리 완료: {place_name}")
        return {
            "status": "success",
            "placeName": place_name,
            "result": result
        }
        
    except Exception as e:
        error_msg = str(e)
        print(f"✗ 처리 실패: {place_name} - {error_msg}")
        return {
            "status": "failed",
            "placeName": place_name,
            "error": error_msg
        }


def process_chunk_of_places(place_chunk_list):
    return [process_single_place(place_obj) for place_obj in place_chunk_list]