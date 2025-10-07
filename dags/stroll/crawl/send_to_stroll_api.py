import requests
import os
from dotenv import load_dotenv

DIRECTORY_PATH = __file__[0:__file__.rfind(os.sep)]
load_dotenv(DIRECTORY_PATH + os.sep + '.env', override=True)
CRUD_API_URL = os.getenv("CRUD_API_URL")
CRUD_API_ID = os.getenv("CRUD_API_ID")
CRUD_API_PASSWORD = os.getenv("CRUD_API_PASSWORD")


def get_access_token():
    url = CRUD_API_URL + "/api/auth/login"
    body = { "userId" : CRUD_API_ID, "password" : CRUD_API_PASSWORD }
    response = requests.post(url, json=body)
    access_token = response.json()["accessToken"]
    print(CRUD_API_ID, CRUD_API_PASSWORD, access_token)
    return access_token

access_token = get_access_token()

def send_place_to_api(place_obj):
    # api 호출
    global access_token
    place_name = place_obj["placeName"]
    category = place_obj["category"]
    address = place_obj["address"]
    detail_address = place_obj["detailAddress"]
    address = place_obj["address"]
    # images = place_obj["images"]

    url = CRUD_API_URL + "/api/place"
    print(url, "in send_place_to_api")
    headers = {
            "Authorization": f"Bearer {access_token}"
    }
    data = {
        "placeName": place_name,
        "category": category,
        "address": address,
        "detailAddress": detail_address,
        "content": "",
    }
    try :   
        response = requests.post(url, headers=headers, data=data, files=None)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"Place 전송 중 오류 발생: {e}")
        access_token = get_access_token()
        response = requests.post(url, headers=headers, data=data, files=None)
    print(response , place_obj, "in send_place_to_api")
    return response.json()