import pymysql
import json
import os
from dotenv import load_dotenv


load_dotenv()
RDS_HOST = os.getenv("RDS_HOST")
RDS_PORT = os.getenv("RDS_PORT")
RDS_USER = os.getenv("RDS_USER")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")
RDS_DATABASE = os.getenv("RDS_DATABASE")

def select_places_from_rds(tmp_path : str) -> None:
    conn = pymysql.connect(host=RDS_HOST, port=int(RDS_PORT), user=RDS_USER, password=RDS_PASSWORD, database=RDS_DATABASE)
    cursor = conn.cursor()
    cursor.execute("SELECT place_no, title, gu_address, after_gu_address FROM place WHERE content =''")
    places = cursor.fetchall()
    place_obj_list = []
    for place in places:
        place_obj = {
            "place_no": place[0],
            "title": place[1],
            "address": place[2] + " " + place[3]
        }
        place_obj_list.append(place_obj)
    print(f"총 {len(place_obj_list)}개의 장소가 선택되었습니다.")
    cursor.close()
    conn.close()
    with open(os.path.join(tmp_path, "empty_places.ndjson"), "w", encoding="utf-8") as f:
        for place_obj in place_obj_list:
            f.write(json.dumps(place_obj, ensure_ascii=False) + "\n")
    return

def main():
    select_places_from_rds(RDS_HOST, RDS_PORT, RDS_USER, RDS_PASSWORD, RDS_DATABASE)
# main()