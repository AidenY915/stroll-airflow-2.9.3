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
# DIRECTORY = os.path.dirname(os.path.abspath(__file__))

def save_content_in_rds(tmp_path : str) -> None:
    conn = pymysql.connect(host=RDS_HOST, port=int(RDS_PORT), user=RDS_USER, password=RDS_PASSWORD, database=RDS_DATABASE)
    cursor = conn.cursor()
    for file in os.listdir(os.path.join(tmp_path, "filled-contents")):
        with open(os.path.join(tmp_path, "filled-contents", file), "r", encoding="utf-8") as f:
            record = json.loads(f.read())
            place_no = record["place_no"]
            content = record["content"]
            cursor.execute("UPDATE place SET content = %s WHERE place_no = %s", (content, place_no))
    conn.commit()
    cursor.close()
    conn.close()
    return

def main():
    save_content_in_rds()
# main()
