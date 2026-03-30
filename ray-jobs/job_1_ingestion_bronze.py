import os
import io
import requests
import time
from datetime import datetime
from minio import Minio
from dotenv import load_dotenv
import ray

load_dotenv("/home/ray/.env")

minio_client = Minio(
    "minio:9000",
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=False
)

BUCKET = os.getenv("BUCKET", "chess-data")
BRONZE_PREFIX = "bronze/raw_pgn/"


def get_top_players(category="live_rapid", limit=3):
    url = "https://api.chess.com/pub/leaderboards"
    headers = {"User-Agent": "ChessPipeline/1.0"}

    try:
        resp = requests.get(url, headers=headers, timeout=10)

        if resp.status_code != 200:
            print("❌ API failed:", resp.status_code)
            return ["hikaru", "magnuscarlsen"]

        data = resp.json()

        players = data.get(category, [])[:limit]
        return [p["username"] for p in players]

    except Exception as e:
        print("❌ fallback due to error:", e)
        return ["hikaru", "magnuscarlsen"]

def download_raw_pgn(username, year, month):
    url = f"https://api.chess.com/pub/player/{username}/games/{year}/{month:02d}/pgn"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
        "Accept": "*/*",
        "Connection": "keep-alive"
    }

    r = requests.get(url, headers=HEADERS, timeout=10)

    print("DOWNLOAD:", username, r.status_code, len(r.text))

    if r.status_code != 200:
        return None

    return r.text

def upload_to_bronze(username, pgn_text):
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    path = f"{BRONZE_PREFIX}{username}_{ts}.pgn"

    minio_client.put_object(
        BUCKET,
        path,
        data=io.BytesIO(pgn_text.encode()),
        length=len(pgn_text)
    )

    return path

def run():
    users = get_top_players()
    print("USERS =", users)
    year = datetime.now().year
    month = datetime.now().month

    for u in users:
        pgn = download_raw_pgn(u, year, month)
        if pgn:
            path = upload_to_bronze(u, pgn)
            print(f"🥉 Bronze saved: {path}")
        time.sleep(1)

if __name__ == "__main__":
    ray.init(address="ray://ray-head:10001")
    run()
    ray.shutdown()