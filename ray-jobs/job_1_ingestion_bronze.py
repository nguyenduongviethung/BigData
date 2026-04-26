import os
import io
import requests
from datetime import datetime
from minio import Minio
from dotenv import load_dotenv
import ray

load_dotenv("/home/ray/.env")

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
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*"
    }

    r = requests.get(url, headers=headers, timeout=15)

    print("DOWNLOAD:", username, r.status_code, len(r.text))

    if r.status_code != 200:
        return None

    return r.text


def upload_to_bronze(username, pgn_text):
    minio_client = Minio(
        "minio:9000",
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False
    )

    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    path = f"{BRONZE_PREFIX}{username}_{ts}.pgn"

    minio_client.put_object(
        BUCKET,
        path,
        data=io.BytesIO(pgn_text.encode()),
        length=len(pgn_text)
    )

    return path


@ray.remote
def process_user(username, year, month):
    try:
        pgn = download_raw_pgn(username, year, month)

        if not pgn:
            return f"❌ No data: {username}"

        path = upload_to_bronze(username, pgn)

        return f"🥉 Bronze saved: {path}"

    except Exception as e:
        return f"❌ Error {username}: {e}"


def run():
    users = get_top_players()
    print("USERS =", users)

    year = datetime.now().year
    month = datetime.now().month

    futures = [
        process_user.remote(u, year, month)
        for u in users
    ]

    results = ray.get(futures)

    for r in results:
        print(r)


if __name__ == "__main__":
    ray.init(address="ray://ray-head:10001")

    run()

    ray.shutdown()