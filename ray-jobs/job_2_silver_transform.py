import ray
import io
import os
import chess.pgn
import polars as pl
from minio import Minio
from dotenv import load_dotenv
from job_1_ingestion_bronze import BRONZE_PREFIX

load_dotenv("/home/ray/.env")

minio_client = Minio(
    "minio:9000",
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=False
)

BUCKET = os.getenv("BUCKET", "chess-data")
SILVER_PATH = "silver/clean_games.parquet"

@ray.remote
def process_object(object_name):
    try:
        from minio import Minio
        import os
        import chess.pgn
        import io

        minio_client = Minio(
            "minio:9000",
            access_key=os.getenv("MINIO_ROOT_USER"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
            secure=False
        )

        resp = minio_client.get_object(BUCKET, object_name)
        raw = resp.read().decode()

        username = object_name.split("_")[0].split("/")[-1]

        game = chess.pgn.read_game(io.StringIO(raw))
        if not game:
            return None

        return {
            "username": username,
            "white": game.headers.get("White"),
            "black": game.headers.get("Black"),
            "white_elo": int(game.headers.get("WhiteElo", 0)),
            "black_elo": int(game.headers.get("BlackElo", 0)),
            "result": game.headers.get("Result"),
            "ply_count": len(list(game.mainline_moves())),
            "time_control": game.headers.get("TimeControl")
        }

    except Exception as e:
        return None


def run():
    objects = list(
        minio_client.list_objects(BUCKET, prefix=BRONZE_PREFIX, recursive=True)
    )

    futures = [
        process_object.remote(obj.object_name)
        for obj in objects
    ]

    results = ray.get(futures)
    results = [r for r in results if r]

    df = pl.DataFrame(results)

    buffer = io.BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)

    minio_client.put_object(
        BUCKET,
        SILVER_PATH,
        buffer,
        length=buffer.getbuffer().nbytes
    )

    print("🥈 Uploaded Silver to MinIO")

if __name__ == "__main__":
    ray.init(address="ray://ray-head:10001")
    run()
    ray.shutdown()