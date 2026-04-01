import polars as pl
import os
from datetime import datetime
from dotenv import load_dotenv
from minio import Minio
import io
from job_2_silver_transform import SILVER_PATH

load_dotenv("/home/ray/.env")

minio_client = Minio(
    "minio:9000",
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=False
)

BUCKET = os.getenv("BUCKET", "chess-data")
GOLD_PATH = f"gold/train_{datetime.now().strftime('%Y%m%d')}.parquet"

def read_from_minio(bucket, path):
    obj = minio_client.get_object(bucket, path)
    return pl.read_parquet(io.BytesIO(obj.read()))

def run():
    df = read_from_minio(
        BUCKET,
        SILVER_PATH
    )

    df = df.with_columns([
        (pl.col("white_elo") - pl.col("black_elo")).alias("elo_diff"),

        (pl.col("white_elo") > pl.col("black_elo"))
        .cast(pl.Int8)
        .alias("white_advantage_numeric"),

        pl.when(pl.col("white_elo") > pl.col("black_elo"))
        .then(1)
        .when(pl.col("white_elo") < pl.col("black_elo"))
        .then(-1)
        .otherwise(0)
        .alias("white_advantage_tri"),

        pl.when(pl.col("ply_count") < 20).then(pl.lit("opening"))
        .when(pl.col("ply_count") < 60).then(pl.lit("middlegame"))
        .otherwise(pl.lit("endgame"))
        .alias("game_phase"),

        pl.when(pl.col("ply_count") < 30).then(pl.lit("short"))
        .when(pl.col("ply_count") < 70).then(pl.lit("medium"))
        .otherwise(pl.lit("long"))
        .alias("game_length_cat")
    ])

    buffer = io.BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)

    minio_client.put_object(
        BUCKET,
        GOLD_PATH,
        buffer,
        length=buffer.getbuffer().nbytes
    )

    print("🥇 Uploaded Gold to MinIO")

if __name__ == "__main__":
    run()