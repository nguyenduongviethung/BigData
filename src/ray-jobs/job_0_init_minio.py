from minio import Minio
import os
from dotenv import load_dotenv

load_dotenv("/home/ray/.env")

BUCKET = os.getenv("BUCKET", "chess-data")

client = Minio(
    "minio:9000",
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=False
)

def init_bucket():
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
        print(f"🪣 Created bucket: {BUCKET}")
    else:
        print(f"✔ Bucket already exists: {BUCKET}")

if __name__ == "__main__":
    init_bucket()