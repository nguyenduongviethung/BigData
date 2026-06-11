import ray
import io
import os
import numpy as np
import polars as pl
import chess.pgn
from minio import Minio
from datetime import datetime
from dotenv import load_dotenv
from utils_tensors import process_game_to_training_data

load_dotenv("/home/ray/.env")

BUCKET = os.getenv("BUCKET", "chess-data")
SILVER_URI = f"s3://{BUCKET}/silver/clean_games"
GOLD_TENSOR_PREFIX = "gold/tensors/"

@ray.remote
def process_and_save_chunk(run_id: str, chunk_index: int, pgn_strings: list):
    minio_client = Minio(
        "minio:9000",
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False
    )

    all_states, all_policies, all_values = [], [], []

    for pgn_str in pgn_strings:
        try:
            if not pgn_str: continue
            game = chess.pgn.read_game(io.StringIO(pgn_str))
            if not game: continue

            samples = process_game_to_training_data(game)
            for s in samples:
                all_states.append(s["state"])
                all_policies.append(s["policy"])
                all_values.append(s["value"])
        except Exception as e:
            print(f"⚠️ Lỗi khi xử lý game: {e}")
            continue
    if not all_states:
        return (
            f"⚠️ Chunk {chunk_index}: "
            f"Không sinh được sample nào."
        )

    X = np.array(all_states, dtype=np.int8)        
    Y_policy = np.array(all_policies, dtype=np.int32) 
    Y_value = np.array(all_values, dtype=np.float32)
    
    buffer = io.BytesIO()
    np.savez_compressed(buffer, states=X, policies=Y_policy, values=Y_value)
    buffer.seek(0)

    # 🚀 TÊN FILE THÔNG MINH: Kết hợp ID của mẻ chạy (Batch) và số thứ tự
    object_name = f"{GOLD_TENSOR_PREFIX}{run_id}_chunk_{chunk_index}.npz"
    minio_client.put_object(BUCKET, object_name, buffer, length=buffer.getbuffer().nbytes)
    
    return f"✅ Đã lưu {object_name} (Kích thước: {X.shape[0]} samples)"

def run():
    storage_options = {
        "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ROOT_USER"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_ROOT_PASSWORD"),
        "AWS_ENDPOINT_URL": "http://minio:9000",
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }
    
    minio_client = Minio(
        "minio:9000",
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False
    )
    
    print("⏳ Đang đọc toàn bộ Silver Table...")

    df = pl.read_delta(
        SILVER_URI,
        storage_options=storage_options
    )

    if df.height == 0:
        print("⚠️ Silver rỗng.")
        return

    print(
        f"📦 Tìm thấy {df.height} game trong Silver."
    )

    pgn_list = df["full_pgn"].to_list()
    chunk_size = 100
    chunks = [pgn_list[i:i + chunk_size] for i in range(0, len(pgn_list), chunk_size)]

    objects = list(minio_client.list_objects(
        BUCKET,
        prefix=GOLD_TENSOR_PREFIX,
        recursive=True
    ))

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"🚀 Bắt đầu xử lý với Run ID: {run_id}")
    
    futures = [process_and_save_chunk.remote(run_id, i, chunk) for i, chunk in enumerate(chunks)]
    
    results = ray.get(futures)
    for r in results:
        print(r)
    
    print("🎉 Hoàn thành xử lý và lưu tensors!")

    for obj in objects:
        minio_client.remove_object(
            BUCKET,
            obj.object_name
        )
    print("🧹 Đã xóa tất cả tensors cũ trong MinIO.")


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    ray.init(address="ray://ray-head:10001", runtime_env={"working_dir": current_dir})
    run()
    ray.shutdown()