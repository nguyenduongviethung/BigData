import ray
import io
import os
import numpy as np
import polars as pl
import chess.pgn
from datetime import datetime # 🚀 Import thư viện
from minio import Minio
from dotenv import load_dotenv
from utils_tensors import process_game_to_training_data

load_dotenv("/home/ray/.env")

BUCKET = os.getenv("BUCKET", "chess-data")
SILVER_URI = f"s3://{BUCKET}/silver/clean_games"
GOLD_TENSOR_PREFIX = "gold/tensors/"
CHECKPOINT_FILE = f"{GOLD_TENSOR_PREFIX}_checkpoint_time.txt" # 🚀 Đổi tên file checkpoint

@ray.remote
def process_and_save_chunk(batch_id: str, chunk_index: int, pgn_strings: list):
    minio_client = Minio(
        "minio:9000",
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False
    )

    all_states, all_policies, all_values = [], [], []

    for pgn_str in pgn_strings:
        if not pgn_str: continue
        game = chess.pgn.read_game(io.StringIO(pgn_str))
        if not game: continue
        
        samples = process_game_to_training_data(game)
        for s in samples:
            all_states.append(s["state"])
            all_policies.append(s["policy"])
            all_values.append(s["value"])

    if not all_states:
        return f"⚠️ Batch {batch_id} - Chunk {chunk_index}: Rỗng."

    X = np.array(all_states, dtype=np.int8)        
    Y_policy = np.array(all_policies, dtype=np.int32) 
    Y_value = np.array(all_values, dtype=np.float32)  

    buffer = io.BytesIO()
    np.savez_compressed(buffer, states=X, policies=Y_policy, values=Y_value)
    buffer.seek(0)

    # 🚀 TÊN FILE THÔNG MINH: Kết hợp ID của mẻ chạy (Batch) và số thứ tự
    object_name = f"{GOLD_TENSOR_PREFIX}batch_{batch_id}_chunk_{chunk_index}.npz"
    minio_client.put_object(BUCKET, object_name, buffer, length=buffer.getbuffer().nbytes)
    
    return f"✅ Đã lưu {object_name} (Kích thước: {X.shape[0]} samples)"


# 🚀 CÁC HÀM QUẢN LÝ CHECKPOINT (DỰA TRÊN THỜI GIAN)
def get_last_watermark(client):
    try:
        resp = client.get_object(BUCKET, CHECKPOINT_FILE)
        return resp.read().decode('utf-8')
    except Exception:
        return "1970-01-01T00:00:00.000000" # Mốc thời gian mặc định xa xưa

def save_new_watermark(client, timestamp_str):
    data = timestamp_str.encode('utf-8')
    client.put_object(BUCKET, CHECKPOINT_FILE, io.BytesIO(data), len(data))


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
    
    # 🚀 BƯỚC 1: Lấy mốc thời gian lần chạy trước
    last_watermark = get_last_watermark(minio_client)
    print(f"📍 High-Water Mark hiện tại: {last_watermark}")
    
    print("⏳ Đang quét bảng Silver...")
    df = pl.read_delta(SILVER_URI, storage_options=storage_options)
    
    # Bắt lỗi an toàn nếu Job 2 chưa kịp chạy bản mới tạo ra cột này
    if "ingested_at" not in df.columns:
        raise ValueError("Chưa tìm thấy cột 'ingested_at'. Bạn cần chạy Job 2 phiên bản mới trước!")

    # 🚀 BƯỚC 2: Chỉ lọc những dòng sinh ra sau mốc last_watermark
    new_df = df.filter(pl.col("ingested_at") > pl.lit(last_watermark))
    
    if new_df.height == 0:
        print("✅ Không có dữ liệu mới. Bỏ qua trích xuất Tensor.")
        return
        
    print(f"📦 Phát hiện {new_df.height} ván cờ mới. Tiến hành trích xuất...")
    
    pgn_list = new_df["moves_string"].to_list()
    chunk_size = 100
    chunks = [pgn_list[i:i + chunk_size] for i in range(0, len(pgn_list), chunk_size)]
    
    # Tạo Batch ID dựa trên thời gian thực tại để đặt tên file
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print(f"🚀 Phân phát {len(chunks)} chunks cho cụm Ray (Batch: {batch_id})...")
    futures = [process_and_save_chunk.remote(batch_id, i, chunk) for i, chunk in enumerate(chunks)]
    
    results = ray.get(futures)
    for r in results:
        print(r)
        
    # 🚀 BƯỚC 3: Cập nhật Checkpoint bằng thời gian lớn nhất (mới nhất) trong mẻ dữ liệu vừa xử lý
    max_timestamp_in_batch = new_df["ingested_at"].max()
    save_new_watermark(minio_client, max_timestamp_in_batch)
    print(f"🥇 Hoàn tất! Đã kéo High-Water Mark lên: {max_timestamp_in_batch}")


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    ray.init(address="ray://ray-head:10001", runtime_env={"working_dir": current_dir})
    run()
    ray.shutdown()