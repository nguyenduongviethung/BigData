import ray
import io
import os
import numpy as np
import polars as pl
import chess.pgn
from minio import Minio
from dotenv import load_dotenv

# Import hàm trích xuất Tensor từ module chúng ta vừa viết
from utils_tensor import process_game_to_training_data

load_dotenv("/home/ray/.env")

BUCKET = os.getenv("BUCKET", "chess-data")
SILVER_URI = f"s3://{BUCKET}/silver/clean_games"
GOLD_TENSOR_PREFIX = "gold/tensors/"

@ray.remote
def process_and_save_chunk(chunk_id: int, pgn_strings: list):
    """
    Worker function: Xử lý một lô các ván cờ và lưu thành 1 file .npz trên MinIO.
    """
    # Khởi tạo MinIO client bên trong mỗi worker
    minio_client = Minio(
        "minio:9000",
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False
    )

    all_states = []
    all_policies = []
    all_values = []

    for pgn_str in pgn_strings:
        if not pgn_str:
            continue
            
        game = chess.pgn.read_game(io.StringIO(pgn_str))
        if not game:
            continue
        
        # Trích xuất tensors cho từng nước đi trong ván
        samples = process_game_to_training_data(game)
        for s in samples:
            all_states.append(s["state"])
            all_policies.append(s["policy"])
            all_values.append(s["value"])

    if not all_states:
        return f"⚠️ Chunk {chunk_id}: Không có dữ liệu hợp lệ."

    # Chuyển list thành ma trận NumPy để tối ưu bộ nhớ
    X = np.array(all_states, dtype=np.int8)        # Shape: (N, 12, 8, 8)
    Y_policy = np.array(all_policies, dtype=np.int32) # Shape: (N,)
    Y_value = np.array(all_values, dtype=np.float32)  # Shape: (N,)

    # Nén các ma trận vào một bộ đệm (buffer)
    buffer = io.BytesIO()
    np.savez_compressed(buffer, states=X, policies=Y_policy, values=Y_value)
    buffer.seek(0)

    # Đẩy thẳng file .npz lên MinIO
    object_name = f"{GOLD_TENSOR_PREFIX}tensor_chunk_{chunk_id}.npz"
    minio_client.put_object(
        BUCKET,
        object_name,
        buffer,
        length=buffer.getbuffer().nbytes
    )
    
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
    
    print("⏳ Đang đọc dữ liệu ván cờ từ Silver Zone...")
    df = pl.read_delta(SILVER_URI, storage_options=storage_options)
    
    # Lấy danh sách PGN (cột mà chúng ta đã thêm ở Bước 2)
    if "moves_string" not in df.columns:
        raise ValueError("Không tìm thấy cột 'moves_string'. Hãy chắc chắn Job 2 đã chạy phiên bản mới nhất!")
        
    pgn_list = df["moves_string"].to_list()
    
    # Chia nhỏ dữ liệu (Chunking) để phân tán cho các Ray worker. 
    # Ví dụ: Mỗi chunk chứa 100 ván cờ (sẽ sinh ra khoảng 6000-8000 tensors).
    chunk_size = 100
    chunks = [pgn_list[i:i + chunk_size] for i in range(0, len(pgn_list), chunk_size)]
    
    print(f"🚀 Phân phát {len(chunks)} chunks cho Ray Cluster xử lý...")
    
    # Kích hoạt tính toán song song
    futures = [process_and_save_chunk.remote(i, chunk) for i, chunk in enumerate(chunks)]
    
    # Chờ và in kết quả
    results = ray.get(futures)
    for r in results:
        print(r)
        
    print("🥇 Hoàn tất sinh dữ liệu Tensor cho AI!")

if __name__ == "__main__":
    # Lấy đường dẫn tuyệt đối của thư mục chứa code hiện tại (thư mục ray-jobs)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Khởi tạo Ray kèm theo runtime_env để đồng bộ code sang các Worker
    ray.init(
        address="ray://ray-head:10001",
        runtime_env={"working_dir": current_dir}
    )
    
    run()
    
    ray.shutdown()