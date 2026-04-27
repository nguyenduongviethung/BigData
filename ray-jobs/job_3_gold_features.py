import polars as pl
import ray
import os
from dotenv import load_dotenv

load_dotenv("/home/ray/.env")

BUCKET = os.getenv("BUCKET", "chess-data")

# Chỉ định URI dạng thư mục cho Delta Lake
SILVER_URI = f"s3://{BUCKET}/silver/clean_games"
GOLD_URI = f"s3://{BUCKET}/gold/train_features"

def run():
    # Cấu hình kết nối MinIO
    storage_options = {
        "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ROOT_USER"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_ROOT_PASSWORD"),
        "AWS_ENDPOINT_URL": "http://minio:9000",
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    print("⏳ Đang đọc dữ liệu từ Silver Zone (Delta Table)...")
    
    # Đọc Delta Table (Không cần dùng MinIO client)
    try:
        df = pl.read_delta(SILVER_URI, storage_options=storage_options)
    except Exception as e:
        raise RuntimeError(f"Không thể đọc Silver Delta Table. Chi tiết lỗi: {e}")

    print(f"🧠 Bắt đầu trích xuất đặc trưng cho {df.height} ván đấu...")
    
    # Logic Feature Engineering (Giữ nguyên của bạn)
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

    if "moves_string" in df.columns:
        df = df.drop("moves_string")

    print(f"💾 Đang ghi bảng Features ({df.height} dòng) vào Gold Zone...")
    
    df.write_delta(
        target=GOLD_URI,
        mode="overwrite", 
        storage_options=storage_options,
        delta_write_options={"schema_mode": "overwrite"}
    )

    print(f"🥇 Đã tải thành công Delta Table tại: {GOLD_URI}")

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