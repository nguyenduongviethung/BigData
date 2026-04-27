import ray
import io
import os
import chess.pgn
import polars as pl
from minio import Minio
from dotenv import load_dotenv
from job_1_ingestion_bronze import BRONZE_PREFIX

load_dotenv("/home/ray/.env")

# Khởi tạo MinIO client (Vẫn cần để duyệt và đọc file từ Raw/Bronze)
minio_client = Minio(
    "minio:9000",
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=False
)

BUCKET = os.getenv("BUCKET", "chess-data")
# 🚀 URI thay đổi: Chuyển từ việc chỉ định tên file (.parquet) sang chỉ định tên thư mục (Delta Table)
SILVER_URI = f"s3://{BUCKET}/silver/clean_games"

@ray.remote
def process_object(object_name):
    try:
        client = Minio(
            "minio:9000",
            access_key=os.getenv("MINIO_ROOT_USER"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
            secure=False
        )

        resp = client.get_object(BUCKET, object_name)
        raw = resp.read().decode()

        username = object_name.split("_")[0].split("/")[-1]

        game = chess.pgn.read_game(io.StringIO(raw))
        if not game:
            return None

        # 🚀 SỬ DỤNG EXPORTER ĐỂ TRÍCH XUẤT CHUỖI NƯỚC ĐI SẠCH
        # headers=False để không lưu lại các thông tin White, Black, Date... (đã có ở các cột khác)
        # variations=False, comments=False để loại bỏ rác nếu có
        exporter = chess.pgn.StringExporter(headers=False, variations=False, comments=False)
        moves_string = game.accept(exporter)

        return {
            "username": username,
            "white": game.headers.get("White"),
            "black": game.headers.get("Black"),
            "white_elo": int(game.headers.get("WhiteElo", 0) or 0),
            "black_elo": int(game.headers.get("BlackElo", 0) or 0),
            "result": game.headers.get("Result"),
            "ply_count": len(list(game.mainline_moves())),
            "time_control": game.headers.get("TimeControl"),
            "moves_string": moves_string  # <-- CỘT MỚI: Chứa toàn bộ nước đi của ván cờ
        }

    except Exception as e:
        print(f"❌ Error parsing {object_name}: {e}")
        return None
def run():
    objects = list(
        minio_client.list_objects(BUCKET, prefix=BRONZE_PREFIX, recursive=True)
    )

    if not objects:
        print("⚠️ No data in Bronze zone to process.")
        return

    futures = [
        process_object.remote(obj.object_name)
        for obj in objects
    ]

    results = ray.get(futures)
    results = [r for r in results if r]

    if not results:
        print("⚠️ All parsed results were empty.")
        return

    df = pl.DataFrame(results)

    # 🚀 CẤU HÌNH DELTA LAKE KẾT NỐI VỚI MINIO (S3 API)
    storage_options = {
        "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ROOT_USER"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_ROOT_PASSWORD"),
        "AWS_ENDPOINT_URL": "http://minio:9000",
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true" # Flag cực kỳ quan trọng khi dùng MinIO thay cho AWS S3 thật
    }

    print(f"Bắt đầu ghi DataFrame ({df.height} dòng) vào Delta Table...")
    
    # 🚀 GHI DELTA TABLE
    # mode="append" giúp thêm dữ liệu của batch mới vào bảng có sẵn thay vì ghi đè mất dữ liệu cũ
    df.write_delta(
        target=SILVER_URI,
        mode="append",
        storage_options=storage_options,
        delta_write_options={"schema_mode": "merge"} # Tự động thêm cột mới nếu data sau này có cấu trúc thay đổi
    )

    print(f"🥈 Đã tải thành công Delta Table tại: {SILVER_URI}")


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