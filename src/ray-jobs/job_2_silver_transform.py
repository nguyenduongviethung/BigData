import ray
import io
import os
import chess.pgn
import polars as pl
from datetime import datetime # 🚀 Import thư viện thời gian
from minio import Minio
from minio.commonconfig import CopySource
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
SILVER_URI = f"s3://{BUCKET}/silver/clean_games"
ARCHIVE_PREFIX = "bronze/archive_pgn/"

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

        exporter = chess.pgn.StringExporter(headers=False, variations=False, comments=False)
        moves_string = game.accept(exporter)

        return {
            "source_file": object_name,
            "username": username,
            "white": game.headers.get("White"),
            "black": game.headers.get("Black"),
            "white_elo": int(game.headers.get("WhiteElo", 0) or 0),
            "black_elo": int(game.headers.get("BlackElo", 0) or 0),
            "result": game.headers.get("Result"),
            "ply_count": len(list(game.mainline_moves())),
            "time_control": game.headers.get("TimeControl"),
            "moves_string": moves_string,
            "eco": game.headers.get("ECO"),
            "eco_url": game.headers.get("ECOUrl"),
            "termination": game.headers.get("Termination")
        }
    except Exception as e:
        print(f"❌ Error parsing {object_name}: {e}")
        return None

def run():
    objects = list(minio_client.list_objects(BUCKET, prefix=BRONZE_PREFIX, recursive=True))

    if not objects:
        print("✅ Thư mục raw_pgn trống. Không có dữ liệu mới để xử lý.")
        return

    print(f"📦 Tìm thấy {len(objects)} file PGN mới. Bắt đầu phân tán tác vụ...")
    futures = [process_object.remote(obj.object_name) for obj in objects]

    results = ray.get(futures)
    results = [r for r in results if r]

    if not results:
        print("⚠️ Không có kết quả hợp lệ nào được parse.")
        return

    df = pl.DataFrame(results)

    # =========================================================================
    # 🚀 BƯỚC ĐỘT PHÁ: ĐÓNG DẤU THỜI GIAN (HIGH-WATER MARK)
    # Lưu dưới dạng chuỗi ISO (VD: "2026-05-31T08:47:54.123456") để dễ so sánh
    # =========================================================================
    current_time_iso = datetime.now().isoformat()
    df = df.with_columns(pl.lit(current_time_iso).alias("ingested_at"))
    
    storage_options = {
        "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ROOT_USER"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_ROOT_PASSWORD"),
        "AWS_ENDPOINT_URL": "http://minio:9000",
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    print(f"Bắt đầu ghi DataFrame ({df.height} dòng) vào Delta Table...")
    # Schema_mode="merge" sẽ tự động thêm cột `ingested_at` vào bảng cũ mà không gây lỗi
    df.write_delta(
        target=SILVER_URI,
        mode="append",
        storage_options=storage_options,
        delta_write_options={"schema_mode": "merge"} 
    )

    print(f"🧹 Bắt đầu dọn dẹp: Di chuyển {len(results)} file sang Archive...")
    success_count = 0
    for r in results:
        src_obj = r["source_file"]
        dest_obj = src_obj.replace(BRONZE_PREFIX, ARCHIVE_PREFIX) 
        try:
            minio_client.copy_object(BUCKET, dest_obj, CopySource(BUCKET, src_obj))
            minio_client.remove_object(BUCKET, src_obj)
            success_count += 1
        except Exception as e:
            print(f"⚠️ Lỗi khi di chuyển file {src_obj}: {e}")
            
    print(f"✅ Đã ghi nhãn thời gian '{current_time_iso}' và lưu trữ {success_count} file.")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    ray.init(address="ray://ray-head:10001", runtime_env={"working_dir": current_dir})
    run()
    ray.shutdown()