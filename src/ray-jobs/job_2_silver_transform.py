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
        try:
            raw = resp.read().decode()
        finally:
            resp.close()
            resp.release_conn()

        filename = os.path.basename(object_name)
        parts = filename.replace(".pgn", "").split("_")

        year = int(parts[-3])
        month = int(parts[-2])

        username = "_".join(parts[:-3])

        pgn_io = io.StringIO(raw)

        rows = []

        while True:
            game = chess.pgn.read_game(pgn_io)

            if game is None:
                break

            exporter = chess.pgn.StringExporter(
                headers=True,
                variations=False,
                comments=False
            )

            full_pgn = game.accept(exporter)

            link = game.headers.get("Link", "")

            if link:
                game_id = link.rstrip("/").split("/")[-1]
            else:
                continue


            game_date = (
                game.headers.get("UTCDate")
                or game.headers.get("Date")
            )

            if game_date:
                game_date = game_date.replace(".", "-")

            rows.append({
                "game_id": game_id,
                "game_date": game_date,

                "source_file": object_name,
                "username": username,

                "year": year,
                "month": month,

                "white": game.headers.get("White"),
                "black": game.headers.get("Black"),

                "white_elo": int(game.headers.get("WhiteElo", 0) or 0),
                "black_elo": int(game.headers.get("BlackElo", 0) or 0),

                "result": game.headers.get("Result"),

                "ply_count": len(list(game.mainline_moves())),

                "time_control": game.headers.get("TimeControl"),

                "full_pgn": full_pgn,

                "eco": game.headers.get("ECO"),
                "eco_url": game.headers.get("ECOUrl"),

                "termination": game.headers.get("Termination")
            })

        return rows

    except Exception as e:
        print(f"❌ Error parsing {object_name}: {e}")
        return []

def run():
    objects = list(minio_client.list_objects(BUCKET, prefix=BRONZE_PREFIX, recursive=True))

    if not objects:
        print("✅ Thư mục raw_pgn trống. Không có dữ liệu mới để xử lý.")
        return

    print(f"📦 Tìm thấy {len(objects)} file PGN mới. Bắt đầu phân tán tác vụ...")
    futures = [process_object.remote(obj.object_name) for obj in objects]

    nested_results = ray.get(futures)

    results = [
        row
        for file_rows in nested_results
        for row in file_rows
    ]

    if not results:
        print("⚠️ Không có kết quả hợp lệ nào được parse.")
        return

    df = pl.DataFrame(results)

    before = df.height

    df = df.unique(
        subset=["game_id"],
        keep="first"
    )

    print(
        f"🧹 Dedup nội bộ batch: "
        f"{before} -> {df.height}"
    )

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

    try:
        existing_ids = (
            pl.read_delta(
                SILVER_URI,
                storage_options=storage_options,
                columns=["game_id"]
            )
            .unique()
        )

        before = df.height

        df = df.join(
            existing_ids,
            on="game_id",
            how="anti"
        )

        print(
            f"🔍 Dedupe với Silver: "
            f"{before} -> {df.height}"
        )

    except Exception:
        print("ℹ️ Silver chưa tồn tại. Khởi tạo mới.")

    if df.height == 0:
        print("✅ Không có game mới.")
    
    else: 
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
    for obj in objects:
        src_obj = obj.object_name
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