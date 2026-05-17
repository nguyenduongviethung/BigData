import os
import json
import tempfile
import s3fs
import mlflow
from mlflow.tracking import MlflowClient

# 1. Cấu hình môi trường
os.environ["MLFLOW_TRACKING_URI"] = "http://mlflow.local"
RAW_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio.default.svc.cluster.local:9000")

MODEL_NAME = "Chess_AlphaZero_Model"
ALIASES_TO_KEEP = ["champion", "challenger"]
BASE_S3_DIR = "chess-data/models/alpha_zero_chess_run" # Thư mục gốc chứa các Trainer trên MinIO

def perform_cleanup():
    client = MlflowClient()
    print("🧹 BẮT ĐẦU QUY TRÌNH DỌN DẸP HỆ THỐNG MLOPS...")

    try:
        versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    except Exception as e:
        print(f"❌ Không tìm thấy model {MODEL_NAME} trên MLflow: {e}")
        return

    protected_s3_trainers = set()
    versions_to_delete = []

    # BƯỚC 1: Sàng lọc trên MLflow và tìm đường dẫn vật lý cần bảo vệ
    for v in versions:
        has_important_alias = any(a in ALIASES_TO_KEEP for a in v.aliases)
        
        if has_important_alias:
            print(f"🛡️ PHÁT HIỆN BẢO VẬT: Version {v.version} mang nhãn {v.aliases}")
            try:
                # Trích xuất đường dẫn MinIO từ hộ chiếu JSON của mô hình
                local_path = mlflow.artifacts.download_artifacts(
                    run_id=v.run_id,
                    artifact_path="alphazero_pointer/checkpoint_metadata.json"
                )
                with open(local_path, "r") as f:
                    meta = json.load(f)
                    uri = meta.get("minio_checkpoint_uri", "")
                    
                    # Cắt chuỗi để lấy đúng tên thư mục TorchTrainer (Ví dụ: TorchTrainer_d99d8...)
                    if BASE_S3_DIR in uri:
                        relative_path = uri.split(BASE_S3_DIR + "/")[1]
                        trainer_dir_name = relative_path.split("/")[0] 
                        protected_s3_trainers.add(trainer_dir_name)
                        print(f"   => Khóa bảo vệ thư mục MinIO: {trainer_dir_name}")
            except Exception as e:
                print(f"⚠️ Cảnh báo: Lỗi khi đọc metadata của version {v.version}: {e}")
        else:
            versions_to_delete.append(v)

    # BƯỚC 2: Rút giấy phép các mô hình bị đào thải trên MLflow
    for v in versions_to_delete:
        print(f"🗑️ Gỡ bỏ Version {v.version} khỏi MLflow Registry...")
        client.delete_model_version(MODEL_NAME, v.version)
        # Tùy chọn: client.delete_run(v.run_id) để xóa sạch cả tracking logs nếu muốn.

    # BƯỚC 3: Dọn dẹp không gian vật lý (Ổ cứng MinIO)
    print("\n🧽 Đang quét ổ cứng MinIO để xóa Checkpoint...")
    s3 = s3fs.S3FileSystem(
        key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        client_kwargs={'endpoint_url': f"http://{RAW_ENDPOINT}"}
    )
    
    try:
        all_dirs = s3.ls(BASE_S3_DIR) # Liệt kê tất cả các thư mục trong alpha_zero_chess_run
        
        for d in all_dirs:
            dir_name = d.split("/")[-1]
            
            # Nếu nó là thư mục sinh ra do huấn luyện VÀ KHÔNG nằm trong danh sách bảo vệ
            if dir_name.startswith("TorchTrainer_") and dir_name not in protected_s3_trainers:
                print(f"🔥 Đang đốt tàn dư vật lý: s3://{d}")
                s3.rm(d, recursive=True)
                
    except Exception as e:
        print(f"⚠️ Lỗi kết nối MinIO khi dọn dẹp: {e}")

    print("✅ QUÁ TRÌNH DỌN DẸP HOÀN TẤT! Ổ cứng của bạn đã được giải phóng.")

if __name__ == "__main__":
    perform_cleanup()