import os
import tempfile
import torch
import torch.nn as nn
import torch.optim as optim
import s3fs
import pyarrow.fs
import mlflow
from mlflow.tracking import MlflowClient
import json
import ray
from ray import train
from ray.train import ScalingConfig, RunConfig, CheckpointConfig, Checkpoint
from ray.train.torch import TorchTrainer
from torch.utils.data import DataLoader
from dotenv import load_dotenv

# Đọc file .env ngay từ đầu
current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(current_dir, ".env"))

# Import cấu trúc mạng và lớp đọc dữ liệu đặc thù của dự án
from model import ChessAlphaZeroNet
from dataset import ChessMinIODataset


def train_loop_per_worker(config):
    """
    HÀM THỰC THI TRÊN TỪNG WORKER NODE (PHÂN TÁN)
    """
    dataset_uris = config.get("dataset_uris", [])
    if not dataset_uris:
        raise ValueError("❌ Không tìm thấy danh sách dữ liệu (dataset_uris) trong config!")
    
    key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    secret = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    mini_batch_size = config.get("batch_size", 256)

    # 1. Khởi tạo dữ liệu
    dataset = ChessMinIODataset(file_uris=dataset_uris, access_key=key, secret_key=secret)
    dataloader = DataLoader(dataset, batch_size=1, shuffle=True)
    dataloader = train.torch.prepare_data_loader(dataloader)
    
    # 2. Khởi tạo mô hình
    model = ChessAlphaZeroNet()
    parent_uri = config.get("parent_checkpoint_uri")
    
    if parent_uri:
        print(f"⬇️ Đang tải trọng số từ MinIO: {parent_uri}")
        
        # 1. Khởi tạo lại kết nối MinIO ngay bên trong Worker
        raw_endpoint = os.getenv("MINIO_ENDPOINT", "minio.default.svc.cluster.local:9000")
        s3 = s3fs.S3FileSystem(
            key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            secret=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
            client_kwargs={'endpoint_url': f"http://{raw_endpoint}"}
        )
        
        # 2. Xóa tiền tố s3:// (nếu có) để s3fs đọc chuẩn xác
        clean_path = parent_uri.replace("s3://", "")
        weight_file_path = f"{clean_path}/model_latest.pt"
        
        # 3. Đọc thẳng file .pt từ MinIO vào bộ nhớ (Không cần tải xuống ổ cứng tạm)
        try:
            with s3.open(weight_file_path, "rb") as f:
                model.load_state_dict(torch.load(f, map_location="cpu"))
            print("✅ Đã nạp thành công bộ não tiền bối [@champion]!")
        except Exception as e:
            print(f"❌ Lỗi khi nạp file trọng số từ MinIO: {e}")
            raise e
    
    # 3. Cấu hình tối ưu hóa
    optimizer = optim.Adam(model.parameters(), lr=config["lr"])
    policy_criterion = nn.CrossEntropyLoss()
    value_criterion = nn.MSELoss()
    
    # 4. Vòng lặp huấn luyện
    for epoch in range(config["num_epochs"]):
        model.train()
        total_loss = 0.0
        total_mini_batches = 0
        
        for chunk_idx, (chunk_states, chunk_policies, chunk_values) in enumerate(dataloader):
            chunk_states = chunk_states.view(-1, 12, 8, 8)
            chunk_policies = chunk_policies.view(-1)
            chunk_values = chunk_values.view(-1)
            
            num_states_in_chunk = chunk_states.size(0)
            
            for i in range(0, num_states_in_chunk, mini_batch_size):
                mb_states = chunk_states[i : i + mini_batch_size]
                mb_policies = chunk_policies[i : i + mini_batch_size]
                mb_values = chunk_values[i : i + mini_batch_size]
                
                optimizer.zero_grad()
                pred_policies, pred_values = model(mb_states)
                
                loss_policy = policy_criterion(pred_policies, mb_policies)
                loss_value = value_criterion(pred_values.squeeze(-1), mb_values)
                loss = loss_policy + loss_value
                
                loss.backward()
                optimizer.step()
                
                total_loss += loss.item()
                total_mini_batches += 1

        avg_loss = total_loss / total_mini_batches if total_mini_batches > 0 else float('inf')
        metrics = {"epoch": epoch, "loss": avg_loss}
        
        # 5. Lưu Checkpoint 
        with tempfile.TemporaryDirectory() as temp_ckpt_dir:
            checkpoint_path = os.path.join(temp_ckpt_dir, "model_latest.pt")
            if hasattr(model, "module"):
                state_dict = model.module.state_dict()
            else:
                state_dict = model.state_dict()
                
            torch.save(state_dict, checkpoint_path)
            train.report(metrics, checkpoint=Checkpoint.from_directory(temp_ckpt_dir))

def run_distributed_training(num_workers: int, use_gpu: bool, batch_size: int, lr: float, epochs: int):
    """
    HÀM ĐIỀU PHỐI TRUNG TÂM (ĐIỀU HÀNH TẠI DRIVER NODE)
    """
    print("⚙️ Đang thiết lập hạ tầng kết nối lưu trữ phân tán...")
    
    raw_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    s3_fs = s3fs.S3FileSystem(
        key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        client_kwargs={'endpoint_url': f"http://{raw_endpoint}"}
    )
    custom_minio_fs = pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(s3_fs))
    
    print("🔍 Đang quét danh sách dữ liệu cờ trên MinIO...")
    data_dir = "chess-data/gold/tensors/" 
    try:
        raw_files = s3_fs.ls(data_dir, detail=True) # detail=True để lấy thời gian
        
        # Chỉ lấy file .npz
        npz_files = [f for f in raw_files if f['name'].endswith('.npz')]
        
        # Sắp xếp file theo thời gian tạo (Mới nhất lên đầu)
        npz_files.sort(key=lambda x: x['LastModified'], reverse=True)
        
        # 🚀 CỬA SỔ TRƯỢT: Chỉ lấy 100 file mới nhất
        WINDOW_SIZE = 50
        recent_files = npz_files[:WINDOW_SIZE]
        
        dataset_uris = [f"s3://{f['name']}" for f in recent_files]
        print(f"✅ Đã chọn {len(dataset_uris)} file dữ liệu mới nhất để học.")
        
    except Exception as e:
        print(f"⚠️ Lỗi quét dữ liệu: {e}")
        dataset_uris = []

    scaling_config = ScalingConfig(
        num_workers=num_workers,
        use_gpu=use_gpu,
        resources_per_worker={"CPU": 1} if not use_gpu else {"CPU": 1, "GPU": 1}
    )

    os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("MINIO_ROOT_USER", "admin")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_ROOT_PASSWORD", "admin123")
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = f"http://{raw_endpoint}"
    
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-service:5000"))
    mlflow.set_experiment("ChessAlphaZero_System")
    
    with mlflow.start_run() as run:
        mlflow.log_params({
            "num_workers": num_workers, "use_gpu": use_gpu,
            "batch_size": batch_size, "learning_rate": lr, "epochs": epochs
        })
        
        # 🚀 CHUYỂN ĐỔI: Sử dụng Alias @champion thay cho Stage Production
        client = MlflowClient()
        parent_checkpoint_uri = None
        
        try:
            # Truy vấn trực tiếp mô hình đang giữ danh hiệu champion
            prod_version = client.get_model_version_by_alias("Chess_AlphaZero_Model", "champion")
            
            local_json_path = mlflow.artifacts.download_artifacts(
                run_id=prod_version.run_id,
                artifact_path="alphazero_pointer/checkpoint_metadata.json" 
            )
            with open(local_json_path, "r") as f:
                metadata = json.load(f)
                parent_checkpoint_uri = metadata.get("minio_checkpoint_uri")
                
            print(f"📦 Đã tìm thấy bộ não tiền bối (Version {prod_version.version}) đang mang nhãn [@champion].")
            
        except mlflow.exceptions.RestException:
            # Lỗi 404 (Không tìm thấy) sẽ văng ra khi alias chưa từng được tạo
            print("💡 Hệ thống chưa có mô hình [@champion]. Sẽ khởi tạo ma trận trọng số ngẫu nhiên ban đầu.")
        except Exception as e:
            print(f"⚠️ Lỗi kết nối khi nạp mô hình cũ: {e}. Sẽ khởi tạo ngẫu nhiên để không sập tiến trình.")
        
        # Khởi tạo Trainer
        trainer = TorchTrainer(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config={
                "batch_size": batch_size, "lr": lr, "num_epochs": epochs,
                "parent_checkpoint_uri": parent_checkpoint_uri,
                "dataset_uris": dataset_uris
            }, 
            scaling_config=scaling_config,
            run_config=RunConfig(
                storage_path="chess-data/models", 
                name="alpha_zero_chess_run", 
                storage_filesystem=custom_minio_fs, 
                checkpoint_config=CheckpointConfig(
                    num_to_keep=1, checkpoint_score_attribute="loss", checkpoint_score_order="min"
                )
            )
        )
        
        result = trainer.fit()
        final_loss = result.metrics.get("loss", float('inf'))
        print(f"🎉 Hoàn thành huấn luyện! Chỉ số Loss tốt nhất đạt: {final_loss}")
        mlflow.log_metric("final_loss", final_loss)
        
        # Đăng ký mô hình lên MLflow
        if result.checkpoint:
            print(f"💾 File trọng số tối ưu nhất được lưu tại: {result.checkpoint.path}")
            
            with tempfile.TemporaryDirectory() as temp_dir:
                metadata_file = os.path.join(temp_dir, "checkpoint_metadata.json")
                with open(metadata_file, "w") as f:
                    json.dump({"minio_checkpoint_uri": result.checkpoint.path}, f)
                
                mlmodel_file = os.path.join(temp_dir, "MLmodel")
                with open(mlmodel_file, "w") as f:
                    f.write("artifact_path: alphazero_pointer\nflavors:\n  python_function:\n    loader_module: mlflow.pyfunc\n")
                
                mlflow.log_artifacts(temp_dir, artifact_path="alphazero_pointer")
            
            try:
                model_uri = f"runs:/{run.info.run_id}/alphazero_pointer"
                model_version = mlflow.register_model(model_uri=model_uri, name="Chess_AlphaZero_Model")
                print(f"🏆 Đã ghi danh thành công phiên bản: [{model_version.version}]")
                
                # 🚀 CHUYỂN ĐỔI: Gắn nhãn @challenger bằng API Alias mới nhất
                client.set_registered_model_alias(
                    name="Chess_AlphaZero_Model",
                    alias="challenger",
                    version=model_version.version
                )
                print(f"⚔️ Mô hình Version {model_version.version} đã sẵn sàng với nhãn [@challenger] để thách đấu!")
                
            except Exception as e:
                print(f"⚠️ Cảnh báo Registry: {e}")
    return result

if __name__ == "__main__":
    # ray.init(address="auto") 
    
    # 1. Hỏi xem hệ thống có GPU không
    _use_gpu = torch.cuda.is_available()
    
    # 2. Khảo sát tài nguyên THỰC TẾ của cụm Ray
    cluster_resources = ray.cluster_resources()
    total_cpus = int(cluster_resources.get("CPU", 1))
    total_gpus = int(cluster_resources.get("GPU", 0))

    # 3. Tính toán linh hoạt (Dành ra 1 CPU cho Head Node duy trì hệ thống)
    if _use_gpu:
        _num_workers = total_gpus # Nếu có GPU, mỗi GPU là 1 worker
    else:
        # Nếu chạy CPU, trừ đi 1 CPU cho hệ thống thở
        _num_workers = max(1, total_cpus - 1)
    _default_batch = 256 * total_gpus if _use_gpu else 8
    _batch_size = int(os.getenv("TRAIN_BATCH_SIZE", _default_batch))
    _lr = float(os.getenv("TRAIN_LR", 0.0001))
    _epochs = int(os.getenv("TRAIN_EPOCHS", 5))
    
    ray.init(address="ray://ray-head:10001", runtime_env={"working_dir": current_dir}) # Nếu chỉ chạy docker
    # ray.init(address="auto")
    run_distributed_training(
        num_workers=_num_workers, use_gpu=_use_gpu, batch_size=_batch_size, lr=_lr, epochs=_epochs
    )
    ray.shutdown()