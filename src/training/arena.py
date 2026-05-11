import os
import time
import torch
import chess
import ray
import mlflow
from mlflow.tracking import MlflowClient
from dotenv import load_dotenv

# Đọc cấu hình môi trường
current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(current_dir, ".env"))

# Import mô hình và MCTS
from model import ChessAlphaZeroNet
from mcts import MCTS
import s3fs
import json

# ==========================================
# HÀM HELPER: TẢI MODEL TỪ MINIO QUA MLFLOW
# ==========================================
def load_model_by_alias(alias, s3_client):
    """Tải trọng số trực tiếp từ MinIO dựa trên nhãn (Alias)"""
    client = MlflowClient()
    try:
        model_version = client.get_model_version_by_alias("Chess_AlphaZero_Model", alias)
        local_json = mlflow.artifacts.download_artifacts(
            run_id=model_version.run_id,
            artifact_path="alphazero_pointer/checkpoint_metadata.json"
        )
        with open(local_json, "r") as f:
            parent_uri = json.load(f).get("minio_checkpoint_uri")
            
        clean_path = parent_uri.replace("s3://", "") + "/model_latest.pt"
        
        model = ChessAlphaZeroNet()
        with s3_client.open(clean_path, "rb") as f:
            model.load_state_dict(torch.load(f, map_location="cpu"))
        model.eval()
        return model, model_version.version
    except Exception as e:
        print(f"⚠️ Lỗi khi tải mô hình [{alias}]: {e}")
        return None, None

# ==========================================
# CÔNG NHÂN ĐẤU TRƯỜNG (RAY TASK)
# ==========================================
@ray.remote(num_cpus=1)
def play_arena_match(match_id, s3_endpoint, s3_user, s3_pass):
    """Một ván cờ sinh tử. Trả về: 1 (Challenger thắng), 0 (Hòa), -1 (Thua)"""
    print(f"⚔️ [Trận {match_id}] Khởi tạo bàn cờ...")
    
    # 1. Khởi tạo s3fs cục bộ trong Worker
    s3 = s3fs.S3FileSystem(
        key=s3_user, secret=s3_pass,
        client_kwargs={'endpoint_url': f"http://{s3_endpoint}"}
    )
    
    # Bổ sung chứng chỉ AWS cho MLflow trong Worker
    os.environ["AWS_ACCESS_KEY_ID"] = s3_user
    os.environ["AWS_SECRET_ACCESS_KEY"] = s3_pass
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = f"http://{s3_endpoint}"
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-server:5000"))
    
    # 2. Tải 2 bộ não về RAM của Worker
    champ_model, _ = load_model_by_alias("champion", s3)
    chall_model, _ = load_model_by_alias("challenger", s3)
    
    if not champ_model or not chall_model:
        print(f"❌ [Trận {match_id}] LỖI NGHIÊM TRỌNG: Không thể tải mô hình! Hủy trận đấu.")
        raise ValueError("Thiếu mô hình @champion hoặc @challenger. Hãy kiểm tra MLflow!")
    
    # Kẻ thách đấu cầm Trắng (ưu thế đi trước) - Ở thực tế sẽ đảo màu xen kẽ
    white_mcts = MCTS(model=chall_model, num_simulations=20) 
    black_mcts = MCTS(model=champ_model, num_simulations=20)

    board = chess.Board()

    # 3. Hai bên tự đánh
    while not board.is_game_over() and board.fullmove_number < 300:
        if board.turn == chess.WHITE:
            move = white_mcts.get_action_prob(board)
        else:
            move = black_mcts.get_action_prob(board)
        board.push(move)

    # 4. Phán xử kết quả
    result = board.result()
    if result == "1-0": 
        print(f"🏆 [Trận {match_id}] Kẻ thách đấu (@challenger) THẮNG!")
        return 1
    elif result == "0-1":
        print(f"💀 [Trận {match_id}] Kẻ thách đấu (@challenger) THUA!")
        return -1
    else:
        print(f"🤝 [Trận {match_id}] HÒA!")
        return 0

# ==========================================
# ĐIỀU PHỐI ĐẤU TRƯỜNG TẠI DRIVER
# ==========================================
if __name__ == "__main__":
    print("🏟️ CHÀO MỪNG ĐẾN VỚI ĐẤU TRƯỜNG ALPHAZERO 🏟️")
    
    ray.init(address="ray://ray-head:10001", runtime_env={"working_dir": current_dir})
    
    # Thông số MinIO
    s3_ep = os.getenv("MINIO_ENDPOINT", "minio:9000")
    s3_u = os.getenv("MINIO_ROOT_USER", "minioadmin")
    s3_p = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    
    NUM_MATCHES = 4 # Số trận demo (bằng số core CPU để chạy 1 lượt là xong)
    
    print(f"🔥 Phát động {NUM_MATCHES} trận thư hùng trên Ray Cluster...")
    
    # Ném công việc lên Cluster
    futures = [play_arena_match.remote(i, s3_ep, s3_u, s3_p) for i in range(NUM_MATCHES)]
    results = ray.get(futures)
    
    # Thống kê
    wins = results.count(1)
    losses = results.count(-1)
    draws = results.count(0)
    
    win_rate = wins / NUM_MATCHES
    
    print("\n📊 ============ BẢNG VÀNG KẾT QUẢ ============")
    print(f"Thắng: {wins} | Thua: {losses} | Hòa: {draws}")
    print(f"Tỷ lệ thắng của @challenger: {win_rate*100}%")
    print("=============================================\n")
    
    # CHUYỂN GIAO QUYỀN LỰC
    if win_rate >= 0.55:
        print("👑 KẾT LUẬN: TỶ LỆ > 55%. KẺ THÁCH ĐẤU TRỞ THÀNH TÂN VƯƠNG!")
        client = MlflowClient(tracking_uri=os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-server:5000"))
        
        # Lấy Version hiện tại của Challenger
        chall_version = client.get_model_version_by_alias("Chess_AlphaZero_Model", "challenger").version
        
        # Phong tước hiệu @champion mới
        client.set_registered_model_alias("Chess_AlphaZero_Model", "champion", chall_version)
        print(f"✅ Đã gắn nhãn [@champion] cho Version {chall_version} trên MLflow!")
    else:
        print("❌ KẾT LUẬN: Kẻ thách đấu chưa đủ mạnh. Cựu vương vẫn giữ ngôi.")

    ray.shutdown()