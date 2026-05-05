import os
import io
import uuid
import time
import numpy as np
import torch
import chess
from mcts import MCTS
import s3fs
import json
import ray
from dotenv import load_dotenv
import mlflow
from mlflow.tracking import MlflowClient

# Đọc cấu hình môi trường
current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(current_dir, ".env"))

# Import model của bạn
from model import ChessAlphaZeroNet

# --- HÀM HELPER CHUYỂN ĐỔI BÀN CỜ TẠM THỜI (Dùng cho Demo) ---
def board_to_tensor(board):
    """Chuyển FEN thành ma trận (1, 12, 8, 8) cho mạng nơ-ron"""
    tensor = np.zeros((12, 8, 8), dtype=np.float32)
    piece_map = board.piece_map()
    for square, piece in piece_map.items():
        row, col = divmod(square, 8)
        piece_idx = piece.piece_type - 1
        if piece.color == chess.BLACK:
            piece_idx += 6
        tensor[piece_idx, row, col] = 1.0
    return torch.from_numpy(tensor).unsqueeze(0)


# ==========================================
# CÔNG NHÂN TỰ CHƠI (RAY ACTOR)
# ==========================================
@ray.remote(num_cpus=1) # Khai báo đây là 1 Node độc lập chạy ngầm
class SelfPlayWorker:
    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.model = ChessAlphaZeroNet()
        self.model.eval() # Chế độ suy luận
        
        # Kết nối MinIO
        self.raw_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.s3 = s3fs.S3FileSystem(
            key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            secret=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
            client_kwargs={'endpoint_url': f"http://{self.raw_endpoint}"}
        )
        
        self._load_champion_model()

    def _load_champion_model(self):
        """Kéo bộ não @champion từ MinIO vào RAM của Worker"""
        print(f"[Worker {self.worker_id}] 🧠 Đang tìm kiếm nhà vô địch [@champion]...")

        os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("MINIO_ROOT_USER", "minioadmin")
        os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        os.environ["MLFLOW_S3_ENDPOINT_URL"] = f"http://{self.raw_endpoint}"

        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-server:5000"))
        client = MlflowClient()
        
        try:
            prod_version = client.get_model_version_by_alias("Chess_AlphaZero_Model", "champion")
            local_json = mlflow.artifacts.download_artifacts(
                run_id=prod_version.run_id,
                artifact_path="alphazero_pointer/checkpoint_metadata.json"
            )
            with open(local_json, "r") as f:
                parent_uri = json.load(f).get("minio_checkpoint_uri")
                
            clean_path = parent_uri.replace("s3://", "") + "/model_latest.pt"
            with self.s3.open(clean_path, "rb") as f:
                self.model.load_state_dict(torch.load(f, map_location="cpu"))
            print(f"[Worker {self.worker_id}] ✅ Đã nạp thành công bộ não [@champion] Version {prod_version.version}!")
        except Exception as e:
            print(f"[Worker {self.worker_id}] ⚠️ Không tìm thấy/Lỗi nạp [@champion]: {e}. Sử dụng não sơ sinh (Random).")

    def play_games_and_save(self, num_games=10):
        """Vòng lặp chơi cờ và ném dữ liệu lên Data Lake"""
        # Khởi tạo MCTS, gắn Mạng Nơ-ron của Worker vào. 
        # Đặt 50 lần mô phỏng cho mỗi nước đi (Thực tế AlphaZero chạy 800)
        mcts = MCTS(model=self.model, num_simulations=50)

        states_buffer = []
        policies_buffer = []
        values_buffer = []
        
        print(f"[Worker {self.worker_id}] ⚔️ Bắt đầu cày {num_games} ván cờ...")
        start_time = time.time()

        for game_idx in range(num_games):
            board = chess.Board()
            game_states = []
            
            # AI tự đánh với chính mình (Giới hạn 100 nước để tránh lặp vô tận)
            while not board.is_game_over() and board.fullmove_number < 100:
                state_tensor = board_to_tensor(board)
                game_states.append(state_tensor.squeeze(0).numpy())
                
                # ====================================================
                # SỰ THAY ĐỔI LỚN NHẤT: Bỏ random, dùng MCTS để SUY NGHĨ
                # ====================================================
                best_move = mcts.get_action_prob(board)
                board.push(best_move)
                
                # Giả lập ghi nhận chính sách
                policies_buffer.append(1)
                
            # Đánh giá kết quả cuối ván cờ (Reward)
            result = board.result()
            reward = 0.0
            if result == "1-0": reward = 1.0
            elif result == "0-1": reward = -1.0
            
            # Gán Reward ngược lại cho toàn bộ nước đi của ván đó
            values_buffer.extend([reward] * len(game_states))
            states_buffer.extend(game_states)

        # ==========================================
        # ĐẨY DỮ LIỆU LÊN MINIO (GOLD ZONE)
        # ==========================================
        file_name = f"chess-data/gold/tensors/selfplay_{uuid.uuid4().hex[:8]}.npz"
        
        # Nén trên RAM (Không chạm ổ cứng vật lý)
        npz_buffer = io.BytesIO()
        np.savez_compressed(
            npz_buffer, 
            states=np.array(states_buffer, dtype=np.float32),
            policies=np.array(policies_buffer, dtype=np.int64),
            values=np.array(values_buffer, dtype=np.float32)
        )
        npz_buffer.seek(0)
        
        # Bắn qua mạng lên MinIO
        with self.s3.open(file_name, "wb") as f:
            f.write(npz_buffer.read())
            
        duration = time.time() - start_time
        print(f"[Worker {self.worker_id}] 🚀 Đã bắn File {file_name} lên MinIO | Mất {duration:.2f}s")
        return file_name

# ==========================================
# ĐIỀU PHỐI TỪ DRIVER NODE
# ==========================================
if __name__ == "__main__":
    print("🌟 KHỞI ĐỘNG NHÀ MÁY DATA CHESS ALPHA ZERO 🌟")
    
    # Kết nối vào cụm Ray
    ray.init(address="ray://ray-head:10001", runtime_env={"working_dir": current_dir})
    
    NUM_WORKERS = 4  # Số lượng máy tự chơi song song (Có thể tăng lên hàng trăm)
    GAMES_PER_WORKER = 5 
    
    print(f"🏭 Đang triệu hồi {NUM_WORKERS} Công nhân (Ray Actors)...")
    workers = [SelfPlayWorker.remote(worker_id=i) for i in range(NUM_WORKERS)]
    
    print(f"🔥 Ra lệnh đồng loạt: Mỗi công nhân cày {GAMES_PER_WORKER} ván!")
    # Lệnh .remote() sẽ kích hoạt tất cả chạy song song
    futures = [worker.play_games_and_save.remote(num_games=GAMES_PER_WORKER) for worker in workers]
    
    # Chờ thu thập kết quả
    saved_files = ray.get(futures)
    
    print("\n🎉 HOÀN TẤT CHIẾN DỊCH!")
    print(f"📂 Các file Tensor mới đã an tọa trên MinIO Data Lake:")
    for f in saved_files:
        print(f"   -> s3://{f}")
        
    ray.shutdown()