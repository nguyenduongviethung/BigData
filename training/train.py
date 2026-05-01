import os
import torch
import torch.nn as nn
import torch.optim as optim
import ray
from ray import train
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer
from torch.utils.data import DataLoader

# Import các module đã viết
from model import ChessAlphaZeroNet
from dataset import ChessMinIODataset

def train_loop_per_worker(config):
    """
    Hàm này sẽ được Ray copy và chạy trên TẤT CẢ các Worker trong cụm.
    """
    # 1. Chuẩn bị Dữ liệu
    dataset = ChessMinIODataset()
    dataloader = DataLoader(dataset, batch_size=config["batch_size"], shuffle=True)
    
    # 🔥 MA THUẬT CỦA RAY: Tự động chia đều dữ liệu (Distributed Sampler)
    # Nếu có 4 GPUs, mỗi GPU sẽ chỉ lấy 1/4 lượng dữ liệu, không bị trùng lặp.
    dataloader = train.torch.prepare_data_loader(dataloader)

    # 2. Chuẩn bị Mô hình
    model = ChessAlphaZeroNet()
    
    # 🔥 MA THUẬT CỦA RAY: Tự động bọc mô hình bằng PyTorch DDP
    # Đồng bộ Gradients giữa các máy chủ qua mạng nội bộ.
    model = train.torch.prepare_model(model)

    # 3. Hàm Mất mát & Tối ưu hóa
    optimizer = optim.Adam(model.parameters(), lr=config["lr"])
    policy_criterion = nn.CrossEntropyLoss()
    value_criterion = nn.MSELoss()

    # 4. Vòng lặp Huấn luyện
    for epoch in range(config["num_epochs"]):
        model.train()
        total_loss = 0.0
        
        for batch_idx, (states, policies, values) in enumerate(dataloader):
            optimizer.zero_grad()
            
            # Chạy qua mạng nơ-ron
            pred_policies, pred_values = model(states)
            
            # Tính sai số
            loss_policy = policy_criterion(pred_policies, policies)
            # pred_values có shape (Batch, 1), values có shape (Batch,) -> Cần nén lại cho khớp
            loss_value = value_criterion(pred_values.squeeze(-1), values) 
            
            loss = loss_policy + loss_value
            
            # Lan truyền ngược và cập nhật trọng số
            loss.backward()
            optimizer.step()
            
            total_loss += loss.item()

        # Báo cáo kết quả về cho Ray Head sau mỗi epoch
        avg_loss = total_loss / len(dataloader)
        train.report({"epoch": epoch, "loss": avg_loss})


def run_distributed_training():
    # ⚙️ Cấu hình quy mô cụm (Scaling)
    scaling_config = ScalingConfig(
        num_workers=2,         # Số lượng Worker (hoặc số GPU) muốn huy động
        use_gpu=False,         # Đặt = True nếu bạn có Card rời NVIDIA
    )

    # Khởi tạo Nhạc trưởng Huấn luyện
    trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config={
            "batch_size": 128, 
            "lr": 0.001, 
            "num_epochs": 10
        },
        scaling_config=scaling_config,
    )
    
    print("🚀 Bắt đầu quá trình huấn luyện phân tán...")
    result = trainer.fit()
    print(f"✅ Hoàn tất! Lịch sử đánh giá:\n{result.metrics_dataframe}")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Đóng gói và gửi toàn bộ thư mục ai-training sang các máy con
    ray.init(
        address="ray://ray-head:10001", 
        runtime_env={"working_dir": current_dir}
    )
    run_distributed_training()
    ray.shutdown()