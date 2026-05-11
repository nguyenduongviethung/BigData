import torch
import torch.nn as nn
import torch.nn.functional as F

class ResBlock(nn.Module):
    """
    Khối Residual Block: Giúp mạng nơ-ron có thể xây rất sâu mà không bị quên kiến thức (Vanishing Gradient).
    """
    def __init__(self, num_filters):
        super(ResBlock, self).__init__()
        self.conv1 = nn.Conv2d(num_filters, num_filters, kernel_size=3, padding=1)
        self.bn1 = nn.BatchNorm2d(num_filters)
        self.conv2 = nn.Conv2d(num_filters, num_filters, kernel_size=3, padding=1)
        self.bn2 = nn.BatchNorm2d(num_filters)

    def forward(self, x):
        residual = x
        out = F.relu(self.bn1(self.conv1(x)))
        out = self.bn2(self.conv2(out))
        out += residual  # Skip connection (Cộng gộp phần dư từ lớp trước)
        out = F.relu(out)
        return out

class ChessAlphaZeroNet(nn.Module):
    def __init__(self, num_res_blocks=5, num_filters=128, action_space_size=4672):
        super(ChessAlphaZeroNet, self).__init__()
        
        # 1. KHỐI ĐẦU VÀO (Nhận ma trận bàn cờ 12x8x8)
        self.conv_block = nn.Sequential(
            nn.Conv2d(12, num_filters, kernel_size=3, padding=1),
            nn.BatchNorm2d(num_filters),
            nn.ReLU()
        )
        
        # 2. LÕI TƯ DUY (Các khối ResBlock nối tiếp nhau)
        self.res_blocks = nn.ModuleList(
            [ResBlock(num_filters) for _ in range(num_res_blocks)]
        )
        
        # 3. ĐẦU RA 1: CHIẾN THUẬT (Policy Head - Dự đoán nước đi)
        self.policy_head = nn.Sequential(
            nn.Conv2d(num_filters, 2, kernel_size=1),
            nn.BatchNorm2d(2),
            nn.ReLU(),
            nn.Flatten(),
            nn.Linear(2 * 8 * 8, action_space_size) # Trả ra 4672 xác suất
        )
        
        # 4. ĐẦU RA 2: ĐÁNH GIÁ (Value Head - Dự đoán cục diện Thắng/Thua)
        self.value_head = nn.Sequential(
            nn.Conv2d(num_filters, 1, kernel_size=1),
            nn.BatchNorm2d(1),
            nn.ReLU(),
            nn.Flatten(),
            nn.Linear(1 * 8 * 8, 256),
            nn.ReLU(),
            nn.Linear(256, 1),
            nn.Tanh() # Ép giá trị về khoảng [-1, 1]
        )

    def forward(self, x):
        # Đưa dữ liệu qua lõi
        x = self.conv_block(x)
        for block in self.res_blocks:
            x = block(x)
            
        # Rẽ nhánh ra 2 quyết định cùng lúc
        policy = self.policy_head(x)
        value = self.value_head(x)
        
        return policy, value

# --- KIỂM TRA MÔ HÌNH NHANH ---
if __name__ == "__main__":
    model = ChessAlphaZeroNet(num_res_blocks=5, num_filters=128)
    print("✅ Khởi tạo mạng AI thành công!")
    
    # Tạo một Tensor giả lập: 1 lô gồm 32 ván cờ, mỗi ván có kích thước 12x8x8
    dummy_input = torch.randn(32, 12, 8, 8)
    
    # Chạy thử
    policy_out, value_out = model(dummy_input)
    
    print(f"Kích thước Đầu vào: {dummy_input.shape}")
    print(f"Kích thước Policy (Nước đi): {policy_out.shape} -> Khớp với 4672 hành động")
    print(f"Kích thước Value (Thắng/Thua): {value_out.shape} -> Khớp với 1 giá trị đánh giá")