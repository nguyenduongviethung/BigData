import os
import subprocess
import time

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

def run_step(step_name, command):
    """Hàm chạy một module và kiểm tra lỗi"""
    print(f"\n" + "="*60)
    print(f"🚀 BẮT ĐẦU: {step_name}")
    print("="*60)
    
    # Chạy lệnh terminal
    result = subprocess.run(command, check=True, cwd=CURRENT_DIR)
    
    # Kiểm tra xem file có chạy thành công không (Mã 0 là thành công)
    if result.returncode != 0:
        print(f"\n❌ LỖI NGHIÊM TRỌNG TẠI BƯỚC: {step_name}")
        print("Dừng toàn bộ hệ thống để kiểm tra!")
        exit(1)
    
    print(f"✅ Hoàn thành: {step_name}\n")

def run_evolution_loop(generations=10):
    """Vòng lặp AlphaZero Khép kín"""
    print("🤖 HỆ THỐNG ALPHAZERO TỰ ĐỘNG KÍCH HOẠT 🤖")
    
    for gen in range(1, generations + 1):
        print(f"\n" + "🌟"*20)
        print(f"   TIẾN HÓA THẾ HỆ THỨ {gen}   ")
        print("🌟"*20)
        
        # BƯỚC 1: Sinh dữ liệu (Thu thập kinh nghiệm)
        run_step(f"Self-Play (Thế hệ {gen})", ["python", "self_play.py"])
        
        # BƯỚC 2: Huấn luyện (Cập nhật trọng số -> Sinh ra @challenger)
        run_step(f"Training (Thế hệ {gen})", ["python", "train.py"])
        
        # BƯỚC 3: Đấu trường (Sàng lọc tự nhiên -> Tìm ra @champion mới)
        run_step(f"Arena Battle (Thế hệ {gen})", ["python", "arena.py"])
        
        print(f"💤 Vòng lặp {gen} hoàn tất. Hệ thống nghỉ 5 giây cho mát máy...\n")
        time.sleep(5)

if __name__ == "__main__":
    # Bắt đầu chạy tự động 50 vòng lặp (Bạn có thể sửa con số này)
    run_evolution_loop(generations=1)