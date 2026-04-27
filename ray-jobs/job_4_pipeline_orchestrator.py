import subprocess
import os
import time

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

print("=========================================")
print("BẮT ĐẦU CHESS AI LAKEHOUSE PIPELINE")
print("=========================================")

def run_step(step_name, cmd):
    print(f"\nĐang chạy: {step_name}...")
    start_time = time.time()
    
    result = subprocess.run(cmd, check=True, cwd=CURRENT_DIR)
    
    if result.returncode != 0:
        raise RuntimeError(f"Step failed: {cmd}")
        
    print(f"⏱️ Hoàn thành trong {time.time() - start_time:.2f} giây.")

try:
    run_step("Job 1: Ingestion Bronze (API/PGN)", ["python", "job_1_ingestion_bronze.py"])
    run_step("Job 2: Silver Transform (Làm sạch)",  ["python", "job_2_silver_transform.py"])
    run_step("Job 3: Gold Features (Metadata)",    ["python", "job_3_gold_features.py"])
    
    print("\nToàn bộ pipeline đã chạy thành công!")
    
except Exception as e:
    print(f"\nPIPELINE THẤT BẠI")
    print(f"Chi tiết: {e}")