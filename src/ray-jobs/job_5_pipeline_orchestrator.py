import subprocess
import os
import time
import argparse

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

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--category",
        default="live_rapid"
    )

    parser.add_argument(
        "--year",
        type=int,
        default=None
    )

    parser.add_argument(
        "--month",
        type=int,
        default=None
    )

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
 
    common_args = [
        "--category", args.category,
        "--year", str(args.year),
        "--month", str(args.month)
    ]
 
    try:
        cmd = ["python", "job_1_ingestion_bronze.py"]

        for name, value in {
            "category": args.category,
            "year": args.year,
            "month": args.month,
        }.items():
            if value is not None:
                cmd.extend([f"--{name}", str(value)])

        run_step(
            "Job 1: Ingestion Bronze (API/PGN)",
            cmd
        )
        run_step("Job 2: Silver Transform (Làm sạch)",  ["python", "job_2_silver_transform.py"])
        run_step("Job 3: Gold Features (Metadata)",    ["python", "job_3_gold_features.py"])
        run_step("Job 4: Gold Tensors (Tensor)",      ["python", "job_4_gold_tensors.py"])
        print("\nToàn bộ pipeline đã chạy thành công!")
        
    except Exception as e:
        print(f"\nPIPELINE THẤT BẠI")
        print(f"Chi tiết: {e}")