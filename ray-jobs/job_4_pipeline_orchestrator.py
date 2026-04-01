import subprocess

print("🚀 START LAKEHOUSE PIPELINE")

def run_step(cmd):
    result = subprocess.run(cmd, check=True)
    if result.returncode != 0:
        raise RuntimeError(f"Step failed: {cmd}")

run_step(["python", "job_1_ingestion_bronze.py"])
run_step(["python", "job_2_silver_transform.py"])
run_step(["python", "job_3_gold_features.py"])

print("🎉 DONE: Bronze → Silver → Gold")