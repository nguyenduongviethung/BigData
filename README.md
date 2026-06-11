# ♟️ Chess AI Big Data Pipeline

## 1. Overview

Dự án này xây dựng một hệ thống xử lý dữ liệu lớn (Big Data Pipeline) phục vụ huấn luyện mô hình chơi cờ vua sử dụng **Neural Network + Monte Carlo Tree Search (MCTS)**.

Pipeline end-to-end:

### Mục tiêu
- Xử lý **petabyte dữ liệu/ngày** từ self-play  
- Hỗ trợ **continuous training**  
- Thiết kế **scalable, fault-tolerant, reproducible**  
- Chạy trên **cloud / Kubernetes cluster**

---

## 2. System Architecture

Kiến trúc tổng thể:
- **Lambda Architecture** (Batch + Streaming)
- **Data Lakehouse**

### Các thành phần chính

- **Ingestion Layer**: Thu thập dữ liệu batch & streaming  
- **Processing Layer**: Spark / Ray  
- **Storage Layer**: **MinIO + Delta Lake**  
- **Serving & Monitoring**: Airflow, MLflow, Grafana  
- **Training Layer**: PyTorch Distributed (GPU cluster)

Pipeline được orchestration bằng **DAG (Airflow/Dagster)**.

---

## 3. Data Pipeline

### 3.1 Data Collection

#### Nguồn dữ liệu

- **Batch**
  - Lichess PGN database  
  - Chess.com API  
  - FIDE datasets  

- **Streaming (self-play)**
  - Model tự chơi (Ray actors / K8s pods)  
  - Engine evaluation (Stockfish)

#### Công nghệ

- Batch: Python scripts / Apache NiFi  
- Streaming: Kafka  

#### Scale

- > **1 triệu ván/giờ**

---

### 3.2 Data Processing

Chuyển dữ liệu thô thành dữ liệu huấn luyện:

#### Các bước

1. Parse PGN → FEN + move + result  
2. Feature engineering:
   - Tensor **8×8×12**
   - Metadata (castling, en-passant, etc.)
3. Label:
   - Policy (move distribution)
   - Value (win/loss/draw)
4. Data augmentation:
   - Symmetry (×8)
5. Cleaning:
   - Lọc game lỗi / chất lượng thấp  

#### Công nghệ

- Batch: Spark (PySpark)  
- Streaming: Ray Data / Spark Structured Streaming  

---

### 3.3 Data Storage

Sử dụng **Data Lakehouse với MinIO (S3-compatible) + Delta Lake**:

| Layer   | Mô tả                     | Format        |
|--------|--------------------------|--------------|
| Raw    | Dữ liệu gốc              | PGN (gzip)   |
| Bronze | Sau parsing              | Parquet      |
| Silver | Clean + augmented        | Parquet      |
| Gold   | Ready-to-train           | Columnar     |

#### Kiến trúc

- **MinIO** đóng vai trò object storage (tương thích S3 API)
- **Delta Lake** quản lý metadata, versioning và ACID transaction
- Có thể deploy **on-premise hoặc Kubernetes**

#### Tính năng

- Versioning: Delta Lake / DVC  
- Time-travel (rollback dataset)  
- High throughput (phù hợp self-play streaming)  
- Chi phí thấp hơn so với cloud storage  

---

### 3.4 Visualization & Monitoring

#### Monitoring

- Airflow + Prometheus + Grafana  
- Kafka throughput  
- Pipeline latency & error rate  

#### Data Quality

- Great Expectations  
- Phân bố ELO, độ dài ván  

#### Training Metrics

- TensorBoard / Weights & Biases  
- Loss (policy + value)  
- Elo rating progression  

---

## 4. Training Integration

- Data từ **Gold Zone** → training job (PyTorch Distributed)  
- Self-play → loop lại pipeline (closed-loop RL)  
- Tracking: MLflow  

---

## 5. Tech Stack

| Category        | Technology |
|----------------|-----------|
| Orchestration  | Airflow / Dagster |
| Processing     | Ray + Spark |
| Storage        | MinIO + Delta Lake |
| ML             | PyTorch Distributed |
| Monitoring     | Grafana + Prometheus |
| Infrastructure | Kubernetes |

---

## 6. Challenges

- **Data volume & velocity**  
  → Dùng Ray + distributed processing  

- **Parsing bottleneck**  
  → Rust / Polars  

- **Data drift**  
  → Continual learning + replay buffer  

- **Reproducibility**  
  → MLflow + Delta time-travel  

---

## 7. Future Work

- Feature Store (Feast)  
- LLM sinh commentary cho nước cờ  
- Real-time inference API  
- Mở rộng sang Go / Shogi  

---

## 8. Expected Outcome

- Pipeline production-ready  
- Xử lý dữ liệu quy mô lớn  
- Mô hình đạt **Elo > 3500**  

---

# 9. Installation & Usage

## 9.1 Prerequisites

Yêu cầu cài đặt trước:

- Docker
- Docker Buildx
- kubectl
- k3d
- Kubernetes
- Python 3.11+
- Ray CLI

Kiểm tra:

```bash
docker --version
kubectl version --client
k3d version
ray --version
```

---

## 9.2 Cluster Initialization

Khởi tạo Kubernetes cluster bằng k3d và registry nội bộ:

```bash
k3d cluster create alphazero-cluster \
    --servers 1 \
    --agents 1 \
    -p "80:80@loadbalancer" \
    -p "10001:10001@loadbalancer" \
    --registry-create alphazero-registry:5050
```

Kiểm tra cluster:

```bash
kubectl get nodes
```

---

## 9.3 Build & Push Docker Images

### Ray Worker Image

```bash
docker buildx build \
    -f docker/ray/Dockerfile \
    -t localhost:5050/alphazero-node:v1 \
    --push .
```

### MLflow Image

```bash
docker buildx build \
    -f docker/mlflow/Dockerfile \
    -t localhost:5050/custom-mlflow:v1 \
    --push .
```

### Dashboard Image

```bash
docker buildx build \
    -f docker/dashboard/Dockerfile \
    -t localhost:5050/chess-dashboard:v24 \
    --push .
```

Kiểm tra image đã được đẩy lên registry:

```bash
docker image ls
```

---

## 9.4 Environment Configuration

Tạo file `.env` từ mẫu:

```bash
cp .env.example .env
```

Cập nhật các biến môi trường cần thiết trong file `.env`.

Ví dụ:

```env
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123

POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=mlflow

MLFLOW_S3_ENDPOINT_URL=http://minio-service:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin123
```

Tạo Kubernetes Secret từ file `.env`:

```bash
kubectl create secret generic alphazero-secret \
    --from-env-file=.env
```

Kiểm tra:

```bash
kubectl get secret
```

---

## 9.5 Deploy Infrastructure

Triển khai các thành phần của hệ thống:

### MinIO

```bash
kubectl apply -f k8s/minio.yaml
```

### Ray Cluster

```bash
kubectl apply -f k8s/ray-cluster.yaml
```

### PostgreSQL

```bash
kubectl apply -f k8s/postgres.yaml
```

### MLflow

```bash
kubectl apply -f k8s/mlflow.yaml
```

### Dashboard

```bash
kubectl apply -f k8s/dashboard.yaml
```

### Ingress

```bash
kubectl apply -f k8s/ingress.yaml
```

Triển khai toàn bộ:

```bash
kubectl apply -f k8s/
```

---

## 9.6 Verify Deployment

Kiểm tra pods:

```bash
kubectl get pods -A
```

Kiểm tra services:

```bash
kubectl get svc
```

Kiểm tra ingress:

```bash
kubectl get ingress
```

Kiểm tra RayCluster:

```bash
kubectl get raycluster
```

---

## 9.7 Access Services

### Ray Dashboard

```text
http://ray.local
```

Hoặc:

```text
http://ray.local:8265
```

### MLflow

```text
http://mlflow.local
```

### Dashboard

```text
http://dashboard.local
```

---

## 9.8 Submit Ray Jobs

Thực thi một file Python trong cluster:

```bash
ray job submit \
    --address http://ray.local:8265 \
    --working-dir ./src \
    -- python <file_path>
```

Ví dụ:

```bash
ray job submit \
    --address http://ray.local:8265 \
    --working-dir ./src \
    -- python data_processing/bronze_to_silver.py
```

### Theo dõi job

Liệt kê jobs:

```bash
ray job list \
    --address http://ray.local:8265
```

Xem logs:

```bash
ray job logs <job_id> \
    --address http://ray.local:8265
```

---

## 9.9 Training Pipeline

Huấn luyện mô hình:

```bash
ray job submit \
    --address http://ray.local:8265 \
    --working-dir ./src \
    -- python training/train.py
```

Các artifact và metrics sẽ được lưu vào:

- MLflow Tracking Server
- MinIO Artifact Store

---

## 9.10 Shutdown

Xóa toàn bộ cluster:

```bash
k3d cluster delete alphazero-cluster
```

Kiểm tra:

```bash
k3d cluster list
```

---