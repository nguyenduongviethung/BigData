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