import os

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "minio:9000"
)

MINIO_USER = os.getenv(
    "MINIO_USER",
    "admin"
)

MINIO_PASSWORD = os.getenv(
    "MINIO_PASSWORD",
    "admin123"
)

MLFLOW_URI = os.getenv(
    "MLFLOW_URI",
    "http://mlflow-service:5000"
)

RAY_ENDPOINT = os.getenv(
    "RAY_ENDPOINT",
    "ray://ray-head:10001"
)

RAY_DASHBOARD_URL = os.getenv(
    "RAY_DASHBOARD_URL",
    "http://ray-head:8265"
)

DATA_BUCKET = os.getenv(
    "DATA_BUCKET",
    "chess-data"
)