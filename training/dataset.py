import io
import os
import numpy as np
import torch
from torch.utils.data import Dataset
from minio import Minio

class ChessMinIODataset(Dataset):
    def __init__(self, bucket="chess-data", prefix="gold/tensors/"):
        # Kết nối tới MinIO
        self.client = Minio(
            "minio:9000",
            access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
            secure=False
        )
        self.bucket = bucket
        self.prefix = prefix
        self.samples = []
        
        print("⏳ Đang tải Tensors từ MinIO vào bộ nhớ...")
        self._load_data()

    def _load_data(self):
        # Lấy danh sách tất cả file .npz trong Gold Zone
        objects = self.client.list_objects(self.bucket, prefix=self.prefix, recursive=True)
        
        for obj in objects:
            if obj.object_name.endswith('.npz'):
                resp = self.client.get_object(self.bucket, obj.object_name)
                # Đọc byte trực tiếp không cần lưu file cứng
                file_data = io.BytesIO(resp.read())
                data = np.load(file_data)
                
                # Mạng Neural (Conv2d) luôn yêu cầu input dạng Float32
                X = torch.tensor(data['states'], dtype=torch.float32) 
                P = torch.tensor(data['policies'], dtype=torch.long)    # Phân loại đa lớp dùng Long
                V = torch.tensor(data['values'], dtype=torch.float32)   # Hồi quy dùng Float32
                
                # Đóng gói thành danh sách các mẫu
                for i in range(len(X)):
                    self.samples.append((X[i], P[i], V[i]))
                    
                resp.close()
                resp.release_conn()
                
        print(f"✅ Đã nạp thành công {len(self.samples)} trạng thái cờ vua.")

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, idx):
        return self.samples[idx]