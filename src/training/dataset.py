import io
import os
import numpy as np
import torch
import s3fs
from torch.utils.data import Dataset,IterableDataset
from minio import Minio

class ChessMinIODataset(Dataset):
    def __init__(self, file_uris, endpoint_url="http://minio:9000", access_key="minioadmin", secret_key="minioadmin"):
        self.file_uris = file_uris
        self.s3 = s3fs.S3FileSystem(
            key=access_key,
            secret=secret_key,
            client_kwargs={'endpoint_url': endpoint_url}
        )

    def __len__(self):
        return len(self.file_uris)

    def __getitem__(self, idx):
        file_uri = self.file_uris[idx]
        
        # Mở file từ MinIO
        with self.s3.open(file_uri, 'rb') as f:
            # 💡 BÍ QUYẾT: Đọc toàn bộ luồng byte vào RAM trước để NumPy có thể giải nén (.npz cần tính năng seek)
            buffer = io.BytesIO(f.read())
            
            # Đọc file NumPy Zip
            data = np.load(buffer)
            
            # Lấy các ma trận ra và chuyển đổi (Ép kiểu) ngay lập tức sang PyTorch Tensor
            # Lưu ý: PyTorch mạng nơ-ron thường yêu cầu đầu vào dạng float32
            states = torch.from_numpy(data['states']).float() 
            policies = torch.from_numpy(data['policies']).long() 
            values = torch.from_numpy(data['values']).float()
            
        # Trả về một dictionary hoặc tuple tùy theo cách bạn bóc tách trong vòng lặp train
        return states, policies, values