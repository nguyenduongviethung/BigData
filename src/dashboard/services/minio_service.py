import polars as pl
import streamlit as st
from minio import Minio

from config import (
    MINIO_ENDPOINT,
    MINIO_USER,
    MINIO_PASSWORD,
    DATA_BUCKET
)


@st.cache_resource
def get_minio_client():

    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=False
    )

@st.cache_data(ttl=10)
def minio_health():

    try:

        buckets = list_buckets()

        total_objects = 0

        for bucket in buckets:

            total_objects += count_objects(
                bucket.name
            )

        return {
            "healthy": True,
            "bucket_count": len(buckets),
            "object_count": total_objects
        }

    except Exception as e:

        return {
            "healthy": False,
            "message": str(e)
        }


def list_buckets():

    client = get_minio_client()

    return list(client.list_buckets())

@st.cache_data(ttl=300)
def count_objects(bucket):

    client = get_minio_client()

    return sum(
        1
        for _ in client.list_objects(
            bucket,
            recursive=True
        )
    )


@st.cache_data(ttl=60)
def load_dataset_df():

    storage_options = {
        "AWS_ACCESS_KEY_ID": MINIO_USER,
        "AWS_SECRET_ACCESS_KEY": MINIO_PASSWORD,
        "AWS_ENDPOINT_URL": f"http://{MINIO_ENDPOINT}",
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    uri = (
        f"s3://{DATA_BUCKET}"
        "/gold/train_features"
    )

    df = pl.read_delta(
        uri,
        storage_options=storage_options
    )

    return df.to_pandas()