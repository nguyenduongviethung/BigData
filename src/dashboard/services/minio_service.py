from io import BytesIO

import pandas as pd
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


def list_buckets():

    client = get_minio_client()

    return list(client.list_buckets())


def count_objects(bucket):

    client = get_minio_client()

    return sum(
        1
        for _ in client.list_objects(
            bucket,
            recursive=True
        )
    )


def load_games_df():

    client = get_minio_client()

    dfs = []

    objects = client.list_objects(
        DATA_BUCKET,
        prefix="games/",
        recursive=True
    )

    for obj in objects:

        if not obj.object_name.endswith(".parquet"):
            continue

        response = client.get_object(
            DATA_BUCKET,
            obj.object_name
        )

        dfs.append(
            pd.read_parquet(
                BytesIO(
                    response.read()
                )
            )
        )

    if not dfs:
        return pd.DataFrame()

    return pd.concat(
        dfs,
        ignore_index=True
    )