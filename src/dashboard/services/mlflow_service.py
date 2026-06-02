import mlflow
import streamlit as st


from mlflow.tracking import MlflowClient

from config import MLFLOW_URI


@st.cache_resource
def get_mlflow():

    mlflow.set_tracking_uri(
        MLFLOW_URI
    )

    return mlflow


def get_experiments():

    return get_mlflow().search_experiments()


def get_latest_runs(limit=100):

    experiments = get_experiments()

    if not experiments:
        return None

    exp = experiments[0]

    runs = mlflow.search_runs(
        experiment_ids=[
            exp.experiment_id
        ],
        max_results=limit
    )

    return exp, runs


def get_registry_models():

    client = MlflowClient()

    return client.search_registered_models()