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

@st.cache_data(ttl=10)
def mlflow_health():

    try:

        experiments = get_experiments()

        model_count = len(
            get_registry_models()
        )

        run_count = 0

        for exp in experiments:

            try:

                runs = mlflow.search_runs(
                    experiment_ids=[
                        exp.experiment_id
                    ]
                )

                run_count += len(runs)

            except Exception:
                pass

        return {
            "healthy": True,
            "experiments": len(experiments),
            "runs": run_count,
            "models": model_count
        }

    except Exception as e:

        return {
            "healthy": False,
            "message": str(e)
        }

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