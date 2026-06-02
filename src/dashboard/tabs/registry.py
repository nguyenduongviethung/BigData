import pandas as pd
import streamlit as st

from services.mlflow_service import (
    get_registry_models
)


def render():

    st.subheader(
        "Model Registry"
    )

    models = get_registry_models()

    rows = []

    for model in models:

        rows.append(
            {
                "Name": model.name
            }
        )

    st.dataframe(
        pd.DataFrame(rows)
    )