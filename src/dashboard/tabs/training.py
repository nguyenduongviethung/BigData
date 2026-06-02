import streamlit as st
import plotly.graph_objects as go

from services.mlflow_service import (
    get_latest_runs
)


def render():

    st.subheader(
        "Training Metrics"
    )

    result = get_latest_runs()

    if result is None:

        st.warning(
            "No runs found"
        )

        return

    exp, runs = result

    st.write(
        f"Experiment: {exp.name}"
    )

    st.dataframe(runs)

    if (
        "metrics.policy_loss"
        in runs.columns
    ):

        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=runs["start_time"],
                y=runs[
                    "metrics.policy_loss"
                ],
                name="Policy Loss"
            )
        )

        if (
            "metrics.value_loss"
            in runs.columns
        ):

            fig.add_trace(
                go.Scatter(
                    x=runs["start_time"],
                    y=runs[
                        "metrics.value_loss"
                    ],
                    name="Value Loss"
                )
            )

        st.plotly_chart(
            fig,
            use_container_width=True
        )