import streamlit as st

from services.minio_service import (
    list_buckets
)

from services.mlflow_service import (
    get_experiments
)

from services.ray_service import (
    cluster_resources
)


def render():

    st.subheader(
        "Pipeline Status"
    )

    col1, col2, col3 = st.columns(3)

    try:

        buckets = list_buckets()

        with col1:

            st.metric(
                "Buckets",
                len(buckets)
            )

    except Exception as e:

        with col1:
            st.error(str(e))

    try:

        experiments = get_experiments()

        with col2:

            st.metric(
                "Experiments",
                len(experiments)
            )

    except Exception as e:

        with col2:
            st.error(str(e))

    try:

        resources = cluster_resources()

        st.metric(
            "CPU Cores",
            f"{resources['cpu_total']:.0f}"
        )

        st.metric(
            "Memory",
            f"{resources['memory_total_gb']:.2f} GiB"
        )

        st.metric(
            "Nodes",
            resources["alive_nodes"]
        )

    except Exception as e:

        with col3:
            st.error(str(e))