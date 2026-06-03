import streamlit as st
import pandas as pd

from services.minio_service import (
    minio_health,
    list_buckets,
    count_objects
)

from services.mlflow_service import (
    mlflow_health,
    get_experiments
)

from services.ray_service import (
    ray_summary
)


def render():

    st.subheader(
        "Pipeline Status"
    )

    col1, col2, col3 = st.columns(3)

    try:
        with col1:
            info = minio_health()

            if info["healthy"]:

                st.success("🟢 MinIO")

            else:

                st.error("🔴 MinIO")

                st.caption(
                    info["message"]
                )

    except Exception as e:

        with col1:
            st.error(str(e))

    try:

        with col2:

            info = mlflow_health()

            if info["healthy"]:

                st.success("🟢 MLflow")

            else:

                st.error("🔴 MLflow")

    except Exception as e:

        with col2:
            st.error(str(e))

    try:
        with col3:

            info = ray_summary()

            if info["healthy"]:

                st.success("🟢 Ray")

            else:

                st.error("🔴 Ray")

    except Exception as e:

        with col3:
            st.error(str(e))

    st.divider()

    st.subheader(
        "MinIO Storage"
    )

    info = minio_health()

    c1, c2 = st.columns(2)

    c1.metric(
        "Buckets",
        info["bucket_count"]
    )

    c2.metric(
        "Objects",
        info["object_count"]
    )

    rows = []

    for bucket in list_buckets():

        rows.append(
            {
                "Bucket": bucket.name,
                "Objects": count_objects(
                    bucket.name
                )
            }
        )

    st.dataframe(
        pd.DataFrame(rows),
        use_container_width=True
    )

    st.divider()

    st.subheader(
        "MLflow Tracking"
    )

    info = mlflow_health()

    c1, c2, c3 = st.columns(3)

    c1.metric(
        "Experiments",
        info["experiments"]
    )

    c2.metric(
        "Runs",
        info["runs"]
    )

    c3.metric(
        "Registry Models",
        info["models"]
    )

    experiments = get_experiments()

    rows = []

    for exp in experiments:

        rows.append(
            {
                "Experiment":
                    exp.name,
                "ID":
                    exp.experiment_id
            }
        )

    st.dataframe(
        pd.DataFrame(rows),
        use_container_width=True
    )

    st.divider()

    st.subheader(
        "Ray Cluster"
    )

    info = ray_summary()

    c1, c2, c3, c4, c5 = st.columns(5)

    c1.metric(
        "Nodes",
        info["alive_nodes"]
    )

    c2.metric(
        "CPU",
        f"{info['cpu_total']:.0f}"
    )

    c3.metric(
        "GPU",
        f"{info['gpu_total']:.0f}"
    )

    c4.metric(
        "Memory",
        f"{info['memory_total_gb']:.1f} GiB"
    )

    c5.metric(
        "Jobs",
        info["job_count"]
    )