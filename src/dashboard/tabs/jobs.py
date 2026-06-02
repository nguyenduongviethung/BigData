import pandas as pd
import requests
import streamlit as st

RAY_DASHBOARD_URL = "http://ray-head:8265"


@st.cache_data(ttl=10)
def get_jobs():

    try:

        response = requests.get(
            f"{RAY_DASHBOARD_URL}/api/jobs/",
            timeout=5
        )

        response.raise_for_status()

        return response.json()

    except Exception as e:

        st.error(
            f"Failed to query Ray Jobs: {e}"
        )

        return []


def render():

    st.subheader("🚀 Ray Jobs")

    jobs = get_jobs()

    if not jobs:

        st.info(
            "No Ray jobs found"
        )
        return

    rows = []

    for job in jobs:

        rows.append(
            {
                "Job ID": job.get(
                    "job_id",
                    "-"
                ),
                "Status": job.get(
                    "status",
                    "-"
                ),
                "Entry Point": job.get(
                    "entrypoint",
                    "-"
                ),
                "Start Time": job.get(
                    "start_time",
                    "-"
                ),
                "End Time": job.get(
                    "end_time",
                    "-"
                )
            }
        )

    df = pd.DataFrame(rows)

    st.dataframe(
        df,
        use_container_width=True
    )

    status_counts = (
        df["Status"]
        .value_counts()
        .reset_index()
    )

    status_counts.columns = [
        "Status",
        "Count"
    ]

    st.bar_chart(
        status_counts.set_index(
            "Status"
        )
    )