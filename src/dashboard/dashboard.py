import streamlit as st

from tabs.pipeline import render as render_pipeline
from tabs.data import render as render_data
from tabs.training import render as render_training
from tabs.registry import render as render_registry
from tabs.jobs import render as render_jobs


st.set_page_config(
    page_title="Chess AI Dashboard",
    layout="wide"
)

st.title(
    "♟️ Chess AI Dashboard"
)

with st.sidebar:

    st.header(
        "Dashboard"
    )

    if st.button(
        "Refresh"
    ):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()


tab1, tab2, tab3, tab4, tab5 = st.tabs(
    [
        "Pipeline",
        "Dataset",
        "Training",
        "Registry",
        "Ray Jobs"
    ]
)

with tab1:
    render_pipeline()

with tab2:
    render_data()

with tab3:
    render_training()

with tab4:
    render_registry()

with tab5:
    render_jobs()