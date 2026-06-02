import streamlit as st
import plotly.express as px

from services.minio_service import (
    load_games_df
)


def render():

    st.subheader(
        "Self-play Dataset"
    )

    df = load_games_df()

    if df.empty:

        st.warning(
            "No game data found"
        )

        return

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Games",
        len(df)
    )

    col2.metric(
        "Avg Moves",
        round(
            df["moves"].mean(),
            2
        )
    )

    col3.metric(
        "Avg Elo",
        round(
            (
                df["white_elo"].mean()
                + df["black_elo"].mean()
            )
            / 2,
            2
        )
    )

    fig = px.histogram(
        df,
        x="moves",
        nbins=50,
        title="Game Length"
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )