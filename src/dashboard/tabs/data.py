import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from services.minio_service import (
    load_dataset_df
)

from services.dataset_service import (
    result_distribution,
    termination_distribution,
    add_termination_type
)

from services.dataset_service import (
    elo_series
)


def render():

    st.subheader(
        "Game Statistics"
    )

    df = load_dataset_df()

    if df.empty:

        st.warning(
            "No game data found"
        )

        return

    stats = result_distribution(df)

    total_games = len(df)

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Games",
        total_games
    )

    col2.metric(
        "White Wins",
        stats["white_win"]
    )

    col3.metric(
        "Black Wins",
        stats["black_win"]
    )

    col4.metric(
        "Draws",
        stats["draw"]
    )

    st.divider()

    st.subheader("Outcome analysis")

    col_left, col_right = st.columns(2)

    with col_left:

        fig = go.Figure(
            data=[
                go.Pie(
                    labels=[
                        "White Win",
                        "Black Win",
                        "Draw"
                    ],
                    values=[
                        stats["white_win"],
                        stats["black_win"],
                        stats["draw"]
                    ],
                    hole=0.3
                )
            ],
            layout={
                "title": "Game Outcomes"
            }

        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

    with col_right:
        term_stats = termination_distribution(df)

        fig = go.Figure(
            data=[
                go.Pie(
                    labels=list(term_stats.keys()),
                    values=list(term_stats.values()),
                    hole=0.3
                )
            ],
            layout={
                "title": "Termination Types"
            }
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

    st.divider()
    st.subheader("Rating analysis")

    col_left, col_right = st.columns(2)
    
    with col_left:

        elo = elo_series(df)

        fig = px.histogram(
            x=elo,
            nbins=40,
            title="ELO Distribution",
            labels={
                "x": "ELO",
                "count": "Games"
            }
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

    with col_right:
        elo_diff = df["white_elo"] - df["black_elo"]

        fig = px.histogram(
            x=elo_diff,
            nbins=40,
            title="ELO Difference",
            labels={
                "x": "ELO Difference",
                "count": "Games"
            }
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

    st.divider()
    st.subheader("Game analysis")

    col_left, col_right = st.columns(2)

    with col_left:
        fig = px.histogram(
            df,
            x="ply_count",
            nbins=50,
            title="Game Length Distribution",
            labels={
                "ply_count": "Ply Count",
                "count": "Games"
            }
        )
    
        st.plotly_chart(
            fig,
            use_container_width=True
        )

    with col_right:
        counts = (
            df["time_control"]
            .value_counts()
            .reset_index()
        )

        counts.columns = [
            "Time Control",
            "Games"
        ]

        fig = px.bar(
            counts,
            x="Time Control",
            y="Games",
            title="Time Control Distribution"
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

    st.divider()
    st.subheader("Opening analysis")

    col_left, col_right = st.columns(2)

    with col_left:
        df["eco_volume"] = (
            df["eco"]
            .str[0]
        )

        eco_volume = (
            df["eco"]
            .dropna()
            .str[0]
            .value_counts()
            .reset_index()
        )

        eco_volume.columns = [
            "Volume",
            "Games"
        ]

        fig = px.bar(
            eco_volume,
            x="Volume",
            y="Games",
            title="ECO Volume Distribution"
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )    

    with col_right:
        eco_top = (
            df["eco"]
            .dropna()
            .value_counts()
            .head(20)
            .reset_index()
        )

        eco_top.columns = [
            "ECO Code",
            "Games"
        ]

        fig = px.bar(
            eco_top,
            x="ECO Code",
            y="Games",
            title="Top ECO Codes"
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

    st.divider()
    st.subheader("Advanced opening analysis")

    col_left, col_right = st.columns(2)

    with col_left:
        eco_stats = (
            df.groupby("eco")
            .agg(
                games=("eco", "count"),
                white_win=(
                    "result",
                    lambda x: (x == "1-0").mean()
                )
            )
            .reset_index()
        )

        fig = px.bar(
            eco_stats,
            x="eco",
            y="white_win",
            title="White Win Rate by ECO Code",
            labels={
                "eco": "ECO Code",
                "white_win": "White Win Rate"
            }
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

    with col_right:
        eco_stats = (
            df.groupby("eco")
            .agg(
                games=("eco", "count"),
                avg_elo=(
                    "white_elo",
                    lambda x: ((x + df.loc[x.index, "black_elo"]) / 2).mean()
                )
            )
            .reset_index()
        )

        fig = px.bar(
            eco_stats,
            x="eco",
            y="avg_elo",
            title="Average ELO by ECO Code",
            labels={
                "eco": "ECO Code",
                "avg_elo": "Average ELO"
            }
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )


    st.divider()
    st.subheader("Termination analysis")

    col_left, col_right = st.columns(2)

    with col_left: # Termination vs Length
        df_term = add_termination_type(df)
        fig = px.box(
            df_term,
            x="termination_type",
            y="ply_count",
            title="Termination vs Game Length",
            labels={
                "termination_type": "Termination",
                "ply_count": "Ply Count"
            }
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

    with col_right: # Termination vs ELO
        df_term = add_termination_type(df)
        df_term["avg_elo"] = (df_term["white_elo"] + df_term["black_elo"]) / 2

        fig = px.box(
            df_term,
            x="termination_type",
            y="avg_elo",
            title="Termination vs Average Elo",
            labels={
                "termination_type": "Termination",
                "avg_elo": "Average Elo"
            }
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )
