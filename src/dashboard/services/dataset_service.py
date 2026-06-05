import pandas as pd

def result_distribution(df):

    return {
        "white_win":
            (df["result"] == "1-0").sum(),

        "black_win":
            (df["result"] == "0-1").sum(),

        "draw":
            (df["result"] == "1/2-1/2").sum()
    }


def elo_series(df):

    return pd.concat(
        [
            df["white_elo"],
            df["black_elo"]
        ],
        ignore_index=True
    )

def normalize_termination(x):

    x = str(x).lower()

    terminations = {
        "won by checkmate": "Checkmate",
        "won by resignation": "Resignation",
        "won on time": "Time",
        "abandoned": "Abandoned",

        "drawn by agreement": "Agreement",
        "drawn by stalemate": "Stalemate",
        "drawn by insufficient material": "Insufficient Material",
        "drawn by 50-move rule": "50-move Rule",
        "drawn by repetition": "Repetition"
    }
    for key, value in terminations.items():

        if key in x:
            return value
        
    return "Other"

def termination_distribution(df):

    df["termination_type"] = df["termination"].apply(
        normalize_termination
    )

    return df["termination_type"].value_counts().to_dict()

def add_termination_type(df):

    tmp = df.copy()

    tmp["termination_type"] = (
        tmp["termination"]
        .fillna("")
        .apply(normalize_termination)
    )

    return tmp