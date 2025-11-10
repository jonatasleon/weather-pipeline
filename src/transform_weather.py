## transform_weather.py
import pandas as pd


def transform_weather(df: pd.DataFrame):
    df["time"] = pd.to_datetime(df["time"])
    df["temperature_2m"] = df["temperature_2m"].astype(float)
    df["day"] = df["time"].dt.date

    return df
