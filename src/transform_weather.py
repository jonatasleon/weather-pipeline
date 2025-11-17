## transform_weather.py
import polars as pl


def transform_weather(df: pl.DataFrame):
    df = df.with_columns(
        [
            pl.col("time").str.to_datetime().alias("time"),
            pl.col("temperature_2m").cast(pl.Float64),
            pl.col("windspeed_10m").cast(pl.Float64),
            pl.col("relative_humidity_2m").cast(pl.Float64),
        ]
    )
    return df
