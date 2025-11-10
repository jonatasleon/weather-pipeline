from io import BytesIO
import logging
from datetime import datetime
from pathlib import Path
from typing import TypedDict

import pandas as pd

from src.export_file import download_file, export_dataframe
from src.fetch_weather import fetch_weather
from src.interest_region import Region
from src.plot_weather import plot_weather
from src.query_weather import analysis_weather
from src.transform_weather import transform_weather


logger = logging.getLogger(__name__)


class Result(TypedDict):
    region: str
    s3_path: str


def orchestrate_weather_collect(region: Region, s3_path: str) -> Result:
    logger.info(f"Orchestrating weather collect for {region['name']}")
    latitude, longitude = region["latitude"], region["longitude"]
    data = fetch_weather(latitude, longitude)
    logger.info(f"Weather data fetched for {latitude}, {longitude}")

    df = pd.DataFrame(data["hourly"])

    stem = f"weather_{region['name']}_{datetime.now():%Y%m%d}"
    raw_filename = f"{stem}.csv"
    raw_file_path = f"{s3_path}/{raw_filename}"
    export_dataframe(df, raw_file_path)
    logger.info(f"Raw data saved to {raw_file_path}")

    return {
        "region": region["name"],
        "s3_path": raw_file_path,
    }


def orchestrate_weather_transform(result: Result, s3_path: str) -> Result:
    with BytesIO() as io_buffer:
        download_file(result["s3_path"], io_buffer)
        df = pd.read_csv(io_buffer)
    df = transform_weather(df)
    logger.info(f"Data transformed for {result['s3_path']}")

    stem = Path(result["s3_path"]).stem
    clean_filename = f"{stem}.parquet"
    clean_file_path = f"{s3_path}/{clean_filename}"

    export_dataframe(df, clean_file_path)
    logger.info(f"Transformed data saved to {clean_file_path}")

    return {
        "region": result["region"],
        "s3_path": clean_file_path,
    }


def orchestrate_weather_analysis(result: Result, s3_path: str) -> Result:
    df = analysis_weather(result["s3_path"])
    logger.info(f"Data analyzed for {result["s3_path"]}")

    stem = Path(result["s3_path"]).stem
    analysis_filename = f"{stem}.csv"
    analysis_file_path = f"{s3_path}/{analysis_filename}"
    export_dataframe(df, analysis_file_path)
    logger.info(f"Analyzed data saved to {analysis_file_path}")

    return {
        "region": result["region"],
        "s3_path": analysis_file_path,
    }


def orchestrate_weather_plot(result: Result, s3_path: str) -> Result:
    df = pd.read_csv(result["s3_path"])
    logger.info(f"Data plotted for {result["s3_path"]}")

    stem = Path(result["s3_path"]).stem
    plot_filename = f"{stem}.png"
    plot_file_path = f"{s3_path}/{plot_filename}"
    plot_weather(df, plot_file_path)
    logger.info(f"Plotted data saved to {plot_file_path}")

    return {
        "region": result["region"],
        "s3_path": plot_file_path,
    }
