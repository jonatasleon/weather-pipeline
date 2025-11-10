import logging
from datetime import datetime
from pathlib import Path
from typing import TypedDict

import pandas as pd

from src.export_file import export_dataframe
from src.fetch_weather import fetch_weather
from src.interest_region import Region
from src.plot_weather import plot_weather
from src.query_weather import analysis_weather
from src.transform_weather import transform_weather


logger = logging.getLogger(__name__)


class Result(TypedDict):
    region: str
    file_path: Path


def orchestrate_weather_collect(region: Region, output_dir: Path) -> Result:
    logger.info(f"Orchestrating weather collect for {region['name']}")
    latitude, longitude = region["latitude"], region["longitude"]
    data = fetch_weather(latitude, longitude)
    logger.info(f"Weather data fetched for {latitude}, {longitude}")

    df = pd.DataFrame(data["hourly"])

    raw_filename = f"weather_{region['name']}_{datetime.now():%Y%m%d}.csv"
    raw_file_path = output_dir / raw_filename
    export_dataframe(df, raw_file_path)
    logger.info(f"Raw data saved to {raw_file_path}")

    return {
        "region": region["name"],
        "file_path": raw_file_path,
    }


def orchestrate_weather_transform(result: Result, output_dir: Path) -> Result:
    df = pd.read_csv(result["file_path"])
    df = transform_weather(df)
    logger.info(f"Data transformed for {result['file_path']}")

    clean_filename = f"{result["file_path"].stem}.parquet"
    clean_file_path = output_dir / clean_filename

    export_dataframe(df, clean_file_path)
    logger.info(f"Transformed data saved to {clean_file_path}")

    return {
        "region": result["region"],
        "file_path": clean_file_path,
    }


def orchestrate_weather_analysis(result: Result, output_dir: Path) -> Result:
    df = analysis_weather(result["file_path"])
    logger.info(f"Data analyzed for {result["file_path"]}")

    analysis_filename = f"{result["file_path"].stem}.csv"
    analysis_file_path = output_dir / analysis_filename
    export_dataframe(df, analysis_file_path)
    logger.info(f"Analyzed data saved to {analysis_file_path}")

    return {
        "region": result["region"],
        "file_path": analysis_file_path,
    }


def orchestrate_weather_plot(result: Result, output_dir: Path) -> Result:
    df = pd.read_csv(result["file_path"])
    logger.info(f"Data plotted for {result["file_path"]}")

    plot_filename = f"{result["file_path"].stem}.png"
    plot_file_path = output_dir / plot_filename
    plot_weather(df, plot_file_path)
    logger.info(f"Plotted data saved to {plot_file_path}")

    return {
        "region": result["region"],
        "file_path": plot_file_path,
    }
