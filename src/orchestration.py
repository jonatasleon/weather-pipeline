import json
import logging
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import TypedDict

import pandas as pd

from src.fetch_weather import fetch_weather_history
from src.file_handling import upload_dataframe, upload_fileobj
from src.interest_region import Region
from src.plot_weather import plot_weather
from src.query_weather import analysis_weather
from src.transform_weather import transform_weather


logger = logging.getLogger(__name__)


class Result(TypedDict):
    region: str
    date: str
    s3_path: str


def orchestrate_weather_collect(region: Region, s3_base_path: str) -> Result:
    logger.info(f"Orchestrating weather collect for {region['name']}")
    latitude, longitude = region["latitude"], region["longitude"]
    data = fetch_weather_history(latitude, longitude)
    logger.info(f"Weather data fetched for {region['name']}")

    now = f"{datetime.now():%Y%m%d_%H%M}"
    stem = f"weather_history_{region['name']}_{now}"
    raw_file_path = f"{s3_base_path}/{stem}.json"
    with BytesIO() as buffer:
        json.dump(data, buffer, indent=None)
        buffer.seek(0)
        upload_fileobj(buffer, raw_file_path)
    logger.info(f"Raw data saved to {raw_file_path}")

    return {
        "region": region["name"],
        "date": now,
        "s3_path": raw_file_path,
    }


def orchestrate_weather_transform(result: Result, s3_base_path: str) -> Result:
    data = json.load(result["s3_path"])
    df = pd.DataFrame(data["hourly"])
    df = transform_weather(df)
    logger.info(f"Data transformed for {result['s3_path']}")

    stem = Path(result["s3_path"]).stem
    clean_filename = f"{stem}.parquet"
    clean_file_path = f"{s3_base_path}/{clean_filename}"

    upload_dataframe(df, clean_file_path)
    logger.info(f"Transformed data saved to {clean_file_path}")

    return {
        "region": result["region"],
        "date": result["date"],
        "s3_path": clean_file_path,
    }


def orchestrate_weather_analysis(result: Result, s3_base_path: str) -> Result:
    df = analysis_weather(result["s3_path"])
    logger.info(f"Data analyzed for {result["s3_path"]}")

    stem = Path(result["s3_path"]).stem
    analysis_filename = f"{stem}.csv"
    analysis_file_path = f"{s3_base_path}/{analysis_filename}"
    upload_dataframe(df, analysis_file_path)
    logger.info(f"Analyzed data saved to {analysis_file_path}")

    return {
        "region": result["region"],
        "date": result["date"],
        "s3_path": analysis_file_path,
    }


def orchestrate_weather_plot(result: Result, s3_base_path: str) -> Result:
    df = pd.read_csv(result["s3_path"])
    logger.info(f"Data plotted for {result["s3_path"]}")

    stem = Path(result["s3_path"]).stem
    plot_filename = f"{stem}.png"
    plot_file_path = f"{s3_base_path}/{plot_filename}"
    with BytesIO() as output_file:
        plot_weather(df, result, output_file)
        output_file.seek(0)
        upload_fileobj(output_file, plot_file_path)
    logger.info(f"Plotted data saved to {plot_file_path}")

    return {
        "region": result["region"],
        "s3_path": plot_file_path,
    }
