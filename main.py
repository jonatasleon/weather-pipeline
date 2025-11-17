from io import BytesIO
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
from dotenv import load_dotenv
from rich.console import Console
from rich.markdown import Markdown
from rich.logging import RichHandler

from src.file_handling import upload_dataframe, ExportFormats, upload_fileobj
from src.fetch_weather import fetch_weather_history
from src.query_weather import analysis_weather
from src.transform_weather import transform_weather
from src.utils import create_s3_path

load_dotenv()

console = Console()


LATITUDE, LONGITUDE = -23.55, -46.63  # São Paulo, Brazil

BASE_DIR = Path(__file__).parent / "data"
RAW_DIR = BASE_DIR / "raw"
CLEAN_DIR = BASE_DIR / "clean"


def setup_logging():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = RichHandler(console=console, rich_tracebacks=True)
    logger.addHandler(handler)
    return logger


def create_dirs():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)


def main():
    logger = setup_logging()
    try:
        bucket_name = os.getenv("BUCKET_NAME")

        logger.info("Fetching weather data")
        data = fetch_weather_history(
            latitude=LATITUDE,
            longitude=LONGITUDE,
            start_date=datetime.now() - timedelta(days=30),
            end_date=datetime.now(),
        )
        with BytesIO() as buffer:
            buffer.write(json.dumps(data).encode())
            buffer.seek(0)
            filename = f"weather_history_{datetime.now():%Y%m%d_%H%M}.json"
            upload_fileobj(buffer, create_s3_path(bucket_name, "history/raw", filename))

        df = pl.from_dict(data["hourly"])
        logger.info("Weather data fetched")

        logger.info("Transforming data")
        df = transform_weather(df)
        logger.info("Data transformed")

        logger.info("Saving transformed data to Parquet")
        clean_s3_path = create_s3_path(
            bucket_name,
            "history/clean",
            f"weather_{datetime.now():%Y%m%d_%H%M}.parquet",
        )
        upload_dataframe(df, clean_s3_path, ExportFormats.PARQUET)
        logger.info("Transformed data saved to Parquet")

        logger.info(f"Querying data from {clean_s3_path}")
        df = analysis_weather(clean_s3_path)
        logger.info("Query result fetched")

        logger.info("Printing query result")
        output_md = Markdown(df.to_markdown())
        console.print(output_md)
        logger.info("Query result printed")
    except Exception:
        logger.error("Error occurred", exc_info=True)
        console.print_exception(show_locals=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
