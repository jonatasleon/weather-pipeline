import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from rich.console import Console
from rich.markdown import Markdown
from rich.logging import RichHandler

from src.export_file import export_dataframe, ExportFormats
from src.query_weather import analysis_weather
from src.transform_weather import transform_weather
from src.fetch_weather import fetch_weather

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
        # logger.info("Creating directories")
        # create_dirs()
        # logger.info("Directories created")
        bucket_name = os.getenv("BUCKET_NAME")

        logger.info("Fetching weather data")
        data = fetch_weather(latitude=LATITUDE, longitude=LONGITUDE)
        df = pd.DataFrame(data["hourly"])
        logger.info("Weather data fetched")

        logger.info("Saving raw data to CSV")
        raw_s3_path = f"s3://{bucket_name}/raw/weather_{datetime.now():%Y%m%d_%H%M}.csv"
        export_dataframe(df, raw_s3_path)
        logger.info("Raw data saved to CSV")

        logger.info("Transforming data")
        df = transform_weather(df)
        logger.info("Data transformed")

        logger.info("Saving transformed data to Parquet")
        clean_s3_path = (
            f"s3://{bucket_name}/clean/weather_{datetime.now():%Y%m%d_%H%M}.parquet"
        )
        export_dataframe(df, clean_s3_path, ExportFormats.PARQUET)
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
