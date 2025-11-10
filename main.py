import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from rich.console import Console
from rich.markdown import Markdown
from rich.logging import RichHandler

from src.export_file import export_dataframe, ExportFormats
from src.query_weather import analysis_weather
from src.transform_weather import transform_weather
from src.fetch_weather import fetch_weather

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
        logger.info("Creating directories")
        create_dirs()
        logger.info("Directories created")

        logger.info("Fetching weather data")
        data = fetch_weather(latitude=LATITUDE, longitude=LONGITUDE)
        df = pd.DataFrame(data["hourly"])
        logger.info("Weather data fetched")

        logger.info("Saving raw data to CSV")
        filename = RAW_DIR / f"weather_{datetime.now():%Y%m%d_%H%M}.csv"
        export_dataframe(df, filename, ExportFormats.CSV)
        logger.info("Raw data saved to CSV")

        logger.info("Transforming data")
        df = transform_weather(df)
        logger.info("Data transformed")

        logger.info("Saving transformed data to Parquet")
        filename = CLEAN_DIR / f"weather_{datetime.now():%Y%m%d_%H%M}.parquet"
        export_dataframe(df, filename, ExportFormats.PARQUET)
        logger.info("Transformed data saved to Parquet")

        logger.info(f"Querying data from {filename}")
        df = analysis_weather(filename)
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
