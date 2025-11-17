import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from rich.console import Console
from rich.logging import RichHandler

from src.orchestration import (
    orchestrate_weather_collect,
    orchestrate_weather_transform,
    orchestrate_weather_analysis,
)
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
        result = orchestrate_weather_collect(
            region=dict(
                name="São Paulo",
                latitude=LATITUDE,
                longitude=LONGITUDE,
            ),
            s3_base_path=create_s3_path(bucket_name, "history/raw"),
        )
        logger.info(f"Data collected for {result['region']}: {result['s3_path']!r}")
        result = orchestrate_weather_transform(
            result=result,
            s3_base_path=create_s3_path(bucket_name, "history/clean"),
        )
        logger.info(f"Data transformed for {result['region']}: {result['s3_path']!r}")
        result = orchestrate_weather_analysis(
            result,
            create_s3_path(bucket_name, "history/analysis"),
        )
        logger.info(f"Data analyzed for {result['region']}: {result['s3_path']!r}")
    except Exception:
        logger.error("Error occurred", exc_info=True)
        console.print_exception(show_locals=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
