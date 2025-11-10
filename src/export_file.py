from enum import StrEnum
from pathlib import Path

import pandas as pd


class ExportFormats(StrEnum):
    CSV = "csv"
    PARQUET = "parquet"


def to_csv(dataframe: pd.DataFrame, filename: str):
    dataframe.to_csv(filename, index=False)


def to_parquet(dataframe: pd.DataFrame, filename: str):
    dataframe.to_parquet(filename, index=False)


FORMAT_MAP = {
    ExportFormats.CSV: to_csv,
    ExportFormats.PARQUET: to_parquet,
}


def _get_format_by_filename_extension(filename: str) -> ExportFormats:
    try:
        return ExportFormats(filename.split(".")[-1])
    except ValueError as e:
        raise ValueError(
            f"Invalid format: {filename}. Valid formats are: {', '.join(ExportFormats)}."
        ) from e


def export_dataframe(
    dataframe: pd.DataFrame,
    filename: str | Path,
    format: ExportFormats | None = None,
):
    if isinstance(filename, Path):
        filename = str(filename)

    if format is None:
        format = _get_format_by_filename_extension(filename)

    try:
        FORMAT_MAP[str(format).lower()](dataframe, filename)
    except KeyError as e:
        raise ValueError(
            f"Invalid format: {filename}. Valid formats are: {', '.join(ExportFormats)}."
        ) from e
