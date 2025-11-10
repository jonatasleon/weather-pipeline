from enum import StrEnum
from io import BytesIO

import boto3
import pandas as pd
from botocore.client import BaseClient


class ExportFormats(StrEnum):
    CSV = "csv"
    PARQUET = "parquet"


def to_csv(
    dataframe: pd.DataFrame,
    s3_path: str,
    s3_client: BaseClient,
):
    """Export DataFrame to CSV in S3."""
    bucket, key = _parse_s3_path(s3_path)

    buffer = BytesIO()
    dataframe.to_csv(buffer, index=False)
    buffer.seek(0)

    s3_client.upload_fileobj(buffer, bucket, key)


def to_parquet(
    dataframe: pd.DataFrame,
    s3_path: str,
    s3_client: BaseClient,
):
    """Export DataFrame to Parquet in S3."""
    bucket, key = _parse_s3_path(s3_path)

    buffer = BytesIO()
    dataframe.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3_client.upload_fileobj(buffer, bucket, key)


EXPORTER_MAP = {
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


def _parse_s3_path(s3_path: str) -> tuple[str, str]:
    """Parse S3 path into bucket and key."""
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}. Must start with 's3://'")

    path_without_prefix = s3_path[5:]  # Remove 's3://'
    try:
        bucket, key = path_without_prefix.split("/", 1)
    except ValueError as e:
        raise ValueError(
            f"Invalid S3 path: {s3_path}. Format should be 's3://bucket/key'"
        ) from e

    if not bucket or not key:
        raise ValueError(
            f"Invalid S3 path: {s3_path}. Bucket name or key cannot be empty"
        )

    return bucket, key


def export_dataframe(
    dataframe: pd.DataFrame,
    s3_path: str,
    format: ExportFormats | None = None,
):
    """
    Export DataFrame to S3.

    Args:
        dataframe: The DataFrame to export.
        s3_path: S3 path (str starting with 's3://').
        format: Export format (CSV or PARQUET). If None, inferred from filename extension.
        s3_client: Optional boto3 S3 client. If None, a default client is created.

    Examples:
        export_dataframe(df, "s3://my-bucket/data.csv")
        export_dataframe(df, "s3://my-bucket/data.parquet", s3_client=my_s3_client)
    """
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}. Must start with 's3://'")

    if format is None:
        format = _get_format_by_filename_extension(s3_path)

    s3_client = boto3.client("s3")

    try:
        exporter_function = EXPORTER_MAP[str(format).lower()]
        exporter_function(dataframe, s3_path, s3_client)
    except KeyError as e:
        raise ValueError(
            f"Invalid format: {s3_path}. Valid formats are: {', '.join(ExportFormats)}."
        ) from e


def download_file(
    s3_path: str,
    output_file: BytesIO,
    s3_client: BaseClient | None = None,
):
    if s3_client is None:
        s3_client = boto3.client("s3")

    bucket, key = _parse_s3_path(s3_path)

    s3_client.download_fileobj(bucket, key, output_file)
    output_file.seek(0)
