from enum import StrEnum
from io import BytesIO
from pathlib import Path
from typing import BinaryIO

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


def upload_fileobj(
    io_buffer: BytesIO,
    s3_path: str,
    s3_client: BaseClient | None = None,
):
    if s3_client is None:
        s3_client = boto3.client("s3")

    bucket, key = _parse_s3_path(s3_path)

    s3_client.upload_fileobj(io_buffer, bucket, key)


def download_file(
    s3_path: str,
    output_file: BinaryIO | str | Path,
    s3_client: BaseClient | None = None,
):
    """
    Download a file from S3.

    Args:
        s3_path: S3 path (str starting with 's3://').
        output_file: Output file path (str or Path) or file-like object (BinaryIO).
        s3_client: Optional boto3 S3 client. If None, a default client is created.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")

    bucket, key = _parse_s3_path(s3_path)

    # If output_file is a string or Path, open it as a file
    if isinstance(output_file, (str, Path)):
        with open(output_file, "wb") as f:
            s3_client.download_fileobj(bucket, key, f)
    else:
        # It's a file-like object
        s3_client.download_fileobj(bucket, key, output_file)
        if hasattr(output_file, "seek"):
            output_file.seek(0)


def list_bucket_objects(
    bucket_name: str,
    prefix: str = "",
    s3_client: BaseClient | None = None,
) -> list[str]:
    """
    List all objects in an S3 bucket.

    Args:
        bucket_name: Name of the S3 bucket.
        prefix: Optional prefix to filter objects.
        s3_client: Optional boto3 S3 client. If None, a default client is created.

    Returns:
        List of S3 paths (s3://bucket/key) for all objects.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")

    objects = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                key = obj["Key"]
                objects.append(f"s3://{bucket_name}/{key}")

    return objects


def download_all_from_bucket(
    bucket_name: str,
    output_dir: str | Path,
    prefix: str = "",
    s3_client: BaseClient | None = None,
) -> list[str]:
    """
    Download all files from an S3 bucket to a local directory.

    Args:
        bucket_name: Name of the S3 bucket.
        output_dir: Local directory to save downloaded files.
        prefix: Optional prefix to filter objects.
        s3_client: Optional boto3 S3 client. If None, a default client is created.

    Returns:
        List of local file paths where files were downloaded.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    downloaded_files = []
    objects = list_bucket_objects(bucket_name, prefix, s3_client)

    for s3_path in objects:
        bucket, key = _parse_s3_path(s3_path)

        # Create local directory structure matching S3 prefix structure
        key_parts = key.split("/")
        filename = key_parts[-1]

        # If there are subdirectories in the key, preserve them
        if len(key_parts) > 1:
            subdir = output_path / "/".join(key_parts[:-1])
            subdir.mkdir(parents=True, exist_ok=True)
            local_path = subdir / filename
        else:
            local_path = output_path / filename

        # Download the file
        s3_client.download_file(bucket, key, str(local_path))
        downloaded_files.append(str(local_path))

    return downloaded_files
