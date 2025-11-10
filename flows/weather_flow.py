import os

from prefect import flow, task, unmapped
from prefect_aws import AwsCredentials
from prefect_aws.s3 import S3Bucket

from src.orchestration import (
    orchestrate_weather_analysis,
    orchestrate_weather_collect,
    orchestrate_weather_plot,
    orchestrate_weather_transform,
)
from src.interest_region import REGIONS, Region

try:
    s3_bucket_block = S3Bucket.load("aws-bucket")
    aws_credentials = s3_bucket_block.credentials

    os.environ["BUCKET_NAME"] = s3_bucket_block.bucket_name
    os.environ["AWS_ACCESS_KEY_ID"] = aws_credentials.aws_access_key_id
    os.environ["AWS_SECRET_ACCESS_KEY"] = str(
        aws_credentials.aws_secret_access_key.get_secret_value()
    )

    os.environ["REGION_NAME"] = aws_credentials.region_name or "sa-east-1"
except ValueError as e:
    raise ValueError("AWS credentials/S3 bucket not found") from e


@task(retries=3)
def orchestrate_weather_collect_task(region: Region, s3_path: str) -> dict:
    return orchestrate_weather_collect(region, s3_path)


@task(retries=3)
def orchestrate_weather_transform_task(ctx: dict, s3_path: str) -> dict:
    return orchestrate_weather_transform(ctx, s3_path)


@task(retries=3, tags=["analysis"])
def orchestrate_weather_analysis_task(ctx: dict, s3_path: str) -> dict:
    return orchestrate_weather_analysis(ctx, s3_path)


@task(retries=3, tags=["plot"])
def orchestrate_weather_plot_task(ctx: dict, s3_path: str) -> dict:
    return orchestrate_weather_plot(ctx, s3_path)


@flow(log_prints=True, name="weather-flow")
def main():
    bucket_name = os.getenv("BUCKET_NAME")
    raw_s3_path = unmapped(f"s3://{bucket_name}/raw")
    clean_s3_path = unmapped(f"s3://{bucket_name}/clean")
    analysis_s3_path = unmapped(f"s3://{bucket_name}/analysis")
    plot_s3_path = unmapped(f"s3://{bucket_name}/plot")

    collect_results = orchestrate_weather_collect_task.map(
        REGIONS,
        raw_s3_path,
    )
    transform_results = orchestrate_weather_transform_task.map(
        collect_results,
        clean_s3_path,
    )
    analysis_results = orchestrate_weather_analysis_task.map(
        transform_results,
        analysis_s3_path,
    )

    orchestrate_weather_plot_task.map(analysis_results, plot_s3_path)


if __name__ == "__main__":
    main.serve(
        name="weather-flow",
        cron="0 0 * * *",
    )
