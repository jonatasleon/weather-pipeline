def create_s3_path(bucket_name: str, *paths: str) -> str:
    return f"s3://{bucket_name}/{'/'.join(paths)}"
