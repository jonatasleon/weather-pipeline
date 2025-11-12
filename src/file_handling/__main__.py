import typer
from dotenv import load_dotenv

from . import download_file, download_all_from_bucket, list_bucket_objects

load_dotenv()


app = typer.Typer(help="File handling")


@app.command()
def download(
    s3_path: str = typer.Argument(..., help="Path to the file in S3"),
    output: str | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Path to the output file (default: filename of the S3 path)",
    ),
):
    """Download a single file from S3."""
    if output is None:
        output = s3_path.split("/")[-1]
    download_file(s3_path, output)
    typer.echo(f"File downloaded to {output}")


@app.command()
def download_all(
    bucket_name: str = typer.Argument(..., help="Name of the S3 bucket"),
    output_dir: str = typer.Option(
        "data/raw",
        "--output-dir",
        "-o",
        help="Directory to save downloaded files",
    ),
    prefix: str = typer.Option(
        "",
        "--prefix",
        "-p",
        help="Optional prefix to filter objects",
    ),
):
    """Download all files from an S3 bucket."""
    typer.echo(f"Downloading all files from bucket: {bucket_name}")
    if prefix:
        typer.echo(f"Filtering by prefix: {prefix}")
    
    downloaded_files = download_all_from_bucket(bucket_name, output_dir, prefix)
    
    typer.echo(f"\nDownloaded {len(downloaded_files)} file(s) to {output_dir}")
    for file_path in downloaded_files:
        typer.echo(f"  - {file_path}")


@app.command()
def list(
    bucket_name: str = typer.Argument(..., help="Name of the S3 bucket"),
    prefix: str = typer.Option(
        "",
        "--prefix",
        "-p",
        help="Optional prefix to filter objects",
    ),
):
    """List all objects in an S3 bucket."""
    objects = list_bucket_objects(bucket_name, prefix)
    typer.echo(f"Found {len(objects)} object(s) in bucket '{bucket_name}'")
    if prefix:
        typer.echo(f"Filtered by prefix: {prefix}")
    typer.echo()
    for obj in objects:
        typer.echo(f"  - {obj}")


if __name__ == "__main__":
    app()
