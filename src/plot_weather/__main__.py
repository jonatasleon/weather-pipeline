from datetime import datetime
from pathlib import Path

import pandas as pd
import typer

from . import plot_weather

app = typer.Typer(help="Plot weather data")


def extract_name_and_date(analysis_filepath: str) -> tuple[str, str]:
    stem = Path(analysis_filepath).stem
    *_, name, date = stem.split("_")
    return name, datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")


@app.command()
def main(
    analysis_filepath: str = typer.Argument(..., help="Path to the analysis data file"),
    output: str | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Path to the plot data file (default: weather_plot_{name}_{date}.png)",
    ),
) -> None:
    name, date = extract_name_and_date(analysis_filepath)
    plot_ctx = {"region": name, "date": date}

    if output is None:
        output = f"weather_plot_{name}_{date}.png"

    df = pd.read_csv(analysis_filepath)

    with open(output, "wb") as output_file:
        plot_weather(
            df=df,
            plot_ctx=plot_ctx,
            output_file=output_file,
        )
    typer.echo(f"Plot saved to {output}")


if __name__ == "__main__":
    app()
