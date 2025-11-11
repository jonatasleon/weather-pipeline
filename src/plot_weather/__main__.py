import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

from . import plot_weather


class Namespace(argparse.Namespace):
    analysis_filepath: str
    output: str | None


def parse_args() -> Namespace:
    parser = argparse.ArgumentParser(description="Plot weather data")
    parser.add_argument(
        "analysis_filepath", type=str, help="Path to the analysis data file"
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help="Path to the plot data file (default: weather_plot_{name}_{date}.png)",
    )
    return parser.parse_args(namespace=Namespace())


def extract_name_and_date(analysis_filepath: str) -> tuple[str, str]:
    stem = Path(analysis_filepath).stem
    *_, name, date = stem.split("_")
    return name, datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")


def main():
    args = parse_args()

    name, date = extract_name_and_date(args.analysis_filepath)
    plot_ctx = {"region": name, "date": date}

    output = args.output
    if output is None:
        output = f"weather_plot_{name}_{date}.png"

    df = pd.read_csv(args.analysis_filepath)

    with open(output, "wb") as output_file:
        plot_weather(
            df=df,
            plot_ctx=plot_ctx,
            output_file=output_file,
        )
    print(f"Plot saved to {output}")


if __name__ == "__main__":
    main()
