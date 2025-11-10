from pathlib import Path

from prefect import flow, task, unmapped

from src.orchestration import (
    orchestrate_weather_analysis,
    orchestrate_weather_collect,
    orchestrate_weather_plot,
    orchestrate_weather_transform,
)
from src.interest_region import REGIONS, Region


BASE_DIR = Path(__file__).parents[1] / "data"


@task(retries=3)
def orchestrate_weather_collect_task(region: Region, base_dir: Path) -> dict:
    output_dir = base_dir / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)
    return orchestrate_weather_collect(region, output_dir)


@task(retries=3)
def orchestrate_weather_transform_task(ctx: dict, base_dir: Path) -> dict:
    output_dir = base_dir / "clean"
    output_dir.mkdir(parents=True, exist_ok=True)
    return orchestrate_weather_transform(ctx, output_dir)


@task(retries=3, tags=["analysis"])
def orchestrate_weather_analysis_task(ctx: dict, base_dir: Path) -> dict:
    output_dir = base_dir / "analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    return orchestrate_weather_analysis(ctx, output_dir)


@task(retries=3, tags=["plot"])
def orchestrate_weather_plot_task(ctx: dict, base_dir: Path) -> dict:
    output_dir = base_dir / "plots"
    output_dir.mkdir(parents=True, exist_ok=True)
    return orchestrate_weather_plot(ctx, output_dir)


@flow(log_prints=True, name="weather-flow")
def main():
    unmapped_base_dir = unmapped(BASE_DIR)

    collect_results = orchestrate_weather_collect_task.map(
        REGIONS,
        unmapped_base_dir,
    )
    transform_results = orchestrate_weather_transform_task.map(
        collect_results,
        unmapped_base_dir,
    )
    analysis_results = orchestrate_weather_analysis_task.map(
        transform_results,
        unmapped_base_dir,
    )

    orchestrate_weather_plot_task.map(analysis_results, unmapped_base_dir)


if __name__ == "__main__":
    main.serve(
        name="weather-flow",
        cron="0 0 * * *",
    )
