from io import BytesIO
from typing import TypedDict

import matplotlib.pyplot as plt
import pandas as pd


class PlotConfig(TypedDict):
    time_var_name: str
    main_var_name: str
    max_var_name: str
    min_var_name: str
    main_var_label: str
    min_var_label: str
    max_var_label: str
    main_var_color: str
    min_var_color: str
    max_var_color: str
    title: str
    xlabel: str
    ylabel: str


def plot_variable(ax: plt.Axes, df: pd.DataFrame, config: PlotConfig) -> plt.Axes:
    ax.plot(
        df[config["time_var_name"]],
        df[config["main_var_name"]],
        label=config["main_var_label"],
        color=config["main_var_color"],
        marker="o",
        linewidth=2,
    )
    ax.plot(
        df[config["time_var_name"]],
        df[config["max_var_name"]],
        label=config["max_var_label"],
        color=config["max_var_color"],
        marker="^",
        linestyle="--",
        alpha=0.7,
    )
    ax.plot(
        df[config["time_var_name"]],
        df[config["min_var_name"]],
        label=config["min_var_label"],
        color=config["min_var_color"],
        marker="v",
        linestyle="--",
        alpha=0.7,
    )
    ax.fill_between(
        df[config["time_var_name"]],
        df[config["min_var_name"]],
        df[config["max_var_name"]],
        alpha=0.2,
        color=config["main_var_color"],
    )
    ax.set_title(config["title"])
    ax.set_xlabel(config["xlabel"])
    ax.set_ylabel(config["ylabel"])
    ax.legend(loc="best", fancybox=True, framealpha=0.2, prop={"size": 8})
    ax.grid(True, alpha=0.3)
    ax.tick_params(axis="x", rotation=45)
    return ax


def plot_weather(
    df: pd.DataFrame,
    plot_ctx: dict[str, str],
    output_file: BytesIO,
) -> None:
    """
    Plot weather data from a dataframe and save to the specified output path.

    Args:
        df: DataFrame containing weather analysis data with columns like
            day, avg_temp, min_temp, max_temp, avg_windspeed, avg_relative_humidity, etc.
        output_path: Path where the plot image will be saved (should be .png)
    """
    # Convert day column to datetime if it's not already
    df["day"] = pd.to_datetime(df["day"])

    # Create figure with subplots
    fig, axes = plt.subplots(3, 1, figsize=(9, 12), dpi=200, sharex=True)
    fig.suptitle(
        f"Weather Prediction for {plot_ctx['region']} on {plot_ctx['date']}",
        fontsize=16,
        fontweight="bold",
    )

    # Plot 1: Temperature (avg, min, max)
    ax1 = axes[0]
    plot_config = {
        "time_var_name": "day",
        "main_var_name": "avg_temp",
        "max_var_name": "max_temp",
        "min_var_name": "min_temp",
        "main_var_label": "Average Temperature",
        "min_var_label": "Min Temperature",
        "max_var_label": "Max Temperature",
        "main_var_color": "blue",
        "min_var_color": "red",
        "max_var_color": "green",
        "title": "Temperature Over Time",
        "xlabel": "Date",
        "ylabel": "Temperature (°C)",
    }
    plot_variable(ax1, df, plot_config)

    # Plot 2: Wind Speed
    ax2 = axes[1]
    plot_config = {
        "time_var_name": "day",
        "main_var_name": "avg_windspeed",
        "max_var_name": "max_windspeed",
        "min_var_name": "min_windspeed",
        "main_var_label": "Average Wind Speed",
        "min_var_label": "Min Wind Speed",
        "max_var_label": "Max Wind Speed",
        "main_var_color": "green",
        "min_var_color": "red",
        "max_var_color": "blue",
        "title": "Wind Speed Over Time",
        "xlabel": "Date",
        "ylabel": "Wind Speed (km/h)",
    }
    plot_variable(ax2, df, plot_config)

    # Plot 3: Relative Humidity
    ax3 = axes[2]
    plot_config = {
        "time_var_name": "day",
        "main_var_name": "avg_relative_humidity",
        "max_var_name": "max_relative_humidity",
        "min_var_name": "min_relative_humidity",
        "main_var_label": "Average Relative Humidity",
        "min_var_label": "Min Relative Humidity",
        "max_var_label": "Max Relative Humidity",
        "main_var_color": "purple",
        "min_var_color": "red",
        "max_var_color": "blue",
        "title": "Relative Humidity Over Time",
        "xlabel": "Date",
        "ylabel": "Relative Humidity (%)",
    }
    plot_variable(ax3, df, plot_config)

    # Adjust layout to prevent overlap
    plt.tight_layout()

    # Save the plot
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
