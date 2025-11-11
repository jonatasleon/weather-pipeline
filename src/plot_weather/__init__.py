from io import BytesIO

import matplotlib.pyplot as plt
import pandas as pd


def config_ax(ax: plt.Axes, info: dict[str, str]) -> plt.Axes:
    ax.set_title(info["title"])
    ax.set_xlabel(info["xlabel"])
    ax.set_ylabel(info["ylabel"])
    ax.legend(loc="best", prop={"size": 8})
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
    fig, axes = plt.subplots(3, 1, figsize=(9, 12), dpi=200)
    fig.suptitle(
        f"Weather Prediction for {plot_ctx['region']} on {plot_ctx['date']}",
        fontsize=16,
        fontweight="bold",
    )

    # Plot 1: Temperature (avg, min, max)
    ax1 = axes[0]
    ax1.plot(
        df["day"], df["avg_temp"], label="Average Temperature", marker="o", linewidth=2
    )
    ax1.plot(
        df["day"],
        df["min_temp"],
        label="Min Temperature",
        marker="v",
        linestyle="--",
        alpha=0.7,
    )
    ax1.plot(
        df["day"],
        df["max_temp"],
        label="Max Temperature",
        marker="^",
        linestyle="--",
        alpha=0.7,
    )
    ax1.fill_between(df["day"], df["min_temp"], df["max_temp"], alpha=0.2, color="blue")
    config_ax(
        ax1,
        {
            "title": "Temperature Over Time",
            "xlabel": "Date",
            "ylabel": "Temperature (°C)",
        },
    )

    # Plot 2: Wind Speed
    ax2 = axes[1]
    ax2.plot(
        df["day"],
        df["avg_windspeed"],
        label="Average Wind Speed",
        marker="o",
        color="green",
        linewidth=2,
    )
    ax2.plot(
        df["day"],
        df["min_windspeed"],
        label="Min Wind Speed",
        marker="v",
        linestyle="--",
        alpha=0.7,
    )
    ax2.plot(
        df["day"],
        df["max_windspeed"],
        label="Max Wind Speed",
        marker="^",
        linestyle="--",
        alpha=0.7,
    )
    ax2.fill_between(
        df["day"], df["min_windspeed"], df["max_windspeed"], alpha=0.2, color="green"
    )
    config_ax(
        ax2,
        {
            "title": "Wind Speed Over Time",
            "xlabel": "Date",
            "ylabel": "Wind Speed (km/h)",
        },
    )

    # Plot 3: Relative Humidity
    ax3 = axes[2]
    ax3.plot(
        df["day"],
        df["avg_relative_humidity"],
        label="Average Relative Humidity",
        marker="o",
        color="purple",
        linewidth=2,
    )
    ax3.plot(
        df["day"],
        df["min_relative_humidity"],
        label="Min Relative Humidity",
        marker="v",
        linestyle="--",
        alpha=0.7,
    )
    ax3.plot(
        df["day"],
        df["max_relative_humidity"],
        label="Max Relative Humidity",
        marker="^",
        linestyle="--",
        alpha=0.7,
    )
    ax3.fill_between(
        df["day"],
        df["min_relative_humidity"],
        df["max_relative_humidity"],
        alpha=0.2,
        color="purple",
    )
    config_ax(
        ax3,
        {
            "title": "Relative Humidity Over Time",
            "xlabel": "Date",
            "ylabel": "Relative Humidity (%)",
        },
    )

    # Adjust layout to prevent overlap
    plt.tight_layout()

    # Save the plot
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
