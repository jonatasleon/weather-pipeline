from io import BytesIO

import matplotlib.pyplot as plt
import pandas as pd


def plot_weather(df: pd.DataFrame, output_file: BytesIO) -> None:
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
    fig, axes = plt.subplots(3, 1, figsize=(12, 10))
    fig.suptitle("Weather Analysis Over Time", fontsize=16, fontweight="bold")

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
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Temperature (°C)")
    ax1.set_title("Temperature Over Time")
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis="x", rotation=45)

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
    ax2.set_xlabel("Date")
    ax2.set_ylabel("Wind Speed (km/h)")
    ax2.set_title("Wind Speed Over Time")
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.tick_params(axis="x", rotation=45)

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
    ax3.set_xlabel("Date")
    ax3.set_ylabel("Relative Humidity (%)")
    ax3.set_title("Relative Humidity Over Time")
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    ax3.tick_params(axis="x", rotation=45)

    # Adjust layout to prevent overlap
    plt.tight_layout()

    # Save the plot
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
