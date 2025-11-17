import os
from textwrap import dedent

import duckdb


def analysis_weather(s3_path: str):
    query = dedent(
        f"""
        SELECT date_trunc('day', time) AS day,
            AVG(temperature_2m) AS avg_temp,
            MIN(temperature_2m) AS min_temp,
            MAX(temperature_2m) AS max_temp,
            MEDIAN(temperature_2m) AS median_temp,
            MODE(temperature_2m) AS mode_temp,
            STDDEV(temperature_2m) AS stddev_temp,
            VARIANCE(temperature_2m) AS variance_temp,
            COUNT(temperature_2m) AS count_temp,
            SUM(temperature_2m) AS sum_temp,
            AVG(windspeed_10m) AS avg_windspeed,
            MIN(windspeed_10m) AS min_windspeed,
            MAX(windspeed_10m) AS max_windspeed,
            MEDIAN(windspeed_10m) AS median_windspeed,
            MODE(windspeed_10m) AS mode_windspeed,
            STDDEV(windspeed_10m) AS stddev_windspeed,
            VARIANCE(windspeed_10m) AS variance_windspeed,
            COUNT(windspeed_10m) AS count_windspeed,
            SUM(windspeed_10m) AS sum_windspeed,
            AVG(relative_humidity_2m) AS avg_relative_humidity,
            MIN(relative_humidity_2m) AS min_relative_humidity,
            MAX(relative_humidity_2m) AS max_relative_humidity,
            MEDIAN(relative_humidity_2m) AS median_relative_humidity,
            MODE(relative_humidity_2m) AS mode_relative_humidity,
            STDDEV(relative_humidity_2m) AS stddev_relative_humidity,
            VARIANCE(relative_humidity_2m) AS variance_relative_humidity,
            COUNT(relative_humidity_2m) AS count_relative_humidity,
            SUM(relative_humidity_2m) AS sum_relative_humidity,
        FROM '{s3_path}'
        GROUP BY date_trunc('day', time)
        ORDER BY day DESC
        """
    ).strip()
    with duckdb.connect(config={"s3_region": os.environ["REGION_NAME"]}) as conn:
        with conn.cursor() as cursor:
            result = cursor.execute(query)
            return result.df()
