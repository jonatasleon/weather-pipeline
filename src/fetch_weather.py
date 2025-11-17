import requests


FORECAST_API_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_API_URL = "https://archive-api.open-meteo.com/v1/era5"


def fetch_weather_forecast(latitude, longitude, timezone: str = "America/Sao_Paulo"):
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ["temperature_2m", "windspeed_10m", "relative_humidity_2m"],
        "timezone": timezone,
    }
    res = requests.get(FORECAST_API_URL, params=params)
    res.raise_for_status()
    data = res.json()

    return data


def fetch_weather_history(
    latitude,
    longitude,
    start_date,
    end_date,
    timezone: str = "America/Sao_Paulo",
):
    params = {
        "hourly": ["temperature_2m", "windspeed_10m", "relative_humidity_2m"],
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "timezone": timezone,
    }
    res = requests.get(ARCHIVE_API_URL, params=params)
    res.raise_for_status()
    data = res.json()
    return data
