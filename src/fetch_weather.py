import requests


API_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_weather(latitude, longitude, timezone: str = "America/Sao_Paulo"):
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ["temperature_2m", "windspeed_10m", "relative_humidity_2m"],
        "timezone": timezone,
    }
    res = requests.get(API_URL, params=params)
    res.raise_for_status()
    data = res.json()

    return data
