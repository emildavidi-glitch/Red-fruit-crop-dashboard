# =============================================================
# weather.py — Fetch live weather data from Open-Meteo API
# Free API, no key required
# =============================================================

import requests
from datetime import datetime

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_weather(region: dict) -> dict | None:
    """
    Fetch current conditions + 7-day forecast for a region.
    Returns parsed dict or None on failure.
    """
    params = {
        "latitude":  region["lat"],
        "longitude": region["lon"],
        "current":   "temperature_2m,weathercode,windspeed_10m,precipitation",
        "daily":     "temperature_2m_max,temperature_2m_min,weathercode,precipitation_sum",
        "timezone":  "auto",
        "forecast_days": 7,
    }

    try:
        resp = requests.get(OPEN_METEO_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        current = data["current"]
        daily   = data["daily"]

        return {
            "current_temp":  round(current["temperature_2m"], 1),
            "current_code":  current["weathercode"],
            "wind_kmh":      round(current["windspeed_10m"], 1),
            "precip_mm":     round(current["precipitation"], 1),
            "daily_dates":   daily["time"],
            "daily_max":     [round(t, 1) for t in daily["temperature_2m_max"]],
            "daily_min":     [round(t, 1) for t in daily["temperature_2m_min"]],
            "daily_codes":   daily["weathercode"],
            "min_7d":        round(min(daily["temperature_2m_min"]), 1),
            "fetched_at":    datetime.now().strftime("%Y-%m-%d %H:%M"),
        }

    except Exception as e:
        print(f"  [WARNING] Could not fetch weather for {region['name']}: {e}")
        return None
