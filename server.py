"""
AyrWeather - Detailed weather app for Ayrshire, Scotland
FastAPI backend serving API + static frontend on port 3875
"""

import asyncio
import httpx
import logging
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="AyrWeather", version="1.0.0")

LOCATIONS = {
    "ayr":         {"name": "Ayr",         "lat": 55.4583, "lon": -4.6292},
    "troon":       {"name": "Troon",       "lat": 55.5369, "lon": -4.6561},
    "prestwick":   {"name": "Prestwick",   "lat": 55.4961, "lon": -4.6148},
    "kilmarnock":  {"name": "Kilmarnock",  "lat": 55.6111, "lon": -4.4961},
    "irvine":      {"name": "Irvine",      "lat": 55.6110, "lon": -4.6669},
    "largs":       {"name": "Largs",       "lat": 55.7948, "lon": -4.8695},
    "maybole":     {"name": "Maybole",     "lat": 55.3556, "lon": -4.6844},
    "girvan":      {"name": "Girvan",      "lat": 55.2347, "lon": -4.8592},
    "cumnock":     {"name": "Cumnock",     "lat": 55.4542, "lon": -4.2697},
}

# Marine location for Firth of Clyde (offshore point)
MARINE_LAT = 55.5
MARINE_LON = -4.9

NURO_URL = "http://192.168.195.33:3960"

async def fetch_weather(lat: float, lon: float) -> dict:
    """Fetch forecast, marine, and air quality data for a location."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        # Forecast API
        forecast_url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            f"&current=temperature_2m,relative_humidity_2m,apparent_temperature,"
            f"weather_code,wind_speed_10m,wind_direction_10m,wind_gusts_10m,"
            f"surface_pressure,precipitation,cloud_cover,visibility"
            f"&hourly=temperature_2m,weather_code,wind_speed_10m,wind_direction_10m,"
            f"precipitation_probability,precipitation,relative_humidity_2m"
            f"&daily=weather_code,temperature_2m_max,temperature_2m_min,"
            f"precipitation_sum,wind_speed_10m_max,sunrise,sunset,uv_index_max,"
            f"precipitation_probability_max"
            f"&wind_speed_unit=mph&temperature_unit=celsius&precipitation_unit=mm"
            f"&timezone=Europe%2FLondon&forecast_days=7&forecast_hours=48"
        )

        # Marine API
        marine_url = (
            f"https://marine-api.open-meteo.com/v1/marine"
            f"?latitude={MARINE_LAT}&longitude={MARINE_LON}"
            f"&current=wave_height,wave_period,sea_surface_temperature,wind_wave_height"
            f"&hourly=wave_height,wave_period,sea_surface_temperature"
            f"&timezone=Europe%2FLondon&forecast_days=3"
        )

        # Air quality API
        aq_url = (
            f"https://air-quality-api.open-meteo.com/v1/air-quality"
            f"?latitude={lat}&longitude={lon}"
            f"&current=pm2_5,pm10,ozone,nitrogen_dioxide,carbon_monoxide,european_aqi"
            f"&timezone=Europe%2FLondon"
        )

        forecast_task = client.get(forecast_url)
        marine_task = client.get(marine_url)
        aq_task = client.get(aq_url)

        forecast_resp, marine_resp, aq_resp = await asyncio.gather(
            forecast_task, marine_task, aq_task, return_exceptions=True
        )

        result = {}

        if isinstance(forecast_resp, Exception):
            logger.error(f"Forecast error: {forecast_resp}")
            result["forecast"] = None
        else:
            result["forecast"] = forecast_resp.json()

        if isinstance(marine_resp, Exception):
            logger.error(f"Marine error: {marine_resp}")
            result["marine"] = None
        else:
            result["marine"] = marine_resp.json()

        if isinstance(aq_resp, Exception):
            logger.error(f"Air quality error: {aq_resp}")
            result["air_quality"] = None
        else:
            result["air_quality"] = aq_resp.json()

        return result


@app.get("/api/weather/{location_key}")
async def get_weather(location_key: str):
    if location_key not in LOCATIONS:
        raise HTTPException(status_code=404, detail=f"Location '{location_key}' not found")
    loc = LOCATIONS[location_key]
    data = await fetch_weather(loc["lat"], loc["lon"])
    data["location"] = loc
    data["fetched_at"] = datetime.utcnow().isoformat() + "Z"
    return JSONResponse(data)


@app.get("/api/locations")
async def get_locations():
    return JSONResponse(LOCATIONS)


@app.get("/api/nuro")
async def nuro():
    """nuro integration endpoint v2.0"""
    # Try to get current data for Ayr
    try:
        data = await fetch_weather(55.4583, -4.6292)
        current = data.get("forecast", {}).get("current", {})
        marine = data.get("marine", {}).get("current", {})
        aq = data.get("air_quality", {}).get("current", {})

        temp = current.get("temperature_2m")
        wind = current.get("wind_speed_10m")
        wave_h = marine.get("wave_height")
        sea_temp = marine.get("sea_surface_temperature")
        aqi = aq.get("european_aqi")
    except Exception as e:
        logger.error(f"nuro fetch error: {e}")
        temp = wind = wave_h = sea_temp = aqi = None

    streams = [
        {
            "id": "temp",
            "label": "Temperature",
            "type": "gauge",
            "value": temp,
            "unit": "°C",
            "min": -10,
            "max": 35,
            "color": "#f59e0b",
        },
        {
            "id": "wind",
            "label": "Wind Speed",
            "type": "gauge",
            "value": wind,
            "unit": "mph",
            "min": 0,
            "max": 80,
            "color": "#3b82f6",
        },
        {
            "id": "wave_height",
            "label": "Wave Height",
            "type": "gauge",
            "value": wave_h,
            "unit": "m",
            "min": 0,
            "max": 5,
            "color": "#06b6d4",
        },
        {
            "id": "aqi",
            "label": "Air Quality Index",
            "type": "gauge",
            "value": aqi,
            "unit": "AQI",
            "min": 0,
            "max": 300,
            "color": "#10b981",
        },
        {
            "id": "sea_temp",
            "label": "Sea Temperature",
            "type": "stat",
            "value": sea_temp,
            "unit": "°C",
            "color": "#8b5cf6",
        },
    ]

    return JSONResponse({
        "version": "2.0",
        "label": "AyrWeather",
        "icon": "🌤️",
        "description": "Ayrshire weather - Firth of Clyde conditions",
        "streams": streams,
        "updated_at": datetime.utcnow().isoformat() + "Z",
    })


async def send_heartbeat():
    """Send periodic heartbeat to nuro hub."""
    while True:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(
                    f"{NURO_URL}/api/heartbeat",
                    json={
                        "service": "AyrWeather",
                        "version": "2.0",
                        "icon": "🌤️",
                        "port": 3875,
                        "endpoint": "http://192.168.195.33:3875/api/nuro",
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                    }
                )
                logger.debug("Heartbeat sent to nuro")
        except Exception as e:
            logger.debug(f"Heartbeat failed (nuro may be offline): {e}")
        await asyncio.sleep(30)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(send_heartbeat())
    logger.info("AyrWeather started on port 3875")


# Mount static files
app.mount("/", StaticFiles(directory="static", html=True), name="static")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=3875)
