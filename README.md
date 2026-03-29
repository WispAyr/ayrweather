# AyrWeather 🌤️

Detailed weather app for Ayrshire, Scotland. Built with FastAPI + vanilla JS.

## Features

- **Current conditions** hero card — temp, wind, humidity, pressure, visibility
- **48-hour hourly** scroll
- **7-day forecast** cards
- **Firth of Clyde sea conditions** — wave height, wave period, sea temperature
- **Air quality gauge** — PM2.5, PM10, ozone, European AQI
- **Rainfall radar** map (RainViewer + Leaflet, animated)
- **9 Ayrshire locations** — Ayr, Troon, Prestwick, Kilmarnock, Irvine, Largs, Maybole, Girvan, Cumnock
- **Auto-refresh** every 5 minutes
- **nuro integration** at `/api/nuro`

## Data Sources

All free, no API keys required:
- [Open-Meteo](https://open-meteo.com/) — weather forecast
- [Open-Meteo Marine](https://marine-api.open-meteo.com/) — sea conditions
- [Open-Meteo Air Quality](https://air-quality-api.open-meteo.com/) — AQI
- [RainViewer](https://www.rainviewer.com/api.html) — radar tiles

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python server.py
```

Open http://localhost:3875

## PM2 Deploy

```bash
pm2 start ecosystem.config.cjs
pm2 save
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/locations` | List all available locations |
| `GET /api/weather/{location}` | Full weather data for location |
| `GET /api/nuro` | nuro v2.0 integration streams |

## Port

**3875**
