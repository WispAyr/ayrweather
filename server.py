"""
AyrWeather - Detailed weather app for Ayrshire, Scotland
FastAPI backend — thin proxy to Siphon data ingestion service
"""

import asyncio
import httpx
import logging
from datetime import datetime, timezone
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="AyrWeather", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])

# Siphon data ingestion service (primary on big-server, fallback on pu2)
SIPHON_URL = "http://142.202.191.208:3882"
NURO_URL = "http://192.168.195.33:3960"
PRISM_URL = "http://127.0.0.1:3887"

import time

# ── In-memory TTL cache (serves stale data instantly, refreshes in background) ──
_cache = {}  # {key: (data, expires_at)}

def cache_get(key: str):
    entry = _cache.get(key)
    if entry and entry[1] > time.monotonic():
        return entry[0]
    return None

def cache_set(key: str, data, ttl: int = 30):
    _cache[key] = (data, time.monotonic() + ttl)



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


# ── Siphon proxy helper ─────────────────────────────────────────────────

async def siphon_get(path: str, ttl: int = 30) -> dict | None:
    """Fetch from Siphon with TTL cache. Returns cached data on failure."""
    cached = cache_get(f"siphon:{path}")
    if cached is not None:
        return cached
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            r = await client.get(f"{SIPHON_URL}{path}")
            if r.status_code == 200:
                data = r.json()
                cache_set(f"siphon:{path}", data, ttl)
                return data
            logger.warning(f"Siphon {path} returned {r.status_code}")
    except Exception as e:
        logger.warning(f"Siphon {path} error: {e}")
    return None


async def prism_get(path: str, ttl: int = 15) -> dict | None:
    """Fetch from Prism (local) with TTL cache."""
    cached = cache_get(f"prism:{path}")
    if cached is not None:
        return cached
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            r = await client.get(f"{PRISM_URL}{path}")
            if r.status_code == 200:
                data = r.json()
                cache_set(f"prism:{path}", data, ttl)
                return data
    except Exception as e:
        logger.warning(f"Prism {path} error: {e}")
    return None


@app.get("/api/local-sensors/{region}")
async def local_sensors(region: str):
    """Live regional sensor signals from Prism (METAR bbox + SEPA rainfall + cross-verified + flood intelligence)."""
    import asyncio
    mw, pg, rg, wc, wa, sw, ru, sd, vc, sq, fb, av, rn, fc, fx, ts, cf = await asyncio.gather(
        prism_get(f"/api/micro-wind/{region}"),
        prism_get(f"/api/pressure-gradient/{region}"),
        prism_get(f"/api/rain-gradient/{region}"),
        prism_get(f"/api/wind-consensus/{region}"),
        prism_get(f"/api/wind-aloft/{region}"),
        prism_get(f"/api/storm-watch/{region}"),
        prism_get(f"/api/runways-in-use/{region}"),
        prism_get(f"/api/sar-drift/{region}"),
        prism_get(f"/api/vis-corroboration/{region}"),
        prism_get(f"/api/source-quality/{region}"),
        prism_get(f"/api/frontal-boundary"),
        siphon_get(f"/api/stations/aviation/{region}"),
        siphon_get(f"/api/stations/rainfall/{region}"),
        prism_get("/api/flood-alert-crossref"),
        siphon_get("/api/flood/warnings/scotland"),
        prism_get("/api/tidal-state"),
        prism_get("/api/coastal-flood-risk"),
    )
    av_stations = (av or {}).get("data", {}).get("stations", []) if av else []
    rn_stations = (rn or {}).get("data", {}).get("stations", []) if rn else []
    return JSONResponse({
        "region": region,
        "signals": {
            "micro_wind": (mw or {}).get("data"),
            "pressure_gradient": (pg or {}).get("data"),
            "rain_gradient": (rg or {}).get("data"),
            "wind_consensus": (wc or {}).get("data"),
            "wind_aloft": (wa or {}).get("data"),
            "storm_watch": (sw or {}).get("data"),
            "runways_in_use": (ru or {}).get("data"),
            "sar_drift": (sd or {}).get("data"),
            "vis_corroboration": (vc or {}).get("data"),
            "source_quality": (sq or {}).get("data"),
            "frontal_boundary": (fb or {}).get("data"),
            "flood_alert_crossref": (fc or {}).get("data"),
            "flood_warnings": (fx or {}).get("data"),
            "tidal_state": (ts or {}).get("data"),
            "coastal_flood_risk": (cf or {}).get("data"),
        },
        "stations": {
            "aviation": av_stations,
            "aviation_count": len(av_stations),
            "rainfall_count": len(rn_stations),
        },
        "computed_at": {
            "micro_wind": (mw or {}).get("computed_at"),
            "pressure_gradient": (pg or {}).get("computed_at"),
            "rain_gradient": (rg or {}).get("computed_at"),
            "wind_consensus": (wc or {}).get("computed_at"),
            "wind_aloft": (wa or {}).get("computed_at"),
            "storm_watch": (sw or {}).get("computed_at"),
            "runways_in_use": (ru or {}).get("computed_at"),
            "flood_alert_crossref": (fc or {}).get("computed_at"),
            "tidal_state": (ts or {}).get("computed_at"),
            "coastal_flood_risk": (cf or {}).get("computed_at"),
        },
    })


# ── Weather API (proxied from Siphon) ────────────────────────────────────

@app.get("/api/weather/{location_key}")
async def get_weather(location_key: str):
    if location_key not in LOCATIONS:
        raise HTTPException(status_code=404, detail=f"Location '{location_key}' not found")

    data = await siphon_get(f"/api/weather/{location_key}")
    if data:
        return JSONResponse(data)

    # Siphon unreachable — return empty shell so frontend doesn't break
    return JSONResponse({
        "location": LOCATIONS[location_key],
        "forecast": None,
        "marine": None,
        "air_quality": None,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "_siphon_error": True,
    })


@app.get("/api/warnings")
async def get_warnings():
    data = await siphon_get("/api/warnings")
    if data:
        return JSONResponse(data)
    return JSONResponse({
        "warnings": [],
        "region": "Strathclyde",
        "source": "Met Office",
        "fetched_at": None,
    })


@app.get("/api/grid/outages/scotland")
async def get_grid_outages_scotland():
    data = await siphon_get("/api/grid/outages/scotland")
    if data:
        return JSONResponse(data)
    return JSONResponse({"total_active": 0, "ssen": {"active": 0}, "spen": {"available": False}})


@app.get("/api/locations")
async def get_locations():
    return JSONResponse(LOCATIONS)


# ── Nuro integration (proxied from Siphon) ───────────────────────────────

@app.get("/api/nuro")
async def nuro():
    """Proxy to Siphon's nuro endpoint."""
    data = await siphon_get("/api/nuro")
    if data:
        # Override label to still appear as AyrWeather in Nuro
        data["label"] = "AyrWeather"
        data["icon"] = "\U0001f324\ufe0f"
        return JSONResponse(data)
    return JSONResponse({
        "version": "2.0",
        "label": "AyrWeather",
        "icon": "\U0001f324\ufe0f",
        "streams": [],
        "alerts": [],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })



# ── Prism intelligence lenses (proxied) ──────────────────────────────────

@app.get("/api/prism/intelligence/{location}")
async def prism_intelligence(location: str):
    data = await prism_get(f"/api/intelligence/{location}")
    return JSONResponse(data or {})

@app.get("/api/prism/catchment")
async def prism_catchment():
    data = await prism_get("/api/catchment-response")
    return JSONResponse(data or {})

@app.get("/api/prism/storm-river")
async def prism_storm_river():
    data = await prism_get("/api/storm-river-forecast")
    return JSONResponse(data or {})

@app.get("/api/prism/storm-tracker")
async def prism_storm_tracker():
    data = await prism_get("/api/storm-tracker")
    return JSONResponse(data or {})

@app.get("/api/prism/ayrshire-situation")
async def prism_ayrshire_situation():
    data = await prism_get("/api/ayrshire-situation")
    return JSONResponse(data or {})

@app.get("/api/prism/grid-pulse")
async def prism_grid_pulse():
    data = await prism_get("/api/grid-pulse")
    return JSONResponse(data or {})

@app.get("/api/prism/outdoor-score/{location}")
async def prism_outdoor_score(location: str):
    data = await prism_get(f"/api/outdoor-score/{location}")
    return JSONResponse(data or {})

@app.get("/api/prism/gnss-quality")
async def prism_gnss_quality():
    data = await prism_get("/api/gnss-quality")
    return JSONResponse(data or {})

@app.get("/api/prism/wave-analysis")
async def prism_wave_analysis():
    data = await prism_get("/api/wave-analysis")
    return JSONResponse(data or {})

@app.get("/api/prism/flood-risk")
async def prism_flood_risk():
    data = await prism_get("/api/flood-risk")
    return JSONResponse(data or {})

@app.get("/api/prism/storm-forecast/{location}")
async def prism_storm_forecast(location: str):
    data = await prism_get(f"/api/storm-forecast/{location}")
    return JSONResponse(data or {})


# ── New cross-verification lenses (proxied from Prism) ───────────────────

@app.get("/api/prism/wind-consensus/{region}")
async def prism_wind_consensus(region: str):
    data = await prism_get(f"/api/wind-consensus/{region}")
    return JSONResponse(data or {})

@app.get("/api/prism/wind-aloft/{region}")
async def prism_wind_aloft(region: str):
    data = await prism_get(f"/api/wind-aloft/{region}")
    return JSONResponse(data or {})

@app.get("/api/prism/storm-watch/{region}")
async def prism_storm_watch(region: str):
    data = await prism_get(f"/api/storm-watch/{region}")
    return JSONResponse(data or {})

@app.get("/api/prism/source-quality/{region}")
async def prism_source_quality(region: str):
    data = await prism_get(f"/api/source-quality/{region}")
    return JSONResponse(data or {})

@app.get("/api/prism/frontal-boundary")
async def prism_frontal_boundary():
    data = await prism_get("/api/frontal-boundary")
    return JSONResponse(data or {})

@app.get("/api/prism/runways-in-use/{region}")
async def prism_runways_in_use(region: str):
    data = await prism_get(f"/api/runways-in-use/{region}")
    return JSONResponse(data or {})

@app.get("/api/prism/sar-drift/{region}")
async def prism_sar_drift(region: str, lat: float | None = None, lon: float | None = None, hours: float = 6, obj: str = "PIW"):
    qs = []
    if lat is not None: qs.append(f"lat={lat}")
    if lon is not None: qs.append(f"lon={lon}")
    qs.append(f"hours={hours}")
    qs.append(f"obj={obj}")
    data = await prism_get(f"/api/sar-drift/{region}?" + "&".join(qs))
    return JSONResponse(data or {})

@app.get("/api/prism/vis-corroboration/{region}")
async def prism_vis_corroboration(region: str):
    data = await prism_get(f"/api/vis-corroboration/{region}")
    return JSONResponse(data or {})


# ── New data sources (proxied from Siphon) ───────────────────────────────

@app.get("/api/rivers")
async def get_rivers():
    data = await siphon_get("/api/rivers")
    return JSONResponse(data or {"rivers": {}, "count": 0})


@app.get("/api/moon")
async def get_moon():
    data = await siphon_get("/api/moon")
    return JSONResponse(data or {"phase_name": "Unknown", "illumination": 0, "emoji": ""})


@app.get("/api/aurora")
async def get_aurora():
    data = await siphon_get("/api/aurora")
    return JSONResponse(data or {"status": "unknown", "description": "Unavailable"})


@app.get("/api/iss")
async def get_iss():
    data = await siphon_get("/api/iss")
    return JSONResponse(data or {"latitude": 0, "longitude": 0})


@app.get("/api/floods")
async def get_floods():
    data = await siphon_get("/api/floods")
    return JSONResponse(data or {"total_warnings": 0, "items": []})


@app.get("/api/uv")
async def get_uv():
    data = await siphon_get("/api/uv")
    return JSONResponse(data or {"current_uv": None, "max_today": None, "level": "Unknown"})


@app.get("/api/fuel")
async def get_fuel():
    data = await siphon_get("/api/fuel")
    return JSONResponse(data or {})


@app.get("/api/news")
async def get_news():
    data = await siphon_get("/api/news")
    return JSONResponse(data or {})


@app.get("/api/news/{region}")
async def get_news_region(region: str):
    data = await siphon_get(f"/api/news/{region}")
    return JSONResponse(data or {"articles": [], "count": 0})


@app.get("/api/aviation")
async def get_aviation():
    data = await siphon_get("/api/aviation")
    return JSONResponse(data or {"count": 0, "aircraft": []})


@app.get("/api/space")
async def get_space():
    data = await siphon_get("/api/space")
    return JSONResponse(data or {})


@app.get("/api/space/{group}")
async def get_space_group(group: str):
    data = await siphon_get(f"/api/space/{group}")
    return JSONResponse(data or {})


@app.get("/api/finance/fx")
async def get_fx():
    data = await siphon_get("/api/finance/fx")
    return JSONResponse(data or {})


@app.get("/api/transport/incidents")
async def get_transport_incidents():
    data = await siphon_get("/api/transport/incidents")
    return JSONResponse(data or {"count": 0, "incidents": []})


@app.get("/api/grid")
async def get_grid():
    data = await siphon_get("/api/grid")
    return JSONResponse(data or {})


@app.get("/api/grid/regional")
async def get_grid_regional():
    data = await siphon_get("/api/grid/regional")
    return JSONResponse(data or {"regions": [], "scotland": {}})


# ── Lightning (proxied from Nuro hub) ────────────────────────────────────

@app.get("/api/lightning")
async def api_lightning_proxy():
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{NURO_URL}/api/lightning", timeout=5.0)
            if r.status_code == 200:
                return r.json()
    except Exception as e:
        logger.debug(f"Lightning proxy error: {e}")
    return {"active": False, "strikes_last_hour": 0, "strikes_last_24h": 0}


@app.get("/api/lightning/strikes")
async def api_lightning_strikes_proxy(hours: int = 1):
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{NURO_URL}/api/lightning/strikes?hours={hours}", timeout=5.0)
            if r.status_code == 200:
                return r.json()
    except Exception as e:
        logger.debug(f"Lightning strikes proxy error: {e}")
    return []


# ── Siphon registry + Prism lenses (proxied for dashboards) ─────────────

@app.get("/api/siphon/sources")
async def get_siphon_sources():
    data = await siphon_get("/api/sources")
    return JSONResponse(data or [])


@app.get("/api/siphon/health")
async def get_siphon_health():
    data = await siphon_get("/api/health")
    return JSONResponse(data or {})


@app.get("/api/prism/lenses")
async def get_prism_lenses():
    data = await prism_get("/api/lenses")
    return JSONResponse(data or {})


@app.get("/api/prism/health")
async def get_prism_health():
    data = await prism_get("/api/health")
    return JSONResponse(data or {})


# ── Heartbeat ────────────────────────────────────────────────────────────


# ── Flood Intelligence (2026-04-09) ────────────────────────────────────

@app.get("/api/flood/warnings/scotland")
async def flood_warnings_scotland():
    data = await siphon_get("/api/flood/warnings/scotland")
    if data:
        return JSONResponse(data)
    return JSONResponse({"warnings": [], "total": 0, "active": 0})


@app.get("/api/flood/warnings/ayrshire")
async def flood_warnings_ayrshire():
    data = await siphon_get("/api/flood/warnings/ayrshire")
    if data:
        return JSONResponse(data)
    return JSONResponse({"warnings": [], "total": 0, "active": 0})


@app.get("/api/prism/flood-alert-crossref")
async def prism_flood_alert_crossref():
    data = await prism_get("/api/flood-alert-crossref")
    if data:
        return JSONResponse(data)
    return JSONResponse({"error": "not available"})


@app.get("/api/prism/flood-model-accuracy")
async def prism_flood_model_accuracy():
    data = await prism_get("/api/flood-model-accuracy")
    if data:
        return JSONResponse(data)
    return JSONResponse({"error": "not available"})


@app.get("/api/prism/coastal-flood-risk")
async def prism_coastal_flood_risk():
    data = await prism_get("/api/coastal-flood-risk")
    if data:
        return JSONResponse(data)
    return JSONResponse({"error": "not available"})


@app.get("/api/prism/tidal-state")
async def prism_tidal_state():
    data = await prism_get("/api/tidal-state")
    if data:
        return JSONResponse(data)
    return JSONResponse({"error": "not available"})


@app.get("/api/tides")
async def get_tides():
    data = await siphon_get("/api/tides")
    if data:
        return JSONResponse(data)
    return JSONResponse({"stations": []})

async def send_heartbeat():
    while True:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(
                    f"{NURO_URL}/api/heartbeat",
                    json={
                        "service": "AyrWeather",
                        "version": "2.0",
                        "icon": "\U0001f324\ufe0f",
                        "port": 3875,
                        "endpoint": "http://192.168.195.33:3875/api/nuro",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                )
        except Exception:
            pass
        await asyncio.sleep(30)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(send_heartbeat())
    logger.info("AyrWeather v2.0 started — data via Siphon")




@app.get("/api/prism/lightning-risk/{location}")
async def prism_lightning_risk(location: str):
    data = await prism_get(f"/api/lightning-risk/{location}")
    if data:
        return JSONResponse(data)
    return JSONResponse({"error": "not available"})


@app.get("/api/prism/dark-sky/{location}")
async def prism_dark_sky(location: str):
    data = await prism_get(f"/api/dark-sky/{location}")
    if data:
        return JSONResponse(data)
    return JSONResponse({"error": "not available"})


@app.get("/api/prism/beach-safety")
async def prism_beach_safety():
    data = await prism_get("/api/beach-safety")
    if data:
        return JSONResponse(data)
    return JSONResponse({"error": "not available"})


@app.get("/api/prism/forecast-consensus")
async def prism_forecast_consensus():
    data = await prism_get("/api/forecast-consensus")
    if data:
        return JSONResponse(data)
    return JSONResponse({"error": "not available"})


@app.get("/api/prism/water-quality")
async def prism_water_quality():
    data = await prism_get("/api/water-quality-cascade")
    if data:
        return JSONResponse(data)
    return JSONResponse({"error": "not available"})


@app.get("/api/prism/harbour-access")
async def prism_harbour_access():
    data = await prism_get("/api/harbour-access")
    if data:
        return JSONResponse(data)
    return JSONResponse({"error": "not available"})


@app.get("/api/prism/fishing/{location}")
async def prism_fishing(location: str):
    data = await prism_get(f"/api/fishing/{location}")
    if data:
        return JSONResponse(data)
    return JSONResponse({"error": "not available"})


@app.get("/api/prism/warning-predictor/{location}")
async def prism_warning_predictor(location: str):
    data = await prism_get(f"/api/warning-predictor/{location}")
    if data:
        return JSONResponse(data)
    return JSONResponse({"error": "not available"})
# ── Static files ─────────────────────────────────────────────────────────

import sqlite3
import json as _json

PRISM_DB = "/opt/prism/data/prism.db"

@app.get("/api/storm-history")
async def storm_history(days: int = 14):
    """Lightweight storm tracker snapshots for timeline replay."""
    try:
        conn = sqlite3.connect(PRISM_DB)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            "SELECT id, computed_at, data FROM lens_history "
            "WHERE lens_id = 'storm_tracker' "
            "AND computed_at >= datetime('now', '-"  + str(days) + " days') "
            "ORDER BY computed_at ASC"
        )
        rows = cur.fetchall()
        snapshots = []
        episodes_seen = set()
        all_episodes = []
        for row in rows:
            blob = _json.loads(row["data"])
            snap = blob.get("_snapshot", {})
            snapshots.append({
                "id": row["id"],
                "ts": snap.get("ts", row["computed_at"]),
                "stormy": snap.get("stormy", False),
                "pressure_hpa": snap.get("pressure_hpa"),
                "wind_kph": snap.get("wind_kph"),
                "gust_kph": snap.get("gust_kph"),
                "precip_mm_hr": snap.get("precip_mm_hr"),
                "rivers": snap.get("rivers", {}),
                "outages_scotland": snap.get("outages_scotland", 0),
                "warnings": len(snap.get("warnings", [])),
            })
            for ep in blob.get("recent_episodes", []):
                key = ep.get("start_at", "")
                if key and key not in episodes_seen:
                    episodes_seen.add(key)
                    all_episodes.append(ep)
        conn.close()
        all_episodes.sort(key=lambda e: e.get("start_at", ""))
        return JSONResponse({
            "snapshots": snapshots,
            "episodes": all_episodes,
            "total_snapshots": len(snapshots),
            "range": {
                "from": snapshots[0]["ts"] if snapshots else None,
                "to": snapshots[-1]["ts"] if snapshots else None,
            }
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/storm-history/{snapshot_id}")
async def storm_history_detail(snapshot_id: int):
    """Full data blob for a single storm tracker snapshot."""
    try:
        conn = sqlite3.connect(PRISM_DB)
        cur = conn.cursor()
        cur.execute("SELECT data FROM lens_history WHERE id = ?", (snapshot_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            return JSONResponse({"error": "not found"}, status_code=404)
        return JSONResponse(_json.loads(row[0]))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

app.mount("/", StaticFiles(directory="static", html=True), name="static")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=3875)

