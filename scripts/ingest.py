"""
Fetch current weather + N-hour hourly forecast from Open-Meteo,
write to Neon Postgres.

Run every 15 minutes via GitHub Actions.

Requires:
  - psycopg[binary] >= 3.1
  - requests >= 2.31

Expected schema (must exist before running):
  - observations: UNIQUE (observed_at, location)
  - forecasts:    UNIQUE (fetched_at, valid_at, location)
"""
import logging
import os
import sys
import time
from datetime import datetime, timezone

import psycopg
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---- config from env ----
LAT = float(os.environ.get("WEATHER_LAT", "42.9634"))
LON = float(os.environ.get("WEATHER_LON", "-85.6681"))
LOCATION = os.environ.get("WEATHER_LOCATION", "grand_rapids")
TIMEZONE = os.environ.get("WEATHER_TZ", "UTC")
FORECAST_HOURS = int(os.environ.get("FORECAST_HOURS", "48"))

VARS = [
    "temperature_2m", "apparent_temperature", "dew_point_2m",
    "relative_humidity_2m", "precipitation", "surface_pressure",
    "wind_speed_10m", "wind_gusts_10m", "uv_index", "is_day",
]
FORECAST_EXTRA = ["precipitation_probability"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingest")


def _maybe_bool(v):
    """Preserve NULL semantics; only coerce real 0/1 to bool."""
    return None if v is None else bool(v)


def _get(hourly: dict, key: str, i: int):
    arr = hourly.get(key) or []
    if i >= len(arr):
        return None
    return arr[i]


def _make_session() -> requests.Session:
    """Session with retry on transient 5xx and connection errors."""
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.5,  # 0s, 1.5s, 3s
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def fetch_weather(session: requests.Session) -> dict:
    """Single API call: current observations + N-hour forecast."""
    params = {
        "latitude": LAT,
        "longitude": LON,
        "current": ",".join(VARS),
        "hourly": ",".join(VARS + FORECAST_EXTRA),
        "forecast_hours": FORECAST_HOURS,
        "timezone": TIMEZONE,
        "timeformat": "unixtime",
    }
    r = session.get(
        "https://api.open-meteo.com/v1/forecast",
        params=params,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def insert_observation(cur, data: dict) -> bool:
    """Insert the 'current' block as one observations row. Returns True if written."""
    current = data.get("current") or {}
    if not current or current.get("time") is None:
        log.warning("No 'current' block in response, skipping observation")
        return False

    cur.execute(
        """
        INSERT INTO observations (
            observed_at, location,
            temperature, feels_like, dew_point, humidity,
            precipitation, pressure, wind_speed, wind_gusts,
            uv_index, is_day
        )
        VALUES (
            to_timestamp(%s), %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s
        )
        ON CONFLICT (observed_at, location) DO UPDATE SET
            temperature   = EXCLUDED.temperature,
            feels_like    = EXCLUDED.feels_like,
            dew_point     = EXCLUDED.dew_point,
            humidity      = EXCLUDED.humidity,
            precipitation = EXCLUDED.precipitation,
            pressure      = EXCLUDED.pressure,
            wind_speed    = EXCLUDED.wind_speed,
            wind_gusts    = EXCLUDED.wind_gusts,
            uv_index      = EXCLUDED.uv_index,
            is_day        = EXCLUDED.is_day
        """,
        (
            current["time"], LOCATION,
            current.get("temperature_2m"),
            current.get("apparent_temperature"),
            current.get("dew_point_2m"),
            current.get("relative_humidity_2m"),
            current.get("precipitation"),
            current.get("surface_pressure"),
            current.get("wind_speed_10m"),
            current.get("wind_gusts_10m"),
            current.get("uv_index"),
            _maybe_bool(current.get("is_day")),
        ),
    )
    return True


def insert_forecasts(cur, data: dict) -> int:
    """Insert hourly forecast rows. Returns count attempted."""
    hourly = data.get("hourly") or {}
    times = hourly.get("time", [])
    if not times:
        log.warning("No 'hourly' block in response, skipping forecast")
        return 0

    fetched_at = datetime.now(timezone.utc)

    rows = []
    for i, valid_ts in enumerate(times):
        rows.append((
            fetched_at, valid_ts, LOCATION,
            _get(hourly, "temperature_2m", i),
            _get(hourly, "apparent_temperature", i),
            _get(hourly, "dew_point_2m", i),
            _get(hourly, "relative_humidity_2m", i),
            _get(hourly, "precipitation", i),
            _get(hourly, "precipitation_probability", i),
            _get(hourly, "surface_pressure", i),
            _get(hourly, "wind_speed_10m", i),
            _get(hourly, "wind_gusts_10m", i),
            _get(hourly, "uv_index", i),
            _maybe_bool(_get(hourly, "is_day", i)),
        ))

    cur.executemany(
        """
        INSERT INTO forecasts (
            fetched_at, valid_at, location,
            temperature, feels_like, dew_point, humidity,
            precipitation, precip_prob, pressure,
            wind_speed, wind_gusts, uv_index, is_day
        )
        VALUES (
            %s, to_timestamp(%s), %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (fetched_at, valid_at, location) DO NOTHING
        """,
        rows,
    )
    return len(rows)


def main() -> int:
    # Validate required env up front, with a clear message
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        log.error("DATABASE_URL is not set")
        return 2

    started = time.monotonic()
    try:
        log.info("Fetching weather for %s (%s, %s)", LOCATION, LAT, LON)
        session = _make_session()
        data = fetch_weather(session)

        with psycopg.connect(database_url, connect_timeout=15) as conn:
            with conn.cursor() as cur:
                wrote_obs = insert_observation(cur, data)
                n = insert_forecasts(cur, data)
            conn.commit()

        log.info(
            "Wrote %d observation + %d forecast rows in %.2fs",
            int(wrote_obs), n, time.monotonic() - started,
        )
        return 0
    except requests.HTTPError as e:
        body = e.response.text[:500] if e.response is not None else "<no response>"
        log.error("Open-Meteo HTTP error: %s — body: %s", e, body)
        return 1
    except requests.RequestException as e:
        # Timeouts, connection errors, DNS failures, retry exhaustion
        log.error("Open-Meteo network error: %s", e)
        return 1
    except psycopg.Error as e:
        log.error("Postgres error: %s", e)
        return 1
    except Exception:
        log.exception("Unexpected failure")
        return 1


if __name__ == "__main__":
    sys.exit(main())
