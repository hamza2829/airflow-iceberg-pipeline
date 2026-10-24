"""Download one day of day-ahead prices and store the raw response as Parquet."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

from pipeline import config
from pipeline.transform import local_days

log = logging.getLogger(__name__)


def fetch_from_api(day: date, timeout: float = 30.0) -> pd.DataFrame:
    """Call the Energy-Charts /price endpoint. Raises on HTTP or payload errors."""
    # Ask for [day, day+1] and cut to the local day in the transform step, so the
    # result does not depend on how the API interprets the end date.
    params = {
        "bzn": config.BIDDING_ZONE,
        "start": day.isoformat(),
        "end": (day + timedelta(days=1)).isoformat(),
    }
    log.info("Requesting %s with params %s", config.API_URL, params)
    resp = requests.get(config.API_URL, params=params, timeout=timeout)
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("unix_seconds") or "price" not in payload:
        raise ValueError(f"Unexpected API payload, keys: {sorted(payload)}")
    if len(payload["unix_seconds"]) != len(payload["price"]):
        raise ValueError("API payload has unix_seconds and price of different length")
    return pd.DataFrame(
        {
            "unix_seconds": payload["unix_seconds"],
            "price": payload["price"],
            "unit": payload.get("unit"),
        }
    )


def load_sample(path: Path = config.SAMPLE_CSV) -> pd.DataFrame:
    return pd.read_csv(path, dtype={"unix_seconds": "int64", "price": "float64", "unit": "string"})


def ingest_day(day: date, raw_dir: Path | None = None) -> dict:
    """Fetch prices for `day` (API first, saved sample as fallback) and write raw Parquet.

    Returns metadata for the next task: raw file path, source and the delivery day
    that the raw file actually contains (the sample covers one fixed day).
    """
    raw_dir = raw_dir or config.get_paths().raw_dir
    raw_dir.mkdir(parents=True, exist_ok=True)

    try:
        raw = fetch_from_api(day)
        source = "api"
        delivery_day = day
        log.info("API returned %d rows for %s", len(raw), day)
    except (requests.RequestException, ValueError) as exc:
        log.warning(
            "FALLBACK USED: Energy-Charts API unreachable or invalid (%s: %s). "
            "Loading saved sample %s instead.",
            type(exc).__name__,
            exc,
            config.SAMPLE_CSV,
        )
        raw = load_sample()
        source = "sample"
        days = local_days(raw["unix_seconds"])
        if len(days) != 1:
            raise ValueError(f"Sample file must cover exactly one local day, found {days}")
        delivery_day = days[0]
        log.warning("Sample data is for delivery day %s, not the requested day %s", delivery_day, day)

    raw["source"] = source
    out = raw_dir / f"prices_{config.BIDDING_ZONE}_{delivery_day.isoformat()}_{source}.parquet"
    raw.to_parquet(out, index=False)
    log.info("Wrote %d raw rows to %s", len(raw), out)
    return {"raw_path": str(out), "source": source, "delivery_day": delivery_day.isoformat()}
