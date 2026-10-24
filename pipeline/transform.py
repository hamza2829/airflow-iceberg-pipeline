"""Turn the raw API/sample frame into the typed rows stored in Iceberg."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa

from pipeline import config

REQUIRED_RAW_COLUMNS = ("unix_seconds", "price")

# Arrow schema of the rows written to Iceberg. Must match pipeline.load.ICEBERG_SCHEMA.
ARROW_SCHEMA = pa.schema(
    [
        pa.field("timestamp_utc", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("delivery_day", pa.date32(), nullable=False),
        pa.field("price_eur_mwh", pa.float64(), nullable=True),
        pa.field("bidding_zone", pa.string(), nullable=True),
        pa.field("unit", pa.string(), nullable=True),
        pa.field("source", pa.string(), nullable=True),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=True),
    ]
)


def _to_local(unix_seconds: pd.Series) -> pd.Series:
    return pd.to_datetime(unix_seconds, unit="s", utc=True).dt.tz_convert(config.LOCAL_TZ)


def local_days(unix_seconds: pd.Series) -> list[date]:
    """Sorted distinct local (Europe/Berlin) calendar days in a unix-seconds column."""
    return sorted(set(_to_local(unix_seconds).dt.date))


def transform(raw: pd.DataFrame, day: date, ingested_at: datetime | None = None) -> pa.Table:
    """Keep the rows of local delivery `day` and cast them to the table schema.

    Duplicates and nulls are deliberately NOT dropped here: hiding them would also
    hide upstream bugs. The validate step is responsible for catching them.
    """
    missing = [c for c in REQUIRED_RAW_COLUMNS if c not in raw.columns]
    if missing:
        raise ValueError(f"Raw data is missing required columns: {missing}")

    ts_local = _to_local(raw["unix_seconds"].astype("int64"))
    mask = (ts_local.dt.date == day).to_numpy()
    rows = raw.loc[mask]
    ts_utc = ts_local[mask].dt.tz_convert("UTC")

    out = pd.DataFrame(
        {
            "timestamp_utc": ts_utc.to_numpy(),
            "delivery_day": pd.Series([day] * len(rows), dtype="object"),
            "price_eur_mwh": pd.to_numeric(rows["price"], errors="raise").astype("float64").to_numpy(),
            "bidding_zone": config.BIDDING_ZONE,
            "unit": rows["unit"].astype("string").to_numpy() if "unit" in rows else None,
            "source": rows["source"].astype("string").to_numpy() if "source" in rows else None,
            "ingested_at": ingested_at or datetime.now(timezone.utc),
        }
    )
    # Explicit UTC dtype so empty frames also get the right type.
    out["timestamp_utc"] = pd.to_datetime(out["timestamp_utc"], utc=True)
    out = out.sort_values("timestamp_utc", kind="stable").reset_index(drop=True)
    return pa.Table.from_pandas(out, schema=ARROW_SCHEMA, preserve_index=False)


def expected_rows(day: date, interval_minutes: int = config.INTERVAL_MINUTES) -> int:
    """Number of market intervals in a local day; DST days have 23 or 25 hours."""
    start_local = pd.Timestamp(day).tz_localize(config.LOCAL_TZ)
    end_local = pd.Timestamp(day + timedelta(days=1)).tz_localize(config.LOCAL_TZ)
    minutes = (end_local - start_local).total_seconds() / 60
    return int(minutes // interval_minutes)
