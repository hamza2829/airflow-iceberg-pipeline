"""Print what is in the Iceberg table: snapshots, rows per day and price stats (via DuckDB)."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.load import TABLE_ID, get_catalog  # noqa: E402

table = get_catalog().load_table(TABLE_ID)
print(f"table: {TABLE_ID}")
print(f"snapshots: {len(table.metadata.snapshots)}")
print(f"partition spec: {table.spec()}")

con = duckdb.connect()
con.register("prices", table.scan().to_arrow())
print(
    con.sql(
        """
        SELECT delivery_day, source, count(*) AS rows,
               count(DISTINCT timestamp_utc) AS distinct_ts,
               round(min(price_eur_mwh), 2) AS min_price,
               round(avg(price_eur_mwh), 2) AS avg_price,
               round(max(price_eur_mwh), 2) AS max_price
        FROM prices GROUP BY ALL ORDER BY delivery_day
        """
    )
)
