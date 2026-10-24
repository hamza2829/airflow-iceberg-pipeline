"""Write transformed rows into a local Apache Iceberg table (SQLite catalog)."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DateType, DoubleType, NestedField, StringType, TimestamptzType

from pipeline import config
from pipeline.transform import transform

log = logging.getLogger(__name__)

ICEBERG_SCHEMA = Schema(
    NestedField(1, "timestamp_utc", TimestamptzType(), required=True),
    NestedField(2, "delivery_day", DateType(), required=True),
    NestedField(3, "price_eur_mwh", DoubleType(), required=False),
    NestedField(4, "bidding_zone", StringType(), required=False),
    NestedField(5, "unit", StringType(), required=False),
    NestedField(6, "source", StringType(), required=False),
    NestedField(7, "ingested_at", TimestamptzType(), required=False),
)

# One partition per local delivery day.
PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="delivery_day")
)

TABLE_ID = f"{config.NAMESPACE}.{config.TABLE_NAME}"


def get_catalog(warehouse_dir: Path | None = None) -> SqlCatalog:
    warehouse_dir = warehouse_dir or config.get_paths().warehouse_dir
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        "local",
        uri=f"sqlite:///{(warehouse_dir / 'catalog.db').as_posix()}",
        warehouse=warehouse_dir.as_uri(),
    )


def get_or_create_table(catalog: SqlCatalog) -> Table:
    catalog.create_namespace_if_not_exists(config.NAMESPACE)
    try:
        return catalog.load_table(TABLE_ID)
    except NoSuchTableError:
        log.info("Creating Iceberg table %s partitioned by delivery_day", TABLE_ID)
        return catalog.create_table(TABLE_ID, schema=ICEBERG_SCHEMA, partition_spec=PARTITION_SPEC)


def load_day(rows: pa.Table, day: date, catalog: SqlCatalog | None = None) -> int:
    """Replace the partition of `day` with `rows`. Running it twice gives the same table.

    `overwrite` with a filter on the partition column deletes the old rows of that
    day and appends the new ones in a single Iceberg snapshot.
    """
    other_days = pc.unique(pc.filter(rows["delivery_day"], pc.not_equal(rows["delivery_day"], day)))
    if len(other_days):
        raise ValueError(f"Rows for {day} contain other delivery days: {other_days.to_pylist()}")

    table = get_or_create_table(catalog or get_catalog())
    table.overwrite(rows, overwrite_filter=EqualTo("delivery_day", day.isoformat()))
    snapshot = table.current_snapshot()
    log.info(
        "Overwrote partition delivery_day=%s with %d rows (snapshot %s)",
        day,
        rows.num_rows,
        snapshot.snapshot_id if snapshot else None,
    )
    return rows.num_rows


def load_raw_file(raw_path: str | Path, day: date, catalog: SqlCatalog | None = None) -> int:
    raw = pd.read_parquet(raw_path)
    rows = transform(raw, day)
    log.info("Transformed %d raw rows into %d rows for %s", len(raw), rows.num_rows, day)
    return load_day(rows, day, catalog)
