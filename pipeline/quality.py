"""Data quality checks on the Iceberg table, executed as SQL in DuckDB."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import EqualTo

from pipeline import config
from pipeline.load import TABLE_ID, get_catalog
from pipeline.transform import expected_rows

log = logging.getLogger(__name__)

KEY_COLUMNS = ("timestamp_utc", "delivery_day", "price_eur_mwh")


class DataQualityError(Exception):
    """Raised when at least one data quality check fails."""


@dataclass
class CheckResult:
    name: str
    passed: bool
    details: str


def check_not_null(con: duckdb.DuckDBPyConnection, rel: str) -> CheckResult:
    exprs = ", ".join(f"count(*) FILTER (WHERE {c} IS NULL) AS {c}" for c in KEY_COLUMNS)
    counts = dict(zip(KEY_COLUMNS, con.execute(f"SELECT {exprs} FROM {rel}").fetchone()))
    bad = {c: n for c, n in counts.items() if n}
    return CheckResult(
        "not_null_key_columns",
        not bad,
        f"null counts {bad}" if bad else f"no nulls in {', '.join(KEY_COLUMNS)}",
    )


def check_price_range(
    con: duckdb.DuckDBPyConnection, rel: str, low: float = config.PRICE_MIN, high: float = config.PRICE_MAX
) -> CheckResult:
    n, examples = con.execute(
        f"""
        SELECT count(*), list(price_eur_mwh ORDER BY timestamp_utc)[1:5]
        FROM {rel}
        WHERE price_eur_mwh < ? OR price_eur_mwh > ?
        """,
        [low, high],
    ).fetchone()
    return CheckResult(
        "price_in_range",
        n == 0,
        f"{n} prices outside [{low}, {high}] EUR/MWh, e.g. {examples}" if n else f"all prices in [{low}, {high}] EUR/MWh",
    )


def check_unique_timestamps(con: duckdb.DuckDBPyConnection, rel: str) -> CheckResult:
    dupes = con.execute(
        f"""
        SELECT strftime(timestamp_utc AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ'), count(*) AS n
        FROM {rel} GROUP BY 1 HAVING count(*) > 1 ORDER BY 1
        """
    ).fetchall()
    return CheckResult(
        "unique_timestamps",
        not dupes,
        f"{len(dupes)} duplicated timestamps, first: {dupes[:3]}" if dupes else "no duplicate timestamps",
    )


def check_row_count(con: duckdb.DuckDBPyConnection, rel: str, day: date) -> CheckResult:
    actual = con.execute(f"SELECT count(*) FROM {rel}").fetchone()[0]
    expected = expected_rows(day)
    return CheckResult(
        "expected_row_count",
        actual == expected,
        f"{actual} rows for {day}, expected {expected} ({config.INTERVAL_MINUTES}-minute intervals)",
    )


def run_checks(con: duckdb.DuckDBPyConnection, rel: str, day: date) -> list[CheckResult]:
    return [
        check_not_null(con, rel),
        check_price_range(con, rel),
        check_unique_timestamps(con, rel),
        check_row_count(con, rel, day),
    ]


def read_day(day: date, catalog: SqlCatalog | None = None) -> pa.Table:
    table = (catalog or get_catalog()).load_table(TABLE_ID)
    return table.scan(row_filter=EqualTo("delivery_day", day.isoformat())).to_arrow()


def write_report(day: date, results: list[CheckResult], reports_dir: Path) -> Path:
    reports_dir.mkdir(parents=True, exist_ok=True)
    path = reports_dir / f"dq_report_{day.isoformat()}.json"
    report = {
        "table": TABLE_ID,
        "delivery_day": day.isoformat(),
        "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "passed": all(r.passed for r in results),
        "checks": [asdict(r) for r in results],
    }
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return path


def validate_day(day: date, catalog: SqlCatalog | None = None, reports_dir: Path | None = None) -> Path:
    """Run all checks for one partition, write the report, raise if anything failed."""
    rows = read_day(day, catalog)
    log.info("Read %d rows for delivery_day=%s from %s", rows.num_rows, day, TABLE_ID)

    con = duckdb.connect()
    con.register("prices", rows)
    results = run_checks(con, "prices", day)
    for r in results:
        log.log(logging.INFO if r.passed else logging.ERROR, "[%s] %s: %s", "PASS" if r.passed else "FAIL", r.name, r.details)

    report = write_report(day, results, reports_dir or config.get_paths().reports_dir)
    log.info("Wrote data quality report to %s", report)

    failed = [r for r in results if not r.passed]
    if failed:
        summary = "; ".join(f"{r.name}: {r.details}" for r in failed)
        raise DataQualityError(f"{len(failed)} of {len(results)} checks failed for {day}: {summary}")
    return report
