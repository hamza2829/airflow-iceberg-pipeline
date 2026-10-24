"""Daily pipeline: ingest Energy-Charts day-ahead prices -> load into Iceberg -> validate."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

from airflow.sdk import dag, task

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="energy_prices_iceberg",
    description="German day-ahead prices -> Apache Iceberg -> DuckDB data quality checks",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["iceberg", "duckdb", "energy"],
)
def energy_prices_iceberg():
    @task
    def ingest(ds: str | None = None) -> dict:
        from pipeline.ingest import ingest_day

        log.info("Ingest: fetching day-ahead prices for delivery day %s", ds)
        meta = ingest_day(date.fromisoformat(ds))
        log.info("Ingest: done, source=%s raw_path=%s", meta["source"], meta["raw_path"])
        return meta

    @task
    def load(meta: dict) -> str:
        from pipeline.load import load_raw_file

        day = date.fromisoformat(meta["delivery_day"])
        log.info("Load: writing %s into Iceberg partition delivery_day=%s", meta["raw_path"], day)
        n = load_raw_file(meta["raw_path"], day)
        log.info("Load: done, %d rows in partition %s", n, day)
        return day.isoformat()

    # Data quality failures are deterministic, so retrying would only delay the alert.
    @task(retries=0)
    def validate(day: str) -> str:
        from pipeline.quality import validate_day

        log.info("Validate: running data quality checks for delivery_day=%s", day)
        report = validate_day(date.fromisoformat(day))
        log.info("Validate: all checks passed, report at %s", report)
        return str(report)

    validate(load(ingest()))


energy_prices_dag = energy_prices_iceberg()
