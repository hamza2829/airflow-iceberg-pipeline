"""DAG structure test. Needs Airflow, so it is skipped on Windows / without Airflow."""

import importlib.util
from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("airflow", reason="Airflow is not installed (it does not run natively on Windows)")

DAG_FILE = Path(__file__).resolve().parents[1] / "dags" / "energy_prices_dag.py"


def load_dag():
    spec = importlib.util.spec_from_file_location("energy_prices_dag", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.energy_prices_dag


def test_dag_has_three_tasks_daily_schedule_and_retries():
    dag = load_dag()

    assert dag.dag_id == "energy_prices_iceberg"
    assert {t.task_id for t in dag.tasks} == {"ingest", "load", "validate"}
    assert dag.get_task("ingest").downstream_task_ids == {"load"}
    assert dag.get_task("load").downstream_task_ids == {"validate"}
    assert dag.timetable.expression == "0 0 * * *"  # @daily
    assert dag.get_task("validate").retries == 0
    assert dag.get_task("ingest").retries == 2
    assert dag.get_task("ingest").retry_delay == timedelta(minutes=2)
