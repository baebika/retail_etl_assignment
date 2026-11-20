"""DAG integrity tests for retail_etl.

Run inside the Airflow container (Airflow + Snowflake provider installed):

    docker compose run --rm airflow-cli bash -c \
      "pip install pytest -q && pytest /opt/airflow/tests -v"
"""
import sys
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"
sys.path.insert(0, str(DAGS_DIR))  # so `from ingestion import ...` works

try:  # Airflow 3.x location
    from airflow.dag_processing.dagbag import DagBag  # noqa: E402
except ImportError:  # older layouts
    from airflow.models import DagBag  # noqa: E402

DAG_ID = "retail_etl"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR))


def test_dag_parses_without_import_errors(dagbag):
    assert dagbag.import_errors == {}, f"Import errors: {dagbag.import_errors}"


def test_dag_is_loaded(dagbag):
    assert DAG_ID in dagbag.dags


def test_dag_has_no_cycles(dagbag):
    # Version-independent cycle check: Kahn's algorithm over the task graph.
    dag = dagbag.dags[DAG_ID]
    indegree = {t.task_id: len(t.upstream_task_ids) for t in dag.tasks}
    ready = [t for t, d in indegree.items() if d == 0]
    visited = 0
    while ready:
        tid = ready.pop()
        visited += 1
        for down in dag.get_task(tid).downstream_task_ids:
            indegree[down] -= 1
            if indegree[down] == 0:
                ready.append(down)
    assert visited == len(indegree), "Cycle detected in DAG"


def test_all_tasks_have_retries(dagbag):
    dag = dagbag.dags[DAG_ID]
    for task in dag.tasks:
        assert task.retries >= 1, f"{task.task_id} has retries={task.retries}"
        assert task.retry_delay is not None, f"{task.task_id} has no retry_delay"


def test_expected_tasks_and_order(dagbag):
    dag = dagbag.dags[DAG_ID]
    ids = set(dag.task_ids)
    expected = {
        "get_run_details", "create_batch_task", "build_file_specs",
        "ingest_one_file", "silver_transform", "validate_silver_integrity",
        "gold_transform", "reconcile_data", "mark_batch_success",
    }
    assert expected <= ids, f"Missing tasks: {expected - ids}"
    assert "silver_transform" in dag.get_task("ingest_one_file").downstream_task_ids
    assert "gold_transform" in dag.get_task("validate_silver_integrity").downstream_task_ids
    assert "reconcile_data" in dag.get_task("gold_transform").downstream_task_ids
    assert "mark_batch_success" in dag.get_task("reconcile_data").downstream_task_ids