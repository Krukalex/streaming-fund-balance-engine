import logging
from unittest.mock import MagicMock, patch

import pytest
from airflow.models import DagBag
from airflow.sdk.exceptions import AirflowFailException, AirflowTaskTimeout

from include.fund_balance_dag.callbacks import handle_failed_dag_run


def _get_dag():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    assert not dagbag.import_errors, dagbag.import_errors
    # Use the parsed dags dict instead of get_dag(), which queries the
    # metadata DB (not available in the astro pytest container).
    dag = dagbag.dags.get("fund_balance_dag")
    assert dag is not None
    return dag


def _get_task_callable():
    return _get_dag().get_task("get_fund_balance").python_callable


def _run_task(*, conf=None, params=None, run_id="manual__2025_01_01"):
    """
    Invoke the task callable with a fake airflow context and mocked docker
    """
    callable = _get_task_callable()

    dag_run = MagicMock()
    dag_run.run_id = run_id
    dag_run.conf = conf or {}

    mock_container = MagicMock()
    mock_container.exec_run.return_value = (0, b"ok")

    with patch("docker.from_env") as mock_docker:
        mock_docker.return_value.containers.get.return_value = mock_container
        callable(
            dag_run=dag_run,
            params=params
            or {
                "run_mode": "incremental",
                "starting_offset": None,
                "ending_offset": None
            },
        )

    args, kwargs = mock_container.exec_run.call_args
    command = args[0][2]
    return command


def test_run_mode_defaults_to_incremental_when_not_in_conf():
    """
    Verify that running with no config arguments defaults to incremental mode
    """

    command = _run_task(conf={})

    assert "RUN_MODE=incremental" in command
    assert "STARTING_OFFSET=" not in command
    assert "ENDING_OFFSET=" not in command


def test_run_mode_from_conf_is_passed_to_spark_command():
    """
    Verify that running in backfill mode uses starting and ending offsets passed in configuration
    """

    command = _run_task(
        conf={
            "run_mode": "backfill",
            "starting_offset": 0,
            "ending_offset": 10
        }
    )

    assert "RUN_MODE=backfill" in command
    assert "STARTING_OFFSET=0" in command
    assert "ENDING_OFFSET=10" in command


def test_backfill_without_offsets_raises():
    """
    Verify that using backfill mode without a starting offset raises an error
    """

    with pytest.raises(ValueError, match="starting_offset"):
        _run_task(conf={"run_mode": "backfill"})


def test_conf_overrides_params():
    """
    Verify that dag_run.conf values take precedence over DAG params.
    """

    command = _run_task(
        conf={
            "run_mode": "backfill",
            "starting_offset": 0,
            "ending_offset": 10,
        },
        params={
            "run_mode": "incremental",
            "starting_offset": 100,
            "ending_offset": 200,
        },
    )

    assert "RUN_MODE=backfill" in command
    assert "STARTING_OFFSET=0" in command
    assert "ENDING_OFFSET=10" in command
    assert "STARTING_OFFSET=100" not in command
    assert "ENDING_OFFSET=200" not in command


def test_fund_balance_dag_param_contract():
    """
    Verify DAG params, tags, retries, and task failure wiring stay in place.
    """
    dag = _get_dag()
    task = dag.get_task("get_fund_balance")

    assert set(dag.params.keys()) >= {
        "run_mode",
        "starting_offset",
        "ending_offset",
    }
    assert dag.params["run_mode"] == "incremental"
    assert dag.params["starting_offset"] is None
    assert dag.params["ending_offset"] is None

    run_mode_param = dag.params.get_param("run_mode")
    assert set(run_mode_param.schema.get("enum", [])) == {
        "incremental",
        "backfill",
    }

    assert "fund-balance" in dag.tags
    assert "spark" in dag.tags
    assert dag.default_args.get("retries", 0) >= 2
    assert task.execution_timeout is not None
    assert task.on_failure_callback is not None
    assert getattr(task.on_failure_callback, "__name__",
                   None) == "handle_failed_dag_run"


@pytest.mark.parametrize(
    "exception, expected_reason",
    [
        (AirflowTaskTimeout("timed out"), "timeout"),
        (AirflowFailException("manual fail"), "manual_fail"),
        (RuntimeError("spark failed"), "application_error"),
    ],
)
def test_callback_classifies_failure_reason(exception, expected_reason, caplog):
    """
    Verify failure callback maps exception types to stable reason labels.
    """
    ti = MagicMock()
    ti.task_id = "get_fund_balance"

    with caplog.at_level(logging.ERROR, logger="airflow.task"):
        handle_failed_dag_run(
            {
                "exception": exception,
                "task_instance": ti,
            }
        )

    assert f"reason={expected_reason}" in caplog.text
    assert "task=get_fund_balance" in caplog.text
