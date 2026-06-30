"""
Fund balance batch DAG.

Preferred local pattern: docker exec into the long-running spark-master container
(already has spark/ and jars/ mounted). DockerOperator is valid but spins up a
separate one-off container and needs host bind-mount paths for code + JARs.
"""

from airflow.decorators import dag, task
from pendulum import datetime, duration
from include.fund_balance_dag.callbacks import handle_failed_dag_run
from airflow.models.param import Param


SPARK_MASTER_CONTAINER = "spark-master-fund-balance"
SPARK_MASTER_URL = "spark://spark-master-fund-balance:7077"


@dag(
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["fund-balance", "spark"],
    default_args={
        "retries": 2,
        "retry_delay": duration(seconds=2)
    },
    doc_md=__doc__,
    params={
        "run_mode": Param("incremental", enum=["incremental", "backfill"]),
        "starting_offset": Param(None, type=["null", "integer"]),
        "ending_offset": Param(None, type=["null", "integer"]),
    }
)
def fund_balance_dag():
    @task(
        task_id='get_fund_balance',
        execution_timeout=duration(minutes=10),
        retry_exponential_backoff=True,
        max_retry_delay=duration(15),
        on_failure_callback=handle_failed_dag_run
    )
    def run_spark_fund_balance_job(**context):
        import docker

        dag_run = context["dag_run"]
        conf = dag_run.conf or {}
        params = context.get("params", {})

        # Get environment variables either from run config or default params
        run_id = dag_run.run_id.replace("-", "_")
        run_mode = conf.get("run_mode", params.get("run_mode", "incremental"))
        starting_offset = conf.get(
            "starting_offset", params.get("starting_offset"))
        ending_offset = conf.get("ending_offset", params.get("ending_offset"))

        # Build docker exec command based on run mode
        if run_mode == "backfill":
            if starting_offset is None or ending_offset is None:
                raise ValueError(
                    "backfill requires starting_offset and ending_offset in conf/params"
                )
            env_prefix = (
                f"RUN_ID={run_id} RUN_MODE=backfill "
                f"STARTING_OFFSET={starting_offset} ENDING_OFFSET={ending_offset} "
            )
        else:
            env_prefix = f"RUN_ID={run_id} RUN_MODE=incremental "
        command = (
            "JARS=$(find /opt/spark/jars-extra -maxdepth 1 -name '*.jar' | paste -sd, -) && "
            f"{env_prefix}"
            f"/opt/spark/bin/spark-submit --master {SPARK_MASTER_URL} "
            '--jars "${JARS}" /opt/spark-apps/job.py'
        )

        client = docker.from_env()
        container = client.containers.get(SPARK_MASTER_CONTAINER)
        exit_code, output = container.exec_run(
            ["bash", "-lc", command],
            user="root",
        )

        if output:
            print(output.decode("utf-8", errors="replace"))

        if exit_code != 0:
            raise RuntimeError(
                f"spark-submit failed in {SPARK_MASTER_CONTAINER} "
                f"with exit code {exit_code}"
            )

    run_spark_fund_balance_job()


fund_balance_dag()
