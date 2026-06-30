from airflow.sdk.exceptions import AirflowTaskTimeout, AirflowFailException
import logging
import json
log = logging.getLogger("airflow.task")


def handle_failed_dag_run(context):
    print(f"""
    context:
    {json.dumps(context, indent=2, default=str)}
  """)
    exc = context.get("exception")
    ti = context["task_instance"]

    if isinstance(exc, AirflowTaskTimeout):
        # execution_timeout exceeded (your "took too long" case)
        reason = "timeout"
    elif isinstance(exc, AirflowFailException):
        reason = "manual_fail"
    else:
        reason = "application_error"

    print(f"task={ti.task_id} reason={reason} exc={exc!r}")
    log.error("task=%s reason=%s exc=%r", ti.task_id, reason, exc)
