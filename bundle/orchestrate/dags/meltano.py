# If you want to define a custom DAG, create
# a new file under orchestrate/dags/ and Airflow
# will pick it up automatically.

import json
import logging
import os
import subprocess

from airflow import DAG

try:
    from airflow.operators.bash_operator import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

from datetime import timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "concurrency": 1,
}

DEFAULT_TAGS = ["meltano"]

project_root = os.getenv("MELTANO_PROJECT_ROOT", os.getcwd())

meltano_bin = ".meltano/run/bin"

if not Path(project_root).joinpath(meltano_bin).exists():
    logger.warning(
        f"A symlink to the 'meltano' executable could not be found at '{meltano_bin}'. Falling back on expecting it to be in the PATH instead."
    )
    meltano_bin = "meltano"


def _meltano_v1_generator(schedules):
    """Generate singular dag's for each legacy Meltano elt task.

    Args:
        schedules (list): List of Meltano schedules.
    """
    for schedule in schedules:
        logger.info(f"Considering schedule '{schedule['name']}': {schedule}")

        if not schedule["cron_interval"]:
            logger.info(
                f"No DAG created for schedule '{schedule['name']}' because its interval is set to `@once`.",
            )
            continue

        args = DEFAULT_ARGS.copy()
        if schedule["start_date"]:
            args["start_date"] = schedule["start_date"]

        dag_id = f"meltano_{schedule['name']}"

        tags = DEFAULT_TAGS.copy()
        if schedule["extractor"]:
            tags.append(schedule["extractor"])
        if schedule["loader"]:
            tags.append(schedule["loader"])
        if schedule["transform"] == "run":
            tags.append("transform")
        elif schedule["transform"] == "only":
            tags.append("transform-only")

        # from https://airflow.apache.org/docs/stable/scheduler.html#backfill-and-catchup
        #
        # It is crucial to set `catchup` to False so that Airflow only create a single job
        # at the tail end of date window we want to extract data.
        #
        # Because our extractors do not support date-window extraction, it serves no
        # purpose to enqueue date-chunked jobs for complete extraction window.
        dag = DAG(
            dag_id,
            tags=tags,
            catchup=False,
            default_args=args,
            schedule_interval=schedule["interval"],
            max_active_runs=1,
        )

        elt = BashOperator(
            task_id="extract_load",
            bash_command=f"cd {project_root}; {meltano_bin} schedule run {schedule['name']}",
            dag=dag,
        )

        # register the dag
        globals()[dag_id] = dag

        logger.info(f"DAG created for schedule '{schedule['name']}'")


def _meltano_v2_job_generator(schedules):
    """Generate dag's for each task within a Meltano scheduled job.

    Args:
        schedules (list): List of Meltano scheduled jobs.
    """
    for schedule in schedules:
        if not schedule.get("job"):
            logger.info(
                f"No DAG's created for schedule '{schedule['name']}'. It was passed to job generator but has no job."
            )
            continue
        if not schedule["cron_interval"]:
            logger.info(
                f"No DAG created for schedule '{schedule['name']}' because its interval is set to `@once`."
            )
            continue

        base_id = f"meltano_{schedule['name']}_{schedule['job']['name']}"
        common_tags = DEFAULT_TAGS.copy()
        common_tags.append(schedule["job"]["name"])
        for idx, task in enumerate(schedule["job"]["tasks"]):
            logger.info(
                f"Considering task '{task}' of schedule '{schedule['name']}': {schedule}"
            )
            args = DEFAULT_ARGS.copy()

            dag_id = f"{base_id}_task{idx}"

            task_tags = common_tags.copy()

            # from https://airflow.apache.org/docs/stable/scheduler.html#backfill-and-catchup
            #
            # It is crucial to set `catchup` to False so that Airflow only create a single job
            # at the tail end of date window we want to extract data.
            #
            # Because our extractors do not support date-window extraction, it serves no
            # purpose to enqueue date-chunked jobs for complete extraction window.
            dag = DAG(
                dag_id,
                description=f"Meltano run task[{idx}]: '{task}'",
                tags=task_tags,
                catchup=False,
                default_args=args,
                schedule_interval=schedule["interval"],
                max_active_runs=1,
            )

            elt = BashOperator(
                task_id=task["name"],
                bash_command=f"cd {project_root}; {meltano_bin} run {task}",
                dag=dag,
            )

            # register the dag
            globals()[dag_id] = dag
            logger.info(
                f"Task DAG created for schedule '{schedule['name']}',Task='{task}'"
            )


list_result = subprocess.run(
    [meltano_bin, "schedule", "list", "--format=json"],
    cwd=project_root,
    stdout=subprocess.PIPE,
    universal_newlines=True,
    check=True,
)
schedules = json.loads(list_result.stdout)

if schedules.get("job") and schedules.get("elt"):
    _meltano_v1_generator(schedules.get("elt"))
    _meltano_v2_job_generator(schedules.get("job"))
else:
    _meltano_v1_generator(schedules)
