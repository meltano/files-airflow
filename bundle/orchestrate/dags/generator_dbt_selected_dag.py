from datetime import timedelta
from pendulum import datetime
import os
import yaml
import logging

from airflow import DAG
from dbt_generator_utilities import DbtGeneratorUtilities

logger = logging.getLogger(__name__)


# We're hardcoding this value here for the purpose of the demo, but in a production environment this
# would probably come from a config file and/or environment variables!
MELTANO_PROJECT_ROOT = os.environ["MELTANO_PROJECT_ROOT"]
MELTANO_ENVIRONMENT = "dev"
MELTANO_TARGET = "target-postgres"

DEFAULT_ARGS = {
    "owner": "meltano",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "concurrency": 1,
}

args = DEFAULT_ARGS.copy()

with open(os.path.join(MELTANO_PROJECT_ROOT, "orchestrate", "dag_definition.yml"), "r") as yaml_file:
    yaml_content = yaml.safe_load(yaml_file)
    dags = yaml_content.get("dags")

for dag_name, dag_def in dags.items():
    logger.info(f"Considering dag '{dag_name}'")
    dag_id = f"meltano_{dag_name}"
    dag = DAG(
        dag_id,
        catchup=False,
        default_args=args,
        schedule_interval=dag_def["interval"],
        # We don't care about start date since were not using it and its recommended
        # to be static so we just set it the same date for all
        start_date=datetime(2022, 1, 1),
        max_active_runs=1,
    )

    dbt_utils = DbtGeneratorUtilities(dag, MELTANO_PROJECT_ROOT, MELTANO_ENVIRONMENT, MELTANO_TARGET)
    manifest = dbt_utils.load_manifest()
    selected_models = dbt_utils.get_models_from_select(MELTANO_ENVIRONMENT, dag_def["selection_rule"])
    dbt_tasks = dbt_utils.build_tasks_list(manifest, selected_models)

    for down, up in dbt_utils.build_dag(manifest, dbt_tasks, selected_models):
        down >> up

    # register the dag
    globals()[dag_id] = dag

    logger.info(f"DAG created for schedule '{dag_id}'")
