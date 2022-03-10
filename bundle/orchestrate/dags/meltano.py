from datetime import timedelta
from pendulum import datetime
import os
import yaml
import logging

from airflow import DAG
from generators.generator_factory import GeneratorFactory

logger = logging.getLogger(__name__)


# We're hardcoding this value here for the purpose of the demo, but in a production environment this
# would probably come from a config file and/or environment variables!
MELTANO_PROJECT_ROOT = os.environ["MELTANO_PROJECT_ROOT"]
MELTANO_ENVIRONMENT = "dev"

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
    dags_all = yaml_content.get("dags", {})
    generator_configs = dags_all.get("generator_configs")
    dags = dags_all.get("dag_definitions")

MELTANO_TARGET = generator_configs.get("default_target", "target-postgres")

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

    generator_obj = GeneratorFactory.get_generator(dag_def["generator"])
    generator = generator_obj(MELTANO_PROJECT_ROOT, MELTANO_ENVIRONMENT, MELTANO_TARGET)

    for down, up in generator.build_dag(dag):
        down >> up

    # register the dag
    globals()[dag_id] = dag

    logger.info(f"DAG created for schedule '{dag_id}'")
