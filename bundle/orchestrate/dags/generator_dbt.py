import json
from datetime import timedelta
from pendulum import datetime
import os

from airflow import DAG
from airflow.operators.bash import BashOperator

# We're hardcoding this value here for the purpose of the demo, but in a production environment this
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = "/usr/local/airflow/dbt"
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

dag = DAG(
    "meltano_dbt_mono_dag",
    start_date=datetime(2020, 12, 23),
    default_args=DEFAULT_ARGS,
    description="A dbt wrapper for airflow",
    schedule_interval=None,
    catchup=False,
)


def load_manifest():
    local_filepath = f"{MELTANO_PROJECT_ROOT}/.meltano/transformers/dbt/target/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)
    return data


def make_dbt_task(node, dbt_verb):
    """Returns an Airflow operator either run and test an individual model"""
    model = node.split(".")[-1]
    if dbt_verb == "run":
        dbt_task = BashOperator(
            task_id=node,
            bash_command=f"""
            cd {MELTANO_PROJECT_ROOT}; meltano --environment={MELTANO_ENVIRONMENT} invoke dbt:{dbt_verb} --models {model}
            """,
            dag=dag,
        )
    elif dbt_verb == "test":
        node_test = node.replace("model", "test")
        dbt_task = BashOperator(
            task_id=node_test,
            bash_command=f"""
            cd {MELTANO_PROJECT_ROOT}; meltano --environment={MELTANO_ENVIRONMENT} invoke dbt:{dbt_verb} --models {model}
            """,
            dag=dag,
        )
    return dbt_task

def build_meltano_cmd(dbt_source_node, env, stream=False):
    parts = dbt_source_node.split(".")
    tap = parts[2]
    if stream:
        meltano_stream = parts[3]
        select_filter = f"--select {meltano_stream}"
    # TODO: this makes and assumption of how taps are named, we should add a translation layer for exceptional cases
    tap = tap.replace("_", "-")
    return f"meltano --log-level=debug --environment={env} elt {tap} {MELTANO_TARGET} {select_filter} --job_id={tap}_{MELTANO_TARGET}"

# This task loads the CSV files from dbt/data into the local postgres database for the purpose of this demo.
# In practice, we'd usually expect the data to have already been loaded to the database.

data = load_manifest()
dbt_tasks = {}

for node in data["nodes"].keys():
    if node.split(".")[0] == "model":
        node_test = node.replace("model", "test")
        dbt_tasks[node] = make_dbt_task(node, "run")
        dbt_tasks[node_test] = make_dbt_task(node, "test")


for node in data["nodes"].keys():
    if node.split(".")[0] == "model":
        # Set dependency to run tests on a model after model runs finishes
        node_test = node.replace("model", "test")
        dbt_tasks[node] >> dbt_tasks[node_test]
        # Set all model -> model dependencies
        for upstream_node in data["nodes"][node]["depends_on"]["nodes"]:
            upstream_node_type = upstream_node.split(".")[0]
            if upstream_node_type == "model":
                dbt_tasks[upstream_node] >> dbt_tasks[node]
            elif upstream_node_type == "source":
                # For source run Meltano jobs
                meltano_cmd = build_meltano_cmd(upstream_node, MELTANO_ENVIRONMENT, stream=True)
                meltano_task = BashOperator(
                    task_id=f"meltano-{upstream_node}",
                    bash_command=f"""
                    cd {MELTANO_PROJECT_ROOT}; {meltano_cmd} 
                    """,
                    dag=dag,
                )
                meltano_task >> dbt_tasks[node]
